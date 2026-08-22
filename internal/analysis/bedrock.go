package analysis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
)

const systemPrompt = `You are a production incident analyst for a Kubernetes-based microservices platform. You receive batches of ERROR and WARNING log lines from running services.

Your job:
1. Determine if the logs indicate a real anomaly (not just routine errors)
2. Assess severity
3. Identify the root cause
4. Recommend immediate action

IMPORTANT DISTINCTIONS:
- Routine: Occasional timeouts, single retry failures, user input validation errors — these are normal
- Anomaly: Repeated connection failures, cascading errors, resource exhaustion, sudden spike in same error, data corruption signals

Respond ONLY in valid JSON with this exact schema:
{
  "is_anomaly": boolean,
  "severity": integer (1-10),
  "summary": "one-line description of what's happening",
  "root_cause": "most likely cause based on the evidence",
  "affected_services": ["service names extracted from logs"],
  "recommendation": "immediate action to take"
}

Severity scale:
1-3: Low — isolated errors, likely transient
4-5: Medium — pattern forming, worth monitoring
6-7: High — active issue affecting users
8-9: Critical — service degradation, immediate action needed
10: Catastrophic — full outage, all hands

If the logs show no anomaly (just normal operational noise), respond:
{
  "is_anomaly": false,
  "severity": 1,
  "summary": "Normal operational errors, no anomaly detected",
  "root_cause": "N/A",
  "affected_services": [],
  "recommendation": "No action required"
}`

// AnalysisResult holds the LLM's assessment of a log batch.
type AnalysisResult struct {
	IsAnomaly        bool     `json:"is_anomaly"`
	Severity         int      `json:"severity"`
	Summary          string   `json:"summary"`
	RootCause        string   `json:"root_cause"`
	AffectedServices []string `json:"affected_services"`
	Recommendation   string   `json:"recommendation"`
}

// BedrockAnalyzer calls AWS Bedrock (Claude) to analyze log batches.
type BedrockAnalyzer struct {
	client           *bedrockruntime.Client
	modelID          string
	maxTokens        int
	anomalyThreshold int
	namespace        string
}

// BedrockConfig holds configuration for the Bedrock analyzer.
type BedrockConfig struct {
	Region           string
	ModelID          string
	MaxTokens        int
	AnomalyThreshold int
	Namespace        string
}

// NewBedrockAnalyzer creates a new Bedrock-based log analyzer.
// It uses the default credential chain (saml2aws login → ~/.aws/credentials).
func NewBedrockAnalyzer(ctx context.Context, cfg BedrockConfig) (*BedrockAnalyzer, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := bedrockruntime.NewFromConfig(awsCfg)

	return &BedrockAnalyzer{
		client:           client,
		modelID:          cfg.ModelID,
		maxTokens:        cfg.MaxTokens,
		anomalyThreshold: cfg.AnomalyThreshold,
		namespace:        cfg.Namespace,
	}, nil
}

// Analyze sends a batch of log lines to Claude and returns the analysis.
func (b *BedrockAnalyzer) Analyze(ctx context.Context, logs []string) (*AnalysisResult, error) {
	// Build the user message
	userMessage := fmt.Sprintf(
		"Analyze these %d log lines from namespace %q (recent batch):\n\n%s",
		len(logs),
		b.namespace,
		strings.Join(logs, "\n"),
	)

	// Build the request body (Claude Messages API format)
	requestBody := ClaudeRequest{
		AnthropicVersion: "bedrock-2023-05-31",
		MaxTokens:        b.maxTokens,
		System:           systemPrompt,
		Messages: []Message{
			{
				Role:    "user",
				Content: userMessage,
			},
		},
	}

	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	// Call Bedrock
	output, err := b.client.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		ModelId:     aws.String(b.modelID),
		ContentType: aws.String("application/json"),
		Body:        bodyBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("invoke model: %w", err)
	}

	// Parse Claude's response
	var response ClaudeResponse
	if err := json.Unmarshal(output.Body, &response); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if len(response.Content) == 0 {
		return nil, fmt.Errorf("empty response from model")
	}

	// Extract the JSON from Claude's text response
	var result AnalysisResult
	if err := json.Unmarshal([]byte(response.Content[0].Text), &result); err != nil {
		return nil, fmt.Errorf("parse analysis result: %w (raw: %s)", err, response.Content[0].Text)
	}

	return &result, nil
}

// ShouldAlert returns true if the analysis result warrants an alert.
func (b *BedrockAnalyzer) ShouldAlert(result *AnalysisResult) bool {
	return result.IsAnomaly && result.Severity >= b.anomalyThreshold
}

// AnalyzeBatch is a convenience method that analyzes and logs the result.
// Use this as the AnalyzeFunc passed to BatchAndAnalyze.
func (b *BedrockAnalyzer) AnalyzeBatch(logs []string) {
	ctx := context.Background()

	result, err := b.Analyze(ctx, logs)
	if err != nil {
		log.Printf("LLM analysis failed: %v", err)
		return
	}

	if b.ShouldAlert(result) {
		log.Printf("🚨 ANOMALY DETECTED [severity=%d]: %s", result.Severity, result.Summary)
		log.Printf("   Root cause: %s", result.RootCause)
		log.Printf("   Affected: %v", result.AffectedServices)
		log.Printf("   Action: %s", result.Recommendation)
		// TODO: Send to alert sinks (Slack, SNS, etc.)
	} else {
		log.Printf("✅ Batch of %d logs analyzed — no anomaly (severity=%d)", len(logs), result.Severity)
	}
}

// --- Claude API types ---

// ClaudeRequest is the request body for Claude on Bedrock.
type ClaudeRequest struct {
	AnthropicVersion string    `json:"anthropic_version"`
	MaxTokens        int       `json:"max_tokens"`
	System           string    `json:"system"`
	Messages         []Message `json:"messages"`
}

// Message represents a single message in the conversation.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ClaudeResponse is the response from Claude on Bedrock.
type ClaudeResponse struct {
	Content    []ContentBlock `json:"content"`
	StopReason string         `json:"stop_reason"`
}

// ContentBlock is a single content block in Claude's response.
type ContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
