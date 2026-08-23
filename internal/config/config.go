package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration.
type Config struct {
	Targets      []TargetConfig    `yaml:"targets"`
	Streaming    StreamingConfig   `yaml:"streaming"`
	Analysis     AnalysisConfig    `yaml:"analysis"`
	Dedup        DedupConfig       `yaml:"dedup"`
	Notification NotificationConfig `yaml:"notification"`
}

// TargetConfig defines which pods to stream from.
type TargetConfig struct {
	Namespace         string   `yaml:"namespace"`
	PodPatterns       []string `yaml:"pod_patterns"`
	ContainerPatterns []string `yaml:"container_patterns"`
}

// StreamingConfig controls stream behavior.
type StreamingConfig struct {
	BufferSize int   `yaml:"buffer_size"`
	TailLines  int64 `yaml:"tail_lines"`
}

// AnalysisConfig controls batching and LLM.
type AnalysisConfig struct {
	BatchSize        int           `yaml:"batch_size"`
	FlushInterval    time.Duration `yaml:"flush_interval"`
	Region           string        `yaml:"region"`
	ModelID          string        `yaml:"model_id"`
	MaxTokens        int           `yaml:"max_tokens"`
	AnomalyThreshold int           `yaml:"anomaly_threshold"`
	MinCallInterval  time.Duration `yaml:"min_call_interval"`
}

// DedupConfig controls log deduplication.
type DedupConfig struct {
	Enabled bool          `yaml:"enabled"`
	Window  time.Duration `yaml:"window"`
}

// NotificationConfig controls alert notifications.
type NotificationConfig struct {
	GoogleChat GoogleChatConfig `yaml:"google_chat"`
}

// GoogleChatConfig holds Google Chat webhook settings.
type GoogleChatConfig struct {
	Enabled    bool   `yaml:"enabled"`
	WebhookURL string `yaml:"webhook_url"`
}

// Load reads and parses the YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// Expand environment variables (e.g., ${GCHAT_WEBHOOK_URL})
	expanded := os.ExpandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.applyDefaults()
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Streaming.BufferSize == 0 {
		c.Streaming.BufferSize = 1000
	}
	if c.Streaming.TailLines == 0 {
		c.Streaming.TailLines = 50
	}
	if c.Analysis.BatchSize == 0 {
		c.Analysis.BatchSize = 50
	}
	if c.Analysis.FlushInterval == 0 {
		c.Analysis.FlushInterval = 30 * time.Second
	}
	if c.Analysis.Region == "" {
		c.Analysis.Region = "eu-west-1"
	}
	if c.Analysis.ModelID == "" {
		c.Analysis.ModelID = "anthropic.claude-3-5-sonnet-20241022-v2:0"
	}
	if c.Analysis.MaxTokens == 0 {
		c.Analysis.MaxTokens = 2048
	}
	if c.Analysis.AnomalyThreshold == 0 {
		c.Analysis.AnomalyThreshold = 6
	}
	if c.Analysis.MinCallInterval == 0 {
		c.Analysis.MinCallInterval = 10 * time.Second
	}
	if c.Dedup.Window == 0 {
		c.Dedup.Window = 60 * time.Second
	}
}
