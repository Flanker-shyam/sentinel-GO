package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Alert represents an anomaly alert to be sent.
type Alert struct {
	Severity         int
	Summary          string
	RootCause        string
	AffectedServices []string
	Recommendation   string
	Timestamp        time.Time
}

// GoogleChatNotifier sends alerts to a Google Chat space via webhook.
type GoogleChatNotifier struct {
	webhookURL string
	httpClient *http.Client
}

// NewGoogleChatNotifier creates a notifier for the given webhook URL.
// Get the webhook URL from: Google Chat Space → Apps & Integrations → Webhooks
func NewGoogleChatNotifier(webhookURL string) *GoogleChatNotifier {
	return &GoogleChatNotifier{
		webhookURL: webhookURL,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// Send posts an alert to the Google Chat space.
func (g *GoogleChatNotifier) Send(ctx context.Context, alert Alert) error {
	message := g.formatMessage(alert)

	payload := gchatMessage{
		Text: message,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send to google chat: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("google chat returned %d", resp.StatusCode)
	}

	return nil
}

// formatMessage creates a formatted alert message for Google Chat.
func (g *GoogleChatNotifier) formatMessage(alert Alert) string {
	severityEmoji := "⚠️"
	if alert.Severity >= 8 {
		severityEmoji = "🚨"
	} else if alert.Severity >= 6 {
		severityEmoji = "🔥"
	}

	return fmt.Sprintf(
		`%s *ANOMALY DETECTED* [severity=%d/10]

*Summary:* %s

*Root Cause:* %s

*Affected Services:* %v

*Recommendation:* %s

_Detected at %s_`,
		severityEmoji,
		alert.Severity,
		alert.Summary,
		alert.RootCause,
		alert.AffectedServices,
		alert.Recommendation,
		alert.Timestamp.Format("2006-01-02 15:04:05 MST"),
	)
}

// gchatMessage is the Google Chat webhook payload format.
type gchatMessage struct {
	Text string `json:"text"`
}
