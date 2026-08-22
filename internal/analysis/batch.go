package analysis

import (
	"context"
	"fmt"
	"time"
)

// AnalyzeFunc is the signature for functions that process a batch of log lines.
type AnalyzeFunc func([]string)

// BatchAndAnalyze reads filtered log lines from the channel, batches them by
// time window or size threshold (whichever comes first), and calls analyze.
// It blocks until the channel is closed or ctx is cancelled.
func BatchAndAnalyze(ctx context.Context, logs <-chan string, maxBatchSize int, flushInterval time.Duration, analyze AnalyzeFunc) {
	batch := make([]string, 0, maxBatchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case line, ok := <-logs:
			if !ok {
				if len(batch) > 0 {
					analyze(batch)
				}
				return
			}
			batch = append(batch, line)
			if len(batch) >= maxBatchSize {
				analyze(batch)
				batch = make([]string, 0, maxBatchSize)
				ticker.Reset(flushInterval)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				analyze(batch)
				batch = make([]string, 0, maxBatchSize)
			}
		case <-ctx.Done():
			return
		}
	}
}

// PrintBatch is a simple AnalyzeFunc that prints the batch to stdout.
// Replace this with LLM analysis later.
func PrintBatch(logs []string) {
	fmt.Printf("\n=== Batch of %d logs ===\n", len(logs))
	for i, line := range logs {
		fmt.Printf("  [%d] %s\n", i+1, line)
	}
	fmt.Println("========================")
}
