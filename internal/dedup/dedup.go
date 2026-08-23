package dedup

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"
)

// Deduplicator suppresses duplicate log lines within a time window.
// Uses a sliding window hash set — each entry expires after the configured duration.
type Deduplicator struct {
	window  time.Duration
	mu      sync.Mutex
	seen    map[[32]byte]time.Time // hash → first seen time
}

// New creates a Deduplicator with the given time window.
// Duplicate lines within the window are suppressed.
func New(window time.Duration) *Deduplicator {
	d := &Deduplicator{
		window: window,
		seen:   make(map[[32]byte]time.Time),
	}
	return d
}

// IsDuplicate returns true if this line has been seen within the time window.
// If not a duplicate, it records the line and returns false.
func (d *Deduplicator) IsDuplicate(line string) bool {
	hash := sha256.Sum256([]byte(line))
	now := time.Now()

	d.mu.Lock()
	defer d.mu.Unlock()

	if firstSeen, exists := d.seen[hash]; exists {
		if now.Sub(firstSeen) < d.window {
			return true // seen within window — duplicate
		}
		// Window expired — treat as new
		d.seen[hash] = now
		return false
	}

	d.seen[hash] = now
	return false
}

// StartCleanup runs a background goroutine that periodically removes expired entries.
// Call this once after creating the deduplicator.
func (d *Deduplicator) StartCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(d.window)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				d.cleanup()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// cleanup removes entries older than the window.
func (d *Deduplicator) cleanup() {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()

	for hash, firstSeen := range d.seen {
		if now.Sub(firstSeen) >= d.window {
			delete(d.seen, hash)
		}
	}
}

// Run reads from the input channel, deduplicates, and forwards unique lines to output.
// Blocks until input is closed or ctx is cancelled.
func (d *Deduplicator) Run(ctx context.Context, in <-chan string, out chan<- string) {
	for {
		select {
		case line, ok := <-in:
			if !ok {
				return
			}
			if !d.IsDuplicate(line) {
				select {
				case out <- line:
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
