package streamer

import (
	"bufio"
	"context"
	"io"
	"log"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// PodInfo holds the info needed to open a log stream.
type PodInfo struct {
	Namespace string
	PodName   string
	Container string
}

// Stream opens a follow-stream on the given pod/container and pushes
// severity-filtered lines into the output channel.
// It blocks until the stream ends or ctx is cancelled.
func Stream(ctx context.Context, clientset *kubernetes.Clientset, pod PodInfo, tailLines int64, out chan<- string) {
	logOpts := &corev1.PodLogOptions{
		Follow:     true,
		Timestamps: true,
		TailLines:  &tailLines,
		Container:  pod.Container,
	}

	stream, err := clientset.CoreV1().
		Pods(pod.Namespace).
		GetLogs(pod.PodName, logOpts).
		Stream(ctx)
	if err != nil {
		log.Printf("[streamer] failed to open log stream for %s/%s/%s: %v", pod.Namespace, pod.PodName, pod.Container, err)
		return
	}

	if err := filterAndForward(ctx, stream, out); err != nil {
		log.Printf("[streamer] stream ended for %s/%s/%s: %v", pod.Namespace, pod.PodName, pod.Container, err)
	}
}

// filterAndForward reads lines from the stream, filters by severity,
// and pushes matching lines into the channel.
func filterAndForward(ctx context.Context, stream io.ReadCloser, out chan<- string) error {
	defer stream.Close()

	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if isSevere(line) {
			select {
			case out <- line:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return scanner.Err()
}

// isSevere checks if the log line has a severity level that we care about.
// It looks for the "level" field in JSON-structured logs.
func isSevere(line string) bool {
	lower := strings.ToLower(line)
	levelIdx := strings.Index(lower, "\"level\":\"")
	if levelIdx == -1 {
		return false
	}

	start := levelIdx + 9 // len(`"level":"`)
	end := strings.IndexByte(line[start:], '"')
	if end == -1 {
		return false
	}

	level := strings.ToUpper(line[start : start+end])
	return level == "ERROR" || level == "WARN" || level == "WARNING" ||
		level == "FATAL" || level == "PANIC"
}
