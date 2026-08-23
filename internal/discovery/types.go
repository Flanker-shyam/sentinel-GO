package discovery

import (
	"context"
	"fmt"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

// Target defines which pods to stream logs from.
type Target struct {
	Namespace         string
	PodPatterns       []string // regex patterns to match pod names
	ContainerPatterns []string // regex patterns to match container names (empty = auto-pick)
}

// PodStream holds the resolved pod + container to stream from.
type PodStream struct {
	Namespace string
	PodName   string
	Container string
}

// StreamFunc is the function signature for streaming logs from a pod.
// Injected into the controller to avoid circular imports with the streamer package.
type StreamFunc func(ctx context.Context, clientset *kubernetes.Clientset, pod PodStream, out chan<- string)

// knownSidecars are containers we skip when auto-picking the app container.
var knownSidecars = map[string]bool{
	"linkerd-init":  true,
	"linkerd-proxy": true,
	"istio-init":    true,
	"istio-proxy":   true,
	"envoy":         true,
	"otlp":          true,
	"datadog-agent": true,
}

// --- Shared helpers ---

// matchesAny returns true if the name matches any of the compiled regexes.
func matchesAny(name string, regexes []*regexp.Regexp) bool {
	for _, re := range regexes {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

// compilePatterns compiles a list of regex strings into regexp objects.
func compilePatterns(patterns []string) ([]*regexp.Regexp, error) {
	regexes := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile("^" + p + "$")
		if err != nil {
			return nil, fmt.Errorf("invalid pattern %q: %w", p, err)
		}
		regexes = append(regexes, re)
	}
	return regexes, nil
}

// pickContainers selects containers from the pod spec matching the given patterns.
// If no patterns are provided, auto-picks the first non-sidecar container.
func pickContainers(pod corev1.Pod, patterns []*regexp.Regexp) []string {
	if len(patterns) > 0 {
		var matched []string
		for _, c := range pod.Spec.Containers {
			if matchesAny(c.Name, patterns) {
				matched = append(matched, c.Name)
			}
		}
		return matched
	}

	// Auto-pick: first non-sidecar container
	for _, c := range pod.Spec.Containers {
		if !knownSidecars[c.Name] {
			return []string{c.Name}
		}
	}

	// Fallback: first container
	if len(pod.Spec.Containers) > 0 {
		return []string{pod.Spec.Containers[0].Name}
	}
	return nil
}
