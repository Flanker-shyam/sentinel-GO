package discovery

import (
	"context"
	"fmt"
	"log"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// Discover lists running pods matching the configured targets and resolves their containers.
func Discover(ctx context.Context, clientset *kubernetes.Clientset, targets []Target) ([]PodStream, error) {
	var result []PodStream

	for _, target := range targets {
		// Compile all pod regexes for this target
		podRegexes, err := compilePatterns(target.PodPatterns)
		if err != nil {
			return nil, fmt.Errorf("invalid pod_patterns: %w", err)
		}

		// Compile all container regexes for this target
		containerRegexes, err := compilePatterns(target.ContainerPatterns)
		if err != nil {
			return nil, fmt.Errorf("invalid container_patterns: %w", err)
		}

		pods, err := clientset.CoreV1().Pods(target.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("failed to list pods in %s: %v", target.Namespace, err)
			continue
		}

		for _, pod := range pods.Items {
			if pod.Status.Phase != corev1.PodRunning {
				continue
			}
			if !matchesAny(pod.Name, podRegexes) {
				continue
			}

			// Pick container(s) matching the patterns
			containers := pickContainers(pod, containerRegexes)
			if len(containers) == 0 {
				log.Printf("no matching container in pod %s", pod.Name)
				continue
			}

			// One PodStream per matched container
			for _, c := range containers {
				result = append(result, PodStream{
					Namespace: target.Namespace,
					PodName:   pod.Name,
					Container: c,
				})
			}
		}
	}

	return result, nil
}

// pickContainers selects containers from the pod spec matching the given patterns.
// If no patterns are provided, auto-picks the first non-sidecar container.
func pickContainers(pod corev1.Pod, patterns []*regexp.Regexp) []string {
	// If patterns provided, return all matching containers
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
