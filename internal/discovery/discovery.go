package discovery

import (
	"context"
	"fmt"
	"log"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Discover performs a one-shot listing of pods matching the configured targets.
// Used for initial discovery or debugging. For continuous tracking, use Controller.
func Discover(ctx context.Context, clientset *kubernetes.Clientset, targets []Target) ([]PodStream, error) {
	var result []PodStream

	for _, target := range targets {
		podRegexes, err := compilePatterns(target.PodPatterns)
		if err != nil {
			return nil, fmt.Errorf("invalid pod_patterns: %w", err)
		}

		containerRegexes, err := compilePatterns(target.ContainerPatterns)
		if err != nil {
			return nil, fmt.Errorf("invalid container_patterns: %w", err)
		}

		pods, err := clientset.CoreV1().Pods(target.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("[discovery] failed to list pods in %s: %v", target.Namespace, err)
			continue
		}

		for _, pod := range pods.Items {
			if pod.Status.Phase != corev1.PodRunning {
				continue
			}
			if !matchesAny(pod.Name, podRegexes) {
				continue
			}

			containers := pickContainers(pod, containerRegexes)
			if len(containers) == 0 {
				log.Printf("[discovery] no matching container in pod %s/%s", target.Namespace, pod.Name)
				continue
			}

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
