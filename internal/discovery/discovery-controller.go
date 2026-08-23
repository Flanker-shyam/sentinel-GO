package discovery

import (
	"context"
	"log"
	"regexp"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// StreamFunc is the function signature for streaming logs from a pod.
// This avoids a circular import between discovery and streamer packages.
type StreamFunc func(ctx context.Context, clientset *kubernetes.Clientset, pod PodStream, out chan<- string)

// Controller continuously discovers and streams from pods matching configured targets.
type Controller struct {
	clientset  *kubernetes.Clientset
	targets    []Target
	streamFn   StreamFunc
	streamChan chan<- string

	mu     sync.Mutex
	active map[string]context.CancelFunc // "ns/pod/container" → cancel
}

// NewController creates a discovery controller.
func NewController(clientset *kubernetes.Clientset, targets []Target, streamFn StreamFunc, streamChan chan<- string) *Controller {
	return &Controller{
		clientset:  clientset,
		targets:    targets,
		streamFn:   streamFn,
		streamChan: streamChan,
		active:     make(map[string]context.CancelFunc),
	}
}

// Run starts watching all configured namespaces. Blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) {
	namespaces := c.uniqueNamespaces()

	var wg sync.WaitGroup
	for _, ns := range namespaces {
		wg.Add(1)
		go func(namespace string) {
			defer wg.Done()
			c.watchNamespace(ctx, namespace)
		}(ns)
	}

	wg.Wait()
}

// watchNamespace runs the list-then-watch loop for a single namespace.
func (c *Controller) watchNamespace(ctx context.Context, namespace string) {
	for {
		if ctx.Err() != nil {
			return
		}

		// LIST — get current state and reconcile
		podList, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[discovery] failed to list pods in %s: %v", namespace, err)
			continue
		}

		c.reconcile(ctx, namespace, podList.Items)

		// WATCH — starting from the resourceVersion we got from LIST
		watcher, err := c.clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
			ResourceVersion: podList.ResourceVersion,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[discovery] failed to watch %s: %v", namespace, err)
			continue
		}

		// Process events until watch expires or ctx is done
		c.handleEvents(ctx, watcher, namespace)
		watcher.Stop()

		// Watch expired — loop back to LIST
		log.Printf("[discovery] watch expired for %s, re-listing", namespace)
	}
}

// reconcile compares the active map against the current pod list.
// Starts missing goroutines, stops stale ones.
func (c *Controller) reconcile(ctx context.Context, namespace string, pods []corev1.Pod) {
	// Collect what SHOULD be active
	desired := make(map[string]PodStream)
	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		if !c.matchesPod(namespace, pod.Name) {
			continue
		}

		containers := c.pickContainers(pod)
		for _, container := range containers {
			key := podKey(namespace, pod.Name, container)
			desired[key] = PodStream{
				Namespace: namespace,
				PodName:   pod.Name,
				Container: container,
			}
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Start goroutines for pods in desired but not in active
	for key, ps := range desired {
		if _, exists := c.active[key]; !exists {
			c.startStream(ctx, key, ps)
		}
	}

	// Stop goroutines for pods in active but not in desired (stale)
	for key := range c.active {
		if _, exists := desired[key]; !exists {
			if belongsToNamespace(key, namespace) {
				c.stopStream(key)
			}
		}
	}
}

// handleEvents processes watch events until the channel closes or ctx is done.
func (c *Controller) handleEvents(ctx context.Context, watcher watch.Interface, namespace string) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return // watch channel closed (expired)
			}

			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}

			switch event.Type {
			case watch.Added, watch.Modified:
				c.handlePodUpdate(ctx, namespace, pod)
			case watch.Deleted:
				c.handlePodDelete(namespace, pod)
			}
		}
	}
}

func (c *Controller) handlePodUpdate(ctx context.Context, namespace string, pod *corev1.Pod) {
	if pod.Status.Phase == corev1.PodRunning && c.matchesPod(namespace, pod.Name) {
		containers := c.pickContainers(*pod)
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, container := range containers {
			key := podKey(namespace, pod.Name, container)
			if _, exists := c.active[key]; !exists {
				c.startStream(ctx, key, PodStream{
					Namespace: namespace,
					PodName:   pod.Name,
					Container: container,
				})
			}
		}
	} else {
		// Pod is no longer Running (Terminating, Failed, etc.) — stop streaming
		c.handlePodDelete(namespace, pod)
	}
}

func (c *Controller) handlePodDelete(namespace string, pod *corev1.Pod) {
	containers := c.pickContainers(*pod)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, container := range containers {
		key := podKey(namespace, pod.Name, container)
		c.stopStream(key)
	}
}

// startStream launches a streaming goroutine. Caller must hold c.mu.
func (c *Controller) startStream(ctx context.Context, key string, ps PodStream) {
	podCtx, cancel := context.WithCancel(ctx)
	c.active[key] = cancel

	log.Printf("[discovery] ▶ start streaming %s", key)

	go func() {
		defer func() {
			// Clean up on exit (stream broke, pod died, etc.)
			c.mu.Lock()
			delete(c.active, key)
			c.mu.Unlock()
			log.Printf("[discovery] ■ stopped streaming %s", key)
		}()
		c.streamFn(podCtx, c.clientset, ps, c.streamChan)
	}()
}

// stopStream cancels a streaming goroutine. Caller must hold c.mu.
func (c *Controller) stopStream(key string) {
	if cancel, ok := c.active[key]; ok {
		cancel()
		delete(c.active, key)
	}
}

// --- Helpers ---

func podKey(namespace, podName, container string) string {
	return namespace + "/" + podName + "/" + container
}

func belongsToNamespace(key, namespace string) bool {
	return len(key) > len(namespace) && key[:len(namespace)] == namespace && key[len(namespace)] == '/'
}

func (c *Controller) matchesPod(namespace, podName string) bool {
	for _, t := range c.targets {
		if t.Namespace != namespace {
			continue
		}
		for _, pattern := range t.PodPatterns {
			re, err := regexp.Compile("^" + pattern + "$")
			if err != nil {
				continue
			}
			if re.MatchString(podName) {
				return true
			}
		}
	}
	return false
}

func (c *Controller) pickContainers(pod corev1.Pod) []string {
	for _, t := range c.targets {
		if t.Namespace != pod.Namespace {
			continue
		}

		// Check if pod matches this target
		matched := false
		for _, pattern := range t.PodPatterns {
			re, err := regexp.Compile("^" + pattern + "$")
			if err != nil {
				continue
			}
			if re.MatchString(pod.Name) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}

		// Use container patterns from this target
		if len(t.ContainerPatterns) > 0 {
			var result []string
			for _, container := range pod.Spec.Containers {
				for _, cp := range t.ContainerPatterns {
					re, err := regexp.Compile("^" + cp + "$")
					if err != nil {
						continue
					}
					if re.MatchString(container.Name) {
						result = append(result, container.Name)
						break
					}
				}
			}
			return result
		}

		// Auto-pick: first non-sidecar
		for _, container := range pod.Spec.Containers {
			if !knownSidecars[container.Name] {
				return []string{container.Name}
			}
		}
		if len(pod.Spec.Containers) > 0 {
			return []string{pod.Spec.Containers[0].Name}
		}
	}
	return nil
}

func (c *Controller) uniqueNamespaces() []string {
	seen := make(map[string]struct{})
	var result []string
	for _, t := range c.targets {
		if _, ok := seen[t.Namespace]; !ok {
			seen[t.Namespace] = struct{}{}
			result = append(result, t.Namespace)
		}
	}
	return result
}
