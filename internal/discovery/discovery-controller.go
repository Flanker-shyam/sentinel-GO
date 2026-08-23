package discovery

import (
	"context"
	"log"
	"regexp"
	"sync"

	"github.com/Flanker-shyam/sentinel-GO/internal/streamer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

type Controller struct {
	clientset *kubernetes.Clientset
	targets []Target
	tailLines int64
	streamChan chan <- string

	mu sync.Mutex
	active map[string]context.CancelFunc
}

func NewController(clientset *kubernetes.Clientset, targets []Target, taillines int64, streamChan chan<-string) *Controller{
	return &Controller{
		clientset: clientset,
		targets: targets,
		tailLines: taillines,
		streamChan: streamChan,
		active: make(map[string]context.CancelFunc),
	}
}

func (c * Controller) watchNamespace(ctx context.Context, namespace string){
	for{
		if ctx.Err() != nil {
			return
		}

		// LIST - get current state and reconcile
		podList, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("[Discovery] failed to list pods in %s: %v", namespace, err)
			continue
		}

		c.reconcile(ctx, namespace, podList.Items)

		//WATCH - starting from the resource version of the last LIST
		watcher, err := c.clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
			ResourceVersion: podList.ResourceVersion,
		})
		if err != nil {
			log.Printf("[Discovery] failed to watch pods in %s: %v", namespace, err)
			continue
		}

		// Process events until watch expires or ctx is done
		c.handleEvents(ctx, watcher, namespace)
		watcher.Stop()

		//watch expired, loop back to LIST
		log.Printf("[Discovery] watch expired for namespace %s, re-listing pods", namespace)
	}
}

func (c *Controller) reconcile(ctx context.Context, namespace string, pods []corev1.Pod){
	// Collect what should be active.
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
				PodName: pod.Name,
				Container: container,
			}
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for key, ps := range desired {
		if _, exists := c.active[key]; !exists {
			c.startStream(ctx, key, ps)
		}
	}

	for key := range c.active {
		if _, exists := desired[key]; !exists {
			if belongsToNamespace(key, namespace) {
				c.stopStream(key)
			}
		}
	}
}

func belongsToNamespace(key, namespace string) bool {
	//key format: namespace/pod/container
	return len(key) > len(namespace) && key[:len(namespace)] == namespace && key[len(namespace)] == '/'
}

func (c *Controller) handleEvents(ctx context.Context, watcher watch.Interface, namespace string){
	for{
		select {
		case <- ctx.Done():
			return
		case event, ok := <-watcher.ResultChan():
			if !ok{
				log.Printf("[Discovery] watch channel closed for namespace %s", namespace)
				return
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}

			switch event.Type{
			case watch.Added, watch.Modified:
				c.handlePodUpdate(ctx, namespace, pod)
			case watch.Deleted:
				c.handlePodDelete(namespace, pod)
			}
		}
	}
}

func (c *Controller) handlePodUpdate(ctx context.Context, namespace string, pod *corev1.Pod){
	if pod.Status.Phase == corev1.PodRunning && c.matchesPod(namespace, pod.Name){
		containers := c.pickContainers(*pod)
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, container := range containers{
			key := podKey(namespace, pod.Name, container)
			if _, exists := c.active[key]; !exists{
				c.startStream(ctx, key, PodStream{
					Namespace: namespace,
					PodName: pod.Name,
					Container: container,
				})
			}
		}
	}else{
		c.handlePodDelete(namespace, pod)
	}
}

func (c *Controller) handlePodDelete(namespace string, pod *corev1.Pod){
	containers := c.pickContainers(*pod)
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, container := range containers{
		key := podKey(namespace, pod.Name, container)
		c.stopStream(key)
	}
}

func (c *Controller) startStream(ctx context.Context, key string, ps PodStream){
	podCtx, cancel := context.WithCancel(ctx)
	c.active[key] = cancel

	log.Printf("[Discovery] -> start streaming %s", key)

	go func(){
		defer func(){
			//Clean up on exit (stream broke, pod died, etc..)
			c.mu.Lock()
			delete(c.active, key)
			defer c.mu.Unlock()
			log.Printf("[Discovery] -> stopped streaming %s", key)
		}()
		streamer.Stream(podCtx, c.clientset, ps, c.tailLines, c.streamChan)
	}()
}

func (c *Controller) stopStream(key string){
	if cancel, ok := c.active[key]; ok{
		cancel()
		delete(c.active, key)
	}
}

func (c *Controller) Run(ctx context.Context){
	namespace := c.uniqueNamespaces()
	 var wg sync.WaitGroup
	 for _, ns := range namespace{
		wg.Add(1)
		go func(namespace string){
			defer wg.Done()
			c.watchNamespace(ctx, namespace)
		}(ns)
	 }
	 wg.Wait()
}
	
func (c * Controller) uniqueNamespaces() []string {
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


//helpers:

func podKey(namespace, podName, container string) string {
	return namespace + "/" + podName + "/" + container
}

func (c* Controller) matchesPod(namespace, podName string) bool{
	for _, t := range c.targets{
		if t.Namespace != namespace{
			continue
		}
		for _, pattern := range t.PodPatterns{
			re, err := regexp.Compile("^"+pattern+"$")
			if err != nil{
				continue
			}
			if re.MatchString(podName){
				return true
			}
		}
	}
	return false
}

func (c * Controller) pickContainers(pod corev1.Pod) []string{
	for _, t := range c.targets{
		if t.Namespace != pod.Namespace {
			continue
		}

		matched := false 
		for _, pattern := range t.PodPatterns {
			re, err := regexp.Compile("^"+pattern+"$")
			if err != nil {
				continue
			}
			if re.MatchString(pod.Name){
				matched = true
				break
			}
		}
		if !matched {
			continue
		}

		if len(t.ContainerPatterns) > 0 {
			var result []string

			for _, c := range pod.Spec.Containers{
				for _, cp := range t.ContainerPatterns{
					re, err := regexp.Compile("^" + cp + "$")
					if err != nil {
						continue
					}
					if re.MatchString(c.Name){
						result = append(result, c.Name)
						break
					}
				}
			}
			return result
		}

		for _, c := range pod.Spec.Containers{
			if !knownSidecars[c.Name]{
				return []string{c.Name}
			}
		}
		if len(pod.Spec.Containers)>0{
			return []string{pod.Spec.Containers[0].Name}
		}
	}
	return nil
}