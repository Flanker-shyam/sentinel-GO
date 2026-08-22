package k8s

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewClientset creates a Kubernetes clientset.
// It uses the kubeconfig at ~/.kube/config (populated by saml2aws login).
// Falls back to in-cluster config if kubeconfig is not found.
func NewClientset() (*kubernetes.Clientset, error) {
	config, err := loadConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create k8s client: %w", err)
	}

	return clientset, nil
}

func loadConfig() (*rest.Config, error) {
	// Try kubeconfig first (local dev / saml2aws login flow)
	home, err := os.UserHomeDir()
	if err == nil {
		kubeconfigPath := filepath.Join(home, ".kube", "config")
		if _, err := os.Stat(kubeconfigPath); err == nil {
			return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		}
	}

	// Fallback to in-cluster config (running inside a pod)
	return rest.InClusterConfig()
}
