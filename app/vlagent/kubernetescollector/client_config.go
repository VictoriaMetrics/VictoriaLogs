package kubernetescollector

import (
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
	"gopkg.in/yaml.v2"
)

func loadKubeAPIConfig() (*kubeAPIConfig, bool, error) {
	cfg, inClusterErr := loadInClusterConfig()
	if inClusterErr == nil {
		return cfg, false, nil
	}

	cfg, localErr := loadLocalConfig()
	if localErr != nil {
		return nil, false, fmt.Errorf("cannot load discovery config from in-cluster config: %w; and from local config: %w", inClusterErr, localErr)
	}
	return cfg, true, nil
}

// loadInClusterConfig loads Kubernetes API configuration from within a pod running in a Kubernetes cluster.
//
// It uses the service account token and CA certificate mounted by Kubernetes at standard paths
// (/var/run/secrets/kubernetes.io/serviceaccount/) and discovers the API server endpoint through
// KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT environment variables that Kubernetes automatically
// sets for all pods.
func loadInClusterConfig() (*kubeAPIConfig, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if len(host) == 0 || len(port) == 0 {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST/KUBERNETES_SERVICE_PORT environment variables are not set")
	}

	const bearerTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	// Verify that vlagent is running in a Kubernetes cluster.
	if _, err := os.Stat(bearerTokenFile); err != nil {
		return nil, err
	}

	opts := &promauth.Options{
		BearerTokenFile: bearerTokenFile,
		TLSConfig: &promauth.TLSConfig{
			CAFile: "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		},
	}
	ac, err := opts.NewConfig()
	if err != nil {
		return nil, fmt.Errorf("cannot initialize in-cluster auth config: %w", err)
	}

	server := "https://" + net.JoinHostPort(host, port)
	return &kubeAPIConfig{
		server: server,
		ac:     ac,
	}, nil
}

// kubeAPIConfig represents ~/.kube/config file structure.
type kubeConfig struct {
	Clusters []kubeConfigCluster `yaml:"clusters"`

	Users []kubeConfigUser `yaml:"users"`

	Contexts []kubeConfigContext `yaml:"contexts"`

	CurrentContext string `yaml:"current-context"`
}

func (c *kubeConfig) findUser(name string) (kubeConfigUser, bool) {
	for _, u := range c.Users {
		if u.Name == name {
			return u, true
		}
	}
	return kubeConfigUser{}, false
}

func (c *kubeConfig) findContext(context string) (kubeConfigContext, bool) {
	for _, c := range c.Contexts {
		if c.Name == context {
			return c, true
		}
	}
	return kubeConfigContext{}, false
}

func (c *kubeConfig) findCluster(cluster string) (kubeConfigCluster, bool) {
	for _, cl := range c.Clusters {
		if cl.Name == cluster {
			return cl, true
		}
	}
	return kubeConfigCluster{}, false
}

type kubeConfigCluster struct {
	Name    string `yaml:"name"`
	Cluster struct {
		Server                   string `yaml:"server"`
		CertificateAuthority     string `yaml:"certificate-authority"`
		CertificateAuthorityData string `yaml:"certificate-authority-data"`
	} `yaml:"cluster"`
}

type kubeConfigUser struct {
	Name string `yaml:"name"`
	User struct {
		Token                 string `yaml:"token"`
		ClientCertificate     string `yaml:"client-certificate"`
		ClientCertificateData string `yaml:"client-certificate-data"`
		ClientKey             string `yaml:"client-key"`
		ClientKeyData         string `yaml:"client-key-data"`
	} `yaml:"user"`
}

type kubeConfigContext struct {
	Name    string `yaml:"name"`
	Context struct {
		Cluster string `yaml:"cluster"`
		User    string `yaml:"user"`
	} `yaml:"context"`
}

// loadLocalConfig loads Kubernetes API configuration from a local kubeconfig file.
// It reads the kubeconfig file from the KUBECONFIG environment variable (to match kubectl default behavior) or falls back
// to ~/.kube/config if KUBECONFIG is not set.
func loadLocalConfig() (*kubeAPIConfig, error) {
	configPath := os.Getenv("KUBECONFIG")
	if configPath == "" {
		configPath = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}

	rawConfig, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var cfg kubeConfig
	if err := yaml.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("cannot parse yaml %q: %w", configPath, err)
	}

	cctx, ok := cfg.findContext(cfg.CurrentContext)
	if !ok {
		return nil, fmt.Errorf("cannot find current context %q in %q", cfg.CurrentContext, configPath)
	}

	cl, ok := cfg.findCluster(cctx.Context.Cluster)
	if !ok {
		return nil, fmt.Errorf("cannot find cluster %q in %q", cctx.Context.Cluster, configPath)
	}

	tlsCfg := promauth.TLSConfig{}

	if cl.Cluster.CertificateAuthority != "" {
		tlsCfg.CAFile = cl.Cluster.CertificateAuthority
	} else if cl.Cluster.CertificateAuthorityData != "" {
		ca, err := base64.StdEncoding.DecodeString(cl.Cluster.CertificateAuthorityData)
		if err != nil {
			return nil, fmt.Errorf("cannot decode base64 encoded CA certificate data from file %q: %w", configPath, err)
		}
		tlsCfg.CA = string(ca)
	}

	u, ok := cfg.findUser(cctx.Context.User)
	if !ok {
		return nil, fmt.Errorf("cannot find current user %q in %q", cctx.Context.User, configPath)
	}

	if u.User.ClientCertificate != "" {
		tlsCfg.CertFile = u.User.ClientCertificate
	} else if u.User.ClientCertificateData != "" {
		clientCert, err := base64.StdEncoding.DecodeString(u.User.ClientCertificateData)
		if err != nil {
			return nil, fmt.Errorf("cannot decode base64 encoded client certificate data from file %q: %w", configPath, err)
		}
		tlsCfg.Cert = string(clientCert)
	}

	if u.User.ClientKey != "" {
		tlsCfg.KeyFile = u.User.ClientKey
	} else if u.User.ClientKeyData != "" {
		clientCertKey, err := base64.StdEncoding.DecodeString(u.User.ClientKeyData)
		if err != nil {
			return nil, fmt.Errorf("cannot decode base64 encoded client certificate key data from file %q: %w", configPath, err)
		}
		tlsCfg.Key = string(clientCertKey)
	}

	opts := &promauth.Options{
		BearerToken: u.User.Token,
		TLSConfig:   &tlsCfg,
	}
	ac, err := opts.NewConfig()
	if err != nil {
		return nil, fmt.Errorf("cannot initialize local auth config from file %q: %w", configPath, err)
	}

	return &kubeAPIConfig{
		server: cl.Cluster.Server,
		ac:     ac,
	}, nil
}
