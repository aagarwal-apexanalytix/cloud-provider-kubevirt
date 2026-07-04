package provider

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"
	kubevirtv1 "kubevirt.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// ProviderName is the name of the kubevirt provider
	ProviderName = "kubevirt"
)

var scheme = runtime.NewScheme()

func init() {
	cloudprovider.RegisterCloudProvider(ProviderName, kubevirtCloudProviderFactory)
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := kubevirtv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
}

type Cloud struct {
	namespace string
	client    client.Client
	config    CloudConfig
}

type CloudConfig struct {
	Kubeconfig   string             `yaml:"kubeconfig"`
	LoadBalancer LoadBalancerConfig `yaml:"loadBalancer"`
	InstancesV2  InstancesV2Config  `yaml:"instancesV2"`
	NodeTopology NodeTopologyConfig `yaml:"nodeTopology"`
	Namespace    string             `yaml:"namespace"`
	InfraLabels  map[string]string  `yaml:"infraLabels"`
}

// NodeTopologyConfig controls the NodeTopologyController which keeps
// physical host topology labels on tenant cluster nodes accurate.
type NodeTopologyConfig struct {
	// Enabled activates the controller. Default false.
	Enabled bool `yaml:"enabled"`
	// InfraClusterName is stamped into node.kubevirt.io/infra-cluster.
	InfraClusterName string `yaml:"infraClusterName"`
	// RackLabelKey is the label on infra nodes carrying rack info.
	// Defaults to "topology.kubernetes.io/rack".
	RackLabelKey string `yaml:"rackLabelKey"`
}

type LoadBalancerConfig struct {
	// Enabled activates the load balancer interface of the CCM
	Enabled bool `yaml:"enabled"`

	// CreationPollInterval determines how many seconds to wait for the load balancer creation between retries
	CreationPollInterval *int `yaml:"creationPollInterval,omitempty"`

	// CreationPollTimeout determines how many seconds to wait for the load balancer creation
	CreationPollTimeout *int `yaml:"creationPollTimeout,omitempty"`

	// Selectorless delegate endpointslices creation on third party by
	// skipping service selector creation
	Selectorless *bool `yaml:"selectorless,omitempty"`

	// EnableEPSController determines if the EPS controller is enabled
	// This is a temporary flag to enable/disable the EPS controller
	// When disabled the service selector is used.
	EnableEPSController *bool `yaml:"enableEPSController,omitempty"`

	// AllocationOnly puts the CCM into "allocate but do not advertise" mode for this tenant.
	// The infra (mirror) Service is still created so infra LB-IPAM assigns an address that is
	// written back to the tenant Service, but the mirror is forced to
	// externalTrafficPolicy=Local and left selectorless with NO EndpointSlices (the EPS
	// controller is disabled) — so the infra CNI (e.g. Cilium BGP) withdraws the route.
	// Use when the TENANT cluster advertises its own LoadBalancer IPs (e.g. single-NIC
	// routable tenants running their own Cilium BGP), to avoid a competing infra BGP path.
	// Independent of the tenant Service's externalTrafficPolicy; applies to all LB Services.
	AllocationOnly *bool `yaml:"allocationOnly,omitempty"`

	// AllocationOnlyLBClass, when non-empty AND AllocationOnly is set, is written as the mirror
	// Service's spec.loadBalancerClass — but PER-SERVICE: only for LB Services whose tenant Service
	// designated a valid (parseable) spec.loadBalancerIP. A class that no infra LB provider serves (e.g.
	// "io.kubevirt/allocation-only") makes Cilium/MetalLB/Kube-VIP ignore that mirror entirely: the
	// infra CNI programs NO local datapath for the IP (so infra-cluster pods route it out to the
	// fabric and reach the tenant's own BGP-advertised edge, instead of self-hijacking it) and
	// reserves NO address for it. For such a Service the tenant owns allocation + BGP advertisement,
	// and the CCM reports its spec.loadBalancerIP as the LB status without waiting for infra
	// allocation. LB Services WITHOUT a tenant spec.loadBalancerIP (e.g. internal mesh Services that
	// take an infra-pool address) keep the legacy allocation-only path untouched — so this can be
	// enabled fleet-wide without disturbing infra-allocated LoadBalancers. Empty (default) keeps
	// legacy allocation-only for every Service (infra allocates + reserves the IP but withdraws the
	// route via etp=Local + no EndpointSlices). Provider-agnostic.
	//
	// The discriminator is purely "did the tenant designate a spec.loadBalancerIP" — so an internal
	// LB Service that sets a static spec.loadBalancerIP also receives the class (its infra datapath
	// is suppressed, and it is expected to be advertised by the tenant's own BGP), by the same
	// tenant-owns-the-designated-IP contract as an edge Service. Services that leave loadBalancerIP
	// unset (infra auto-assigns from a pool) always stay on the legacy path.
	AllocationOnlyLBClass *string `yaml:"allocationOnlyLBClass,omitempty"`
}

type InstancesV2Config struct {
	// Enabled activates the instances interface of the CCM
	Enabled bool `yaml:"enabled"`
	// ZoneAndRegionEnabled indicates if need to get Region and zone labels from the cloud provider
	ZoneAndRegionEnabled bool `yaml:"zoneAndRegionEnabled"`
}

// createDefaultCloudConfig creates a CloudConfig object filled with default values.
// These default values should be overwritten by values read from the cloud-config file.
func createDefaultCloudConfig() CloudConfig {
	return CloudConfig{
		LoadBalancer: LoadBalancerConfig{
			Enabled:              true,
			CreationPollInterval: pointer.Int(int(defaultLoadBalancerCreatePollInterval.Seconds())),
			CreationPollTimeout:  pointer.Int(int(defaultLoadBalancerCreatePollTimeout.Seconds())),
		},
		InstancesV2: InstancesV2Config{
			Enabled:              true,
			ZoneAndRegionEnabled: true,
		},
	}
}

func NewCloudConfigFromBytes(configBytes []byte) (CloudConfig, error) {
	var config = createDefaultCloudConfig()
	err := yaml.Unmarshal(configBytes, &config)
	if err != nil {
		return CloudConfig{}, err
	}
	return config, nil
}

func kubevirtCloudProviderFactory(config io.Reader) (cloudprovider.Interface, error) {
	if config == nil {
		return nil, fmt.Errorf("No %s cloud provider config file given", ProviderName)
	}

	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(config)
	if err != nil {
		return nil, fmt.Errorf("Failed to read cloud provider config: %v", err)
	}
	cloudConf, err := NewCloudConfigFromBytes(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal cloud provider config: %v", err)
	}
	namespace := cloudConf.Namespace
	var restConfig *rest.Config
	if cloudConf.Kubeconfig == "" {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	} else {
		var infraKubeConfig string
		infraKubeConfig, err = GetInfraKubeConfig(cloudConf.Kubeconfig)
		if err != nil {
			return nil, err
		}
		var clientConfig clientcmd.ClientConfig
		clientConfig, err = clientcmd.NewClientConfigFromBytes([]byte(infraKubeConfig))
		if err != nil {
			return nil, err
		}
		restConfig, err = clientConfig.ClientConfig()
		if err != nil {
			return nil, err
		}
		if namespace == "" {
			namespace, _, err = clientConfig.Namespace()
			if err != nil {
				klog.Errorf("Could not find namespace in client config: %v", err)
				return nil, err
			}
		}
	}
	c, err := client.New(restConfig, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, err
	}
	return &Cloud{
		namespace: namespace,
		client:    c,
		config:    cloudConf,
	}, nil
}

// Initialize provides the Cloud with a kubernetes client builder and may spawn goroutines
// to perform housekeeping activities within the Cloud provider.
func (c *Cloud) Initialize(clientBuilder cloudprovider.ControllerClientBuilder, stop <-chan struct{}) {
}

// LoadBalancer returns a balancer interface. Also returns true if the interface is supported, false otherwise.
func (c *Cloud) LoadBalancer() (cloudprovider.LoadBalancer, bool) {
	if !c.config.LoadBalancer.Enabled {
		return nil, false
	}
	return &loadbalancer{
		namespace:   c.namespace,
		client:      c.client,
		config:      c.config.LoadBalancer,
		infraLabels: c.config.InfraLabels,
	}, true
}

// Instances returns an instances interface. Also returns true if the interface is supported, false otherwise.
func (c *Cloud) Instances() (cloudprovider.Instances, bool) {
	return nil, false
}

func (c *Cloud) InstancesV2() (cloudprovider.InstancesV2, bool) {
	if !c.config.InstancesV2.Enabled {
		return nil, false
	}
	return &instancesV2{
		namespace: c.namespace,
		client:    c.client,
		config:    &c.config.InstancesV2,
	}, true
}

// Zones returns a zones interface. Also returns true if the interface is supported, false otherwise.
// DEPRECATED: Zones is deprecated in favor of retrieving zone/region information from InstancesV2.
func (c *Cloud) Zones() (cloudprovider.Zones, bool) {
	return nil, false
}

// Clusters returns a clusters interface.  Also returns true if the interface is supported, false otherwise.
func (c *Cloud) Clusters() (cloudprovider.Clusters, bool) {
	return nil, false
}

// Routes returns a routes interface along with whether the interface is supported.
func (c *Cloud) Routes() (cloudprovider.Routes, bool) {
	return nil, false
}

// ProviderName returns the Cloud provider ID.
func (c *Cloud) ProviderName() string {
	return ProviderName
}

// HasClusterID returns true if a ClusterID is required and set
func (c *Cloud) HasClusterID() bool {
	return true
}

func (c *Cloud) GetInfraKubeconfig() (string, error) {
	return GetInfraKubeConfig(c.config.Kubeconfig)
}

func (c *Cloud) Namespace() string {
	return c.namespace
}

func (c *Cloud) GetCloudConfig() CloudConfig {
	return c.config
}

func GetInfraKubeConfig(infraKubeConfigPath string) (string, error) {
	config, err := os.Open(infraKubeConfigPath)
	if err != nil {
		return "", fmt.Errorf("Couldn't open infra-kubeconfig: %v", err)
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(config)
	if err != nil {
		return "", fmt.Errorf("Failed to read infra-kubeconfig: %v", err)
	}
	return buf.String(), nil
}
