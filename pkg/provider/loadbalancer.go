package provider

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// Default interval between polling the service after creation
	defaultLoadBalancerCreatePollInterval = 5 * time.Second

	// Default timeout between polling the service after creation
	defaultLoadBalancerCreatePollTimeout = 5 * time.Minute

	TenantServiceNameLabelKey      = "cluster.x-k8s.io/tenant-service-name"
	TenantServiceNamespaceLabelKey = "cluster.x-k8s.io/tenant-service-namespace"
	TenantClusterNameLabelKey      = "cluster.x-k8s.io/cluster-name"
	TenantNodeRoleLabelKey         = "cluster.x-k8s.io/role"
)

type loadbalancer struct {
	namespace   string
	client      client.Client
	config      LoadBalancerConfig
	infraLabels map[string]string
}

// GetLoadBalancer returns whether the specified load balancer exists, and
// if so, what its status is.
// Implementations must treat the *v1.Service parameter as read-only and not modify it.
// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager
func (lb *loadbalancer) GetLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service) (status *corev1.LoadBalancerStatus, exists bool, err error) {
	lbName := lb.GetLoadBalancerName(ctx, clusterName, service)
	lbService, err := lb.getLoadBalancerService(ctx, lbName)
	if err != nil {
		klog.Errorf("Failed to get LoadBalancer service: %v", err)
		return nil, false, err
	}
	if lbService == nil {
		return nil, false, nil
	}

	status = &lbService.Status.LoadBalancer
	return status, true, nil
}

// GetLoadBalancerName is an implementation of LoadBalancer.GetLoadBalancerName.
func (lb *loadbalancer) GetLoadBalancerName(ctx context.Context, clusterName string, service *corev1.Service) string {
	// TODO: replace DefaultLoadBalancerName to generate more meaningful loadbalancer names.
	return cloudprovider.DefaultLoadBalancerName(service)
}

// EnsureLoadBalancer creates a new load balancer 'name', or updates the existing one. Returns the status of the balancer
// Implementations must treat the *v1.Service and *v1.Node
// parameters as read-only and not modify them.
// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager
func (lb *loadbalancer) EnsureLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) (*corev1.LoadBalancerStatus, error) {
	lbName := lb.GetLoadBalancerName(ctx, clusterName, service)

	lbService, err := lb.getLoadBalancerService(ctx, lbName)
	if err != nil {
		klog.Errorf("Failed to get LoadBalancer service: %v", err)
		return nil, err
	}

	ports := lb.createLoadBalancerServicePorts(service)
	// LoadBalancer already exist, update the ports if changed
	if lbService != nil {
		if err := lb.updateLoadBalancerServicePorts(ctx, lbService, ports); err != nil {
			return nil, err
		}
		// Reconcile allocation-only posture in place (no delete → the allocated IP is preserved)
		// so a mirror created before AllocationOnly was set converges to etp=Local + selectorless.
		if err := lb.ensureAllocationOnlyPosture(ctx, lbService); err != nil {
			return &lbService.Status.LoadBalancer, err
		}
		return &lbService.Status.LoadBalancer, nil
	}

	vmiLabels := map[string]string{
		TenantNodeRoleLabelKey:    "worker",
		TenantClusterNameLabelKey: clusterName,
	}

	lbLabels := map[string]string{}

	// Copy tenant service labels first so they can be overridden by infra/identity labels
	for key, val := range service.Labels {
		lbLabels[key] = val
	}

	// Infrastructure-wide labels (override tenant labels if conflicting)
	for key, val := range lb.infraLabels {
		lbLabels[key] = val
	}

	// Identity labels always take precedence
	lbLabels[TenantServiceNameLabelKey] = service.Name
	lbLabels[TenantServiceNamespaceLabelKey] = service.Namespace
	lbLabels[TenantClusterNameLabelKey] = clusterName

	lbService, err = lb.createLoadBalancerService(ctx, lbName, service, vmiLabels, lbLabels, ports)
	if err != nil {
		klog.Errorf("Failed to create LoadBalancer service: %v", err)
		return nil, err
	}

	err = wait.PollWithContext(ctx, lb.getLoadBalancerCreatePollInterval(), lb.getLoadBalancerCreatePollTimeout(), func(ctx context.Context) (bool, error) {
		if len(lbService.Status.LoadBalancer.Ingress) != 0 {
			return true, nil
		}
		var service *corev1.Service
		service, err = lb.getLoadBalancerService(ctx, lbName)
		if err != nil {
			klog.Errorf("Failed to get LoadBalancer service: %v", err)
			return false, err
		}
		if service != nil && len(service.Status.LoadBalancer.Ingress) > 0 {
			lbService = service
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		klog.Errorf("Failed to poll LoadBalancer service: %v", err)
		return nil, err
	}

	return &lbService.Status.LoadBalancer, nil
}

// UpdateLoadBalancer updates the ports in the LoadBalancer Service, if needed
// Implementations must treat the *v1.Service and *v1.Node
// parameters as read-only and not modify them.
// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager
func (lb *loadbalancer) UpdateLoadBalancer(ctx context.Context, clusterName string, service *corev1.Service, nodes []*corev1.Node) error {
	lbName := lb.GetLoadBalancerName(ctx, clusterName, service)
	var lbService corev1.Service
	if err := lb.client.Get(ctx, client.ObjectKey{Name: lbName, Namespace: lb.namespace}, &lbService); err != nil {
		if errors.IsNotFound(err) {
			klog.Errorf("Service %s doesn't exist in namespace %s: %v", lbName, lb.namespace, err)
			return err
		}
		klog.Errorf("Failed to get Service %s in namespace %s: %v", lbName, lb.namespace, err)
		return err
	}

	ports := lb.createLoadBalancerServicePorts(service)
	// LoadBalancer already exist, update the ports if changed
	return lb.updateLoadBalancerServicePorts(ctx, &lbService, ports)
}

func (lb *loadbalancer) updateLoadBalancerServicePorts(ctx context.Context, lbService *corev1.Service, ports []corev1.ServicePort) error {
	if !equality.Semantic.DeepEqual(ports, lbService.Spec.Ports) {
		lbService.Spec.Ports = ports
		if err := lb.client.Update(ctx, lbService); err != nil {
			klog.Errorf("Failed to update LoadBalancer service: %v", err)
			return err
		}
	}
	return nil
}

// ensureAllocationOnlyPosture reconciles an existing mirror Service into the allocation-only
// posture (externalTrafficPolicy=Local + no selector) in place, so a mirror created before
// AllocationOnly was enabled converges without a delete (the allocated IP is preserved). A no-op
// unless AllocationOnly is set. With the EPS controller disabled in this mode, the Service ends up
// with no EndpointSlices, so the infra CNI (Cilium BGP, etp=Local) withdraws the route.
func (lb *loadbalancer) ensureAllocationOnlyPosture(ctx context.Context, lbService *corev1.Service) error {
	if lb.config.AllocationOnly == nil || !*lb.config.AllocationOnly {
		return nil
	}
	changed := false
	if lbService.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyTypeLocal {
		lbService.Spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyTypeLocal
		changed = true
	}
	if lbService.Spec.Selector != nil {
		lbService.Spec.Selector = nil
		changed = true
	}
	if !changed {
		return nil
	}
	if err := lb.client.Update(ctx, lbService); err != nil {
		klog.Errorf("Failed to reconcile allocation-only posture for LoadBalancer service %s: %v", lbService.Name, err)
		return err
	}
	return nil
}

// EnsureLoadBalancerDeleted deletes the specified load balancer if it
// exists, returning nil if the load balancer specified either didn't exist or
// was successfully deleted.
// This construction is useful because many cloud providers' load balancers
// have multiple underlying components, meaning a Get could say that the LB
// doesn't exist even if some part of it is still laying around.
// Implementations must treat the *v1.Service parameter as read-only and not modify it.
// Parameter 'clusterName' is the name of the cluster as presented to kube-controller-manager
func (lb *loadbalancer) EnsureLoadBalancerDeleted(ctx context.Context, clusterName string, service *corev1.Service) error {
	lbName := lb.GetLoadBalancerName(ctx, clusterName, service)

	lbService, err := lb.getLoadBalancerService(ctx, lbName)
	if err != nil {
		klog.Errorf("Failed to get LoadBalancer service: %v", err)
		return err
	}
	if lbService != nil {
		if err = lb.client.Delete(ctx, lbService); err != nil {
			klog.Errorf("Failed to delete LoadBalancer service: %v", err)
			return err
		}
	}

	return nil
}

func (lb *loadbalancer) getLoadBalancerService(ctx context.Context, lbName string) (*corev1.Service, error) {
	var service corev1.Service
	if err := lb.client.Get(ctx, client.ObjectKey{Name: lbName, Namespace: lb.namespace}, &service); err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &service, nil
}

func (lb *loadbalancer) createLoadBalancerService(ctx context.Context, lbName string, service *corev1.Service, vmiLabels map[string]string, lbLabels map[string]string, ports []corev1.ServicePort) (*corev1.Service, error) {
	lbService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        lbName,
			Namespace:   lb.namespace,
			Annotations: service.Annotations,
			Labels:      lbLabels,
		},
		Spec: corev1.ServiceSpec{
			Ports:                 ports,
			Type:                  corev1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy: service.Spec.ExternalTrafficPolicy,
		},
	}
	// AllocationOnly takes precedence: allocate the infra LB IP but do not advertise it (the
	// tenant advertises its own). Force externalTrafficPolicy=Local and leave the Service
	// selectorless (no EndpointSlices, since the EPS controller is disabled in this mode) so the
	// infra CNI withdraws the route regardless of the tenant Service's externalTrafficPolicy.
	if lb.config.AllocationOnly != nil && *lb.config.AllocationOnly {
		lbService.Spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyTypeLocal
		lbService.Spec.Selector = nil
	} else if lb.config.EnableEPSController != nil && *lb.config.EnableEPSController && service.Spec.ExternalTrafficPolicy == corev1.ServiceExternalTrafficPolicyTypeLocal {
		// Give controller privilege above selectorless
		lbService.Spec.Selector = nil
	} else if lb.config.Selectorless != nil && *lb.config.Selectorless {
		lbService.Spec.Selector = nil
	} else {
		lbService.Spec.Selector = vmiLabels
	}
	if len(service.Spec.ExternalIPs) > 0 {
		lbService.Spec.ExternalIPs = service.Spec.ExternalIPs
	}
	if service.Spec.LoadBalancerIP != "" {
		lbService.Spec.LoadBalancerIP = service.Spec.LoadBalancerIP
	}
	// Do NOT copy the tenant's HealthCheckNodePort onto the infra Service: tenant nodePort spaces are
	// independent per cluster, so pinning the tenant's value on the SHARED infra cluster collides with
	// other tenants' allocations ("failed to allocate requested HealthCheck NodePort NNNNN: provided
	// port is already allocated") and the LB stays pending forever. Leave it 0 — the infra apiserver
	// allocates a free port; nothing consumes the infra-side health check port cross-cluster.

	if err := lb.client.Create(ctx, lbService); err != nil {
		klog.Errorf("Failed to create LB %s: %v", lbName, err)
		return nil, err
	}
	return lbService, nil
}

func (lb *loadbalancer) createLoadBalancerServicePorts(service *corev1.Service) []corev1.ServicePort {
	ports := make([]corev1.ServicePort, len(service.Spec.Ports))
	for i, port := range service.Spec.Ports {
		ports[i].Name = port.Name
		ports[i].Protocol = port.Protocol
		ports[i].Port = port.Port
		ports[i].TargetPort = intstr.IntOrString{
			Type:   intstr.Int,
			IntVal: port.NodePort,
		}
	}
	return ports
}

func (lb *loadbalancer) getLoadBalancerCreatePollInterval() time.Duration {
	return convertLoadBalancerCreatePollConfig(lb.config.CreationPollInterval, defaultLoadBalancerCreatePollInterval, "interval")
}

func (lb *loadbalancer) getLoadBalancerCreatePollTimeout() time.Duration {
	return convertLoadBalancerCreatePollConfig(lb.config.CreationPollTimeout, defaultLoadBalancerCreatePollTimeout, "timeout")
}

func convertLoadBalancerCreatePollConfig(configValue *int, defaultValue time.Duration, name string) time.Duration {
	if configValue == nil {
		klog.Infof("Setting creation poll %s to default value '%d'", name, defaultValue)
		return defaultValue
	}
	if *configValue <= 0 {
		klog.Infof("Creation poll %s %d' must be > 0. Setting to '%d'", name, *configValue, defaultValue)
		return defaultValue
	}
	return time.Duration(*configValue) * time.Second

}
