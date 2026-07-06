// Package nodeipsync provides an event-driven controller that keeps a tenant Node's InternalIP in sync with
// its backing KubeVirt VMI's current launcher-pod IP — IMMEDIATELY on a live migration or VM restart, rather
// than waiting up to the cloud-node-controller's periodic --node-status-update-frequency (default 5m).
//
// Why: for a pod-network (masquerade) worker, the node InternalIP is the launcher-pod IP reported on the VMI
// interface named "default" (see provider.NodeAddressesFromVMIInterfaces). A live migration moves the VMI to
// a new launcher pod (new pod IP), and a VM restart recreates the launcher pod (new pod IP) — but the stock
// cloud-node-controller only re-derives addresses for an ALREADY-INITIALIZED node in its periodic loop (its
// event path early-returns to label reconcile). So the node IP can be stale for up to the poll interval.
//
// This controller ADDS a fast path (it does NOT replace the periodic loop — both derive from the same source
// via provider.NodeAddressesFromVMIInterfaces and converge): it watches infra VMIs and, on any change that
// alters the "default"-interface IP, patches the matching tenant Node's addresses within seconds. It is
// idempotent (no-op when the InternalIP already matches) and namespace-scoped to the tenant's infra namespace.
package nodeipsync

import (
	"context"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	clientretry "k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	kubevirtv1 "kubevirt.io/api/core/v1"

	"kubevirt.io/cloud-provider-kubevirt/pkg/provider"
)

// ControllerName is the log/identity name of this controller.
const ControllerName = "kubevirt_node_ip_sync_controller"

// vmiGVR is the infra-cluster VirtualMachineInstance resource this controller watches.
var vmiGVR = schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachineinstances"}

// Controller watches infra VMIs and fast-syncs the matching tenant Node's InternalIP.
type Controller struct {
	tenantClient   kubernetes.Interface // patches tenant Nodes (same identity the cloud-node-controller uses)
	infraDynamic   dynamic.Interface
	infraNamespace string
	infraFactory   dynamicinformer.DynamicSharedInformerFactory
	vmiInformer    cache.SharedIndexInformer
	queue          workqueue.RateLimitingInterface
}

// NewNodeIPSyncController wires the infra VMI informer (namespace-scoped) + the tenant node client.
func NewNodeIPSyncController(tenantClient kubernetes.Interface, infraDynamic dynamic.Interface, infraNamespace string) *Controller {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(infraDynamic, 0, infraNamespace, nil)
	vmiInformer := factory.ForResource(vmiGVR).Informer()
	c := &Controller{
		tenantClient:   tenantClient,
		infraDynamic:   infraDynamic,
		infraNamespace: infraNamespace,
		infraFactory:   factory,
		vmiInformer:    vmiInformer,
		queue:          workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
	}
	_, _ = vmiInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueue,
		UpdateFunc: func(_, newObj interface{}) { c.enqueue(newObj) },
		// No DeleteFunc: a deleted VMI means the node is going away (drain/replace) — the tenant node object
		// is removed by CAPI, not something we patch.
	})
	return c
}

// enqueue keys work by the VMI name (which is the tenant node name for operator/pipeline-provisioned workers).
func (c *Controller) enqueue(obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		if tomb, ok2 := obj.(cache.DeletedFinalStateUnknown); ok2 {
			u, ok = tomb.Obj.(*unstructured.Unstructured)
		}
		if !ok {
			return
		}
	}
	c.queue.Add(u.GetName())
}

// Run starts the informer and workers. Signature mirrors the kubevirteps controller (workers, stop, metrics).
func (c *Controller) Run(workers int, stop <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()
	klog.Infof("Starting %s.", ControllerName)
	c.infraFactory.Start(stop)
	if !cache.WaitForCacheSync(stop, c.vmiInformer.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("%s: failed to sync VMI informer cache", ControllerName))
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stop)
	}
	<-stop
	klog.Infof("Stopping %s.", ControllerName)
}

func (c *Controller) worker() {
	for c.processNext() {
	}
}

func (c *Controller) processNext() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)
	if err := c.sync(context.TODO(), key.(string)); err != nil {
		utilruntime.HandleError(fmt.Errorf("%s: sync %q: %w", ControllerName, key, err))
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

// sync derives the node InternalIP from the VMI's "default" interface and patches the tenant Node if it drifted.
func (c *Controller) sync(ctx context.Context, vmiName string) error {
	// Read the VMI from the informer CACHE (not a live GET) — the event that enqueued this key was delivered
	// from that cache, so it is the right source and avoids an API round-trip per event.
	obj, exists, err := c.vmiInformer.GetIndexer().GetByKey(c.infraNamespace + "/" + vmiName)
	if err != nil {
		return err
	}
	if !exists {
		return nil // VMI gone (drain/replace) — nothing to sync
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("unexpected cache object type %T for VMI %q", obj, vmiName)
	}
	var vmi kubevirtv1.VirtualMachineInstance
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &vmi); err != nil {
		return fmt.Errorf("convert VMI %q: %w", vmiName, err)
	}
	// Only act on a running VMI — a Scheduling/Pending/migrating VMI has no stable launcher-pod IP yet.
	if vmi.Status.Phase != kubevirtv1.Running {
		return nil
	}

	// Patch under RetryOnConflict — the periodic cloud-node-controller loop writes the same node.status.
	// addresses, so a concurrent write can conflict; re-read + recompute on conflict (both paths converge).
	return clientretry.RetryOnConflict(clientretry.DefaultRetry, func() error {
		// The tenant node name == VMI name for operator/pipeline workers. If no such node exists (hostname
		// override, or node not yet joined), skip — the periodic cloud-node-controller loop still covers it.
		node, err := c.tenantClient.CoreV1().Nodes().Get(ctx, vmiName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		// Desired InternalIP(s) from the VMI "default" interface (same derivation the cloud-node-controller
		// uses), with the node's current addresses as the fallback source.
		desired := provider.NodeAddressesFromVMIInterfaces(vmi.Status.Interfaces, node.Status.Addresses)
		if len(internalIPs(desired)) == 0 {
			return nil // nothing to set (don't wipe an address to empty)
		}
		merged := mergeInternalIPs(node.Status.Addresses, desired)
		if reflect.DeepEqual(node.Status.Addresses, merged) {
			return nil // already correct — idempotent no-op
		}
		updated := node.DeepCopy()
		updated.Status.Addresses = merged
		if _, err := c.tenantClient.CoreV1().Nodes().UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
			return err
		}
		klog.Infof("%s: fast-synced node %q InternalIP to %v (VMI launcher-pod IP changed)", ControllerName, vmiName, internalIPs(desired))
		return nil
	})
}

// internalIPs returns just the InternalIP address strings from a NodeAddress slice.
func internalIPs(addrs []corev1.NodeAddress) []string {
	var out []string
	for _, a := range addrs {
		if a.Type == corev1.NodeInternalIP {
			out = append(out, a.Address)
		}
	}
	return out
}

// mergeInternalIPs replaces the node's InternalIP addresses with the desired ones while PRESERVING every
// non-InternalIP address (Hostname, ExternalIP) and their relative order — mirroring how the cloud-node-
// controller merges cloud-provided addresses with kubelet-provided ones. Desired InternalIPs are appended
// after the preserved addresses.
func mergeInternalIPs(current, desired []corev1.NodeAddress) []corev1.NodeAddress {
	out := make([]corev1.NodeAddress, 0, len(current)+len(desired))
	for _, a := range current {
		if a.Type != corev1.NodeInternalIP {
			out = append(out, a)
		}
	}
	for _, a := range desired {
		if a.Type == corev1.NodeInternalIP {
			out = append(out, a)
		}
	}
	return out
}
