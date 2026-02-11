package nodetopology

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	controllersmetrics "k8s.io/component-base/metrics/prometheus/controllers"
	"k8s.io/klog/v2"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

const (
	ControllerName = controllerName("node_topology_controller")
)

type controllerName string

func (c controllerName) String() string {
	return string(c)
}

func (c controllerName) dashed() string {
	return strings.ReplaceAll(string(c), "_", "-")
}

// NodeTopologyConfig controls the behaviour of the NodeTopologyController.
type NodeTopologyConfig struct {
	// Enabled activates the controller. Default false.
	Enabled bool `yaml:"enabled"`
	// InfraClusterName is a human-readable name for the infra cluster,
	// stamped into node.kubevirt.io/infra-cluster on every tenant node.
	InfraClusterName string `yaml:"infraClusterName"`
	// RackLabelKey is the label key on infra nodes that carries rack info.
	// Defaults to "topology.kubernetes.io/rack".
	RackLabelKey string `yaml:"rackLabelKey"`
}

// vmiTopologyCacheEntry stores the last-known state of a VMI that is
// relevant for topology label decisions.
type vmiTopologyCacheEntry struct {
	NodeName       string
	Phase          kubevirtv1.VirtualMachineInstancePhase
	MigrationState *kubevirtv1.VirtualMachineInstanceMigrationState
}

// Controller watches VMIs in the infra cluster and keeps physical topology
// labels on the corresponding tenant cluster nodes accurate — even after
// live migration, restart, or deletion.
type Controller struct {
	tenantClient kubernetes.Interface
	infraClient  kubernetes.Interface

	infraDynamic        dynamic.Interface
	infraDynamicFactory dynamicinformer.DynamicSharedInformerFactory
	infraNodeFactory    informers.SharedInformerFactory

	infraNamespace string
	config         NodeTopologyConfig

	queue workqueue.RateLimitingInterface

	vmiCacheMu sync.RWMutex
	vmiCache   map[string]*vmiTopologyCacheEntry

	resyncInterval time.Duration
}

// NewController creates a new NodeTopologyController.
func NewController(
	tenantClient kubernetes.Interface,
	infraClient kubernetes.Interface,
	infraDynamic dynamic.Interface,
	infraNamespace string,
	config NodeTopologyConfig,
) *Controller {
	infraDynamicFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		infraDynamic, 0, infraNamespace, nil)
	// Node informer is cluster-scoped (no namespace filter).
	infraNodeFactory := informers.NewSharedInformerFactory(infraClient, 0)

	return &Controller{
		tenantClient:        tenantClient,
		infraClient:         infraClient,
		infraDynamic:        infraDynamic,
		infraDynamicFactory: infraDynamicFactory,
		infraNodeFactory:    infraNodeFactory,
		infraNamespace:      infraNamespace,
		config:              config,
		queue:               workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		vmiCache:            make(map[string]*vmiTopologyCacheEntry),
		resyncInterval:      30 * time.Second,
	}
}

// syncRequest is queued for every tenant node that needs a label update.
type syncRequest struct {
	// tenantNodeName is the name of the tenant node to reconcile.
	// It equals the VMI name (providerID = kubevirt://<vmi-name>).
	tenantNodeName string
}

// Init sets up informers for VMIs (dynamic) and infra nodes.
func (c *Controller) Init() error {
	// --- VMI informer (same pattern as kubevirteps) ---
	vmiGVR := schema.GroupVersionResource{
		Group:    "kubevirt.io",
		Version:  "v1",
		Resource: "virtualmachineinstances",
	}
	_, err := c.infraDynamicFactory.ForResource(vmiGVR).Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.handleVMIEvent(obj, nil)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.handleVMIEvent(newObj, oldObj)
		},
		DeleteFunc: func(obj interface{}) {
			c.handleVMIEvent(nil, obj)
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add VMI event handler: %w", err)
	}

	// --- Infra Node informer ---
	// When an infra node's topology labels change (e.g. rack relabeled),
	// we need to update all tenant nodes whose VMI runs on that infra node.
	_, err = c.infraNodeFactory.Core().V1().Nodes().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.handleInfraNodeUpdate(oldObj, newObj)
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add infra Node event handler: %w", err)
	}

	return nil
}

// Run starts workers and blocks until stopCh is closed.
func (c *Controller) Run(numWorkers int, stopCh <-chan struct{}, controllerManagerMetrics *controllersmetrics.ControllerManagerMetrics) {
	defer utilruntime.HandleCrash()

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	defer c.queue.ShutDown()

	klog.Infof("Starting %s", ControllerName)
	defer klog.Infof("Shutting down %s", ControllerName)
	controllerManagerMetrics.ControllerStarted(ControllerName.String())
	defer controllerManagerMetrics.ControllerStopped(ControllerName.String())

	c.infraDynamicFactory.Start(stopCh)
	c.infraNodeFactory.Start(stopCh)

	vmiGVR := schema.GroupVersionResource{
		Group:    "kubevirt.io",
		Version:  "v1",
		Resource: "virtualmachineinstances",
	}

	if !cache.WaitForNamedCacheSync(ControllerName.String(), stopCh,
		c.infraDynamicFactory.ForResource(vmiGVR).Informer().HasSynced,
		c.infraNodeFactory.Core().V1().Nodes().Informer().HasSynced,
	) {
		return
	}

	klog.Infof("%s informers synced, starting %d workers", ControllerName, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	// Periodic resync as safety net.
	go c.runPeriodicResync(stopCh)

	<-stopCh
}

// ---------- VMI event handling ----------

func (c *Controller) handleVMIEvent(newObj, oldObj interface{}) {
	// --- Delete ---
	if newObj == nil && oldObj != nil {
		un := toUnstructured(oldObj)
		if un == nil {
			return
		}
		vmiName := un.GetName()
		klog.Infof("VMI %s deleted, removing topology labels from tenant node", vmiName)

		c.vmiCacheMu.Lock()
		delete(c.vmiCache, vmiName)
		c.vmiCacheMu.Unlock()

		c.enqueue(vmiName)
		return
	}

	// --- Add / Update ---
	newUn := toUnstructured(newObj)
	if newUn == nil {
		return
	}
	newVMI := &kubevirtv1.VirtualMachineInstance{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(newUn.Object, newVMI); err != nil {
		klog.Errorf("Failed to convert VMI: %v", err)
		return
	}

	var oldVMI *kubevirtv1.VirtualMachineInstance
	if oldObj != nil {
		if oldUn := toUnstructured(oldObj); oldUn != nil {
			oldVMI = &kubevirtv1.VirtualMachineInstance{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(oldUn.Object, oldVMI); err != nil {
				oldVMI = nil
			}
		}
	}

	if c.isRelevantChange(newVMI, oldVMI) {
		klog.Infof("VMI %s changed (phase=%s node=%s migrating=%v), queueing tenant node update",
			newVMI.Name, newVMI.Status.Phase, newVMI.Status.NodeName, isMigrating(newVMI))
		c.updateCache(newVMI)
		c.enqueue(newVMI.Name)
	}
}

func (c *Controller) isRelevantChange(newVMI, oldVMI *kubevirtv1.VirtualMachineInstance) bool {
	if oldVMI == nil {
		return true
	}
	if newVMI.Status.Phase != oldVMI.Status.Phase {
		return true
	}
	if newVMI.Status.NodeName != oldVMI.Status.NodeName {
		return true
	}
	oldMig := oldVMI.Status.MigrationState != nil
	newMig := newVMI.Status.MigrationState != nil
	if oldMig != newMig {
		return true
	}
	if newMig && oldMig {
		if migrationCompleted(newVMI) != migrationCompleted(oldVMI) ||
			migrationFailed(newVMI) != migrationFailed(oldVMI) {
			return true
		}
	}
	return false
}

// ---------- Infra node event handling ----------

func (c *Controller) handleInfraNodeUpdate(oldObj, newObj interface{}) {
	oldNode, ok := oldObj.(*corev1.Node)
	if !ok {
		return
	}
	newNode, ok := newObj.(*corev1.Node)
	if !ok {
		return
	}

	// Only care about topology label changes on the infra node.
	if !topologyLabelsChanged(oldNode, newNode, c.config.RackLabelKey) {
		return
	}

	klog.Infof("Infra node %s topology labels changed, queueing affected tenant nodes", newNode.Name)

	// Find all VMIs currently running on this infra node.
	c.vmiCacheMu.RLock()
	defer c.vmiCacheMu.RUnlock()
	for vmiName, entry := range c.vmiCache {
		if entry.NodeName == newNode.Name {
			c.enqueue(vmiName)
		}
	}
}

func topologyLabelsChanged(oldNode, newNode *corev1.Node, rackKey string) bool {
	keys := []string{
		corev1.LabelTopologyRegion,
		corev1.LabelTopologyZone,
		DefaultRackLabelKey,
	}
	if rackKey != "" && rackKey != DefaultRackLabelKey {
		keys = append(keys, rackKey)
	}
	for _, k := range keys {
		if oldNode.Labels[k] != newNode.Labels[k] {
			return true
		}
	}
	return false
}

// ---------- Worker / queue ----------

func (c *Controller) enqueue(tenantNodeName string) {
	c.queue.Add(&syncRequest{tenantNodeName: tenantNodeName})
}

func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *Controller) processNextItem(ctx context.Context) bool {
	item, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(item)

	req := item.(*syncRequest)
	err := c.syncNode(ctx, req.tenantNodeName)
	if err == nil {
		c.queue.Forget(item)
	} else if c.queue.NumRequeues(item) < 15 {
		klog.Warningf("Error syncing tenant node %s: %v (will retry)", req.tenantNodeName, err)
		c.queue.AddRateLimited(item)
	} else {
		klog.Errorf("Dropping tenant node %s after too many retries: %v", req.tenantNodeName, err)
		c.queue.Forget(item)
		utilruntime.HandleError(err)
	}
	return true
}

// ---------- Reconciliation ----------

// syncNode is the main reconciliation function. It reads the VMI and infra
// node state and patches the tenant node's labels accordingly.
func (c *Controller) syncNode(ctx context.Context, tenantNodeName string) error {
	// Lookup VMI in infra cluster. The VMI name == tenant node name.
	vmiGVR := schema.GroupVersionResource{
		Group:    "kubevirt.io",
		Version:  "v1",
		Resource: "virtualmachineinstances",
	}

	obj, err := c.infraDynamic.Resource(vmiGVR).Namespace(c.infraNamespace).Get(ctx, tenantNodeName, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			// VMI is gone — clean up labels on the tenant node.
			klog.Infof("VMI %s not found, removing topology labels from tenant node", tenantNodeName)
			return removeTopologyLabels(ctx, c.tenantClient, tenantNodeName, c.config.RackLabelKey)
		}
		return fmt.Errorf("failed to get VMI %s: %w", tenantNodeName, err)
	}

	vmi := &kubevirtv1.VirtualMachineInstance{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, vmi); err != nil {
		return fmt.Errorf("failed to convert VMI %s: %w", tenantNodeName, err)
	}

	// Update cache.
	c.updateCache(vmi)

	// If VMI has no nodeName yet (scheduling), nothing to do.
	if vmi.Status.NodeName == "" {
		klog.V(4).Infof("VMI %s has no nodeName yet, skipping", tenantNodeName)
		return nil
	}

	// Handle migration state.
	if isMigrating(vmi) {
		klog.Infof("VMI %s is migrating, adding migrating annotation to tenant node", tenantNodeName)
		return setMigratingAnnotation(ctx, c.tenantClient, tenantNodeName, true)
	}

	// Not migrating — resolve infra node and apply labels.
	infraNode, err := c.infraClient.CoreV1().Nodes().Get(ctx, vmi.Status.NodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get infra node %s: %w", vmi.Status.NodeName, err)
	}

	labels := buildTopologyLabels(infraNode, c.config)

	// Apply labels and remove migrating annotation in one patch.
	patchLabels := make(map[string]*string, len(labels))
	for k, v := range labels {
		val := v
		patchLabels[k] = &val
	}
	patchAnnotations := map[string]*string{
		AnnotationMigrating: nil, // remove if present
	}

	return patchNodeLabelsAndAnnotations(ctx, c.tenantClient, tenantNodeName, patchLabels, patchAnnotations)
}

// ---------- Periodic resync ----------

func (c *Controller) runPeriodicResync(stopCh <-chan struct{}) {
	ticker := time.NewTicker(c.resyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			c.triggerFullResync()
		}
	}
}

func (c *Controller) triggerFullResync() {
	klog.V(4).Infof("Periodic resync: queueing all cached VMIs")
	c.vmiCacheMu.RLock()
	defer c.vmiCacheMu.RUnlock()
	for vmiName := range c.vmiCache {
		c.enqueue(vmiName)
	}
}

// ---------- Helpers ----------

func (c *Controller) updateCache(vmi *kubevirtv1.VirtualMachineInstance) {
	c.vmiCacheMu.Lock()
	defer c.vmiCacheMu.Unlock()
	c.vmiCache[vmi.Name] = &vmiTopologyCacheEntry{
		NodeName:       vmi.Status.NodeName,
		Phase:          vmi.Status.Phase,
		MigrationState: vmi.Status.MigrationState,
	}
}

func isMigrating(vmi *kubevirtv1.VirtualMachineInstance) bool {
	ms := vmi.Status.MigrationState
	if ms == nil {
		return false
	}
	return !ms.Completed && !ms.Failed
}

func migrationCompleted(vmi *kubevirtv1.VirtualMachineInstance) bool {
	return vmi.Status.MigrationState != nil && vmi.Status.MigrationState.Completed
}

func migrationFailed(vmi *kubevirtv1.VirtualMachineInstance) bool {
	return vmi.Status.MigrationState != nil && vmi.Status.MigrationState.Failed
}

// toUnstructured handles both *unstructured.Unstructured and
// cache.DeletedFinalStateUnknown (tombstone).
func toUnstructured(obj interface{}) *unstructured.Unstructured {
	if un, ok := obj.(*unstructured.Unstructured); ok {
		return un
	}
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if un, ok := tombstone.Obj.(*unstructured.Unstructured); ok {
			return un
		}
	}
	klog.Errorf("Failed to cast object to *unstructured.Unstructured: %T", obj)
	return nil
}
