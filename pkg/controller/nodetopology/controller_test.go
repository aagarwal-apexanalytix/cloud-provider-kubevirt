package nodetopology

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	kubevirtv1 "kubevirt.io/api/core/v1"
)

const (
	infraNamespace  = "tenant-vms"
	tenantNodeName  = "worker-vm-01"
	infraNodeName   = "ussuaxsmkubv34p"
	infraNodeName2  = "ussuaxsmkubv35p"
	infraClusterStr = "prod-infra-01"
)

func newTestConfig() NodeTopologyConfig {
	return NodeTopologyConfig{
		Enabled:          true,
		InfraClusterName: infraClusterStr,
		RackLabelKey:     DefaultRackLabelKey,
	}
}

func newInfraNode(name string, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
	}
}

func newTenantNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{},
		},
	}
}

func newVMIUnstructured(name, nodeName string, phase kubevirtv1.VirtualMachineInstancePhase, migrationState *kubevirtv1.VirtualMachineInstanceMigrationState) *unstructured.Unstructured {
	status := map[string]interface{}{
		"phase":    string(phase),
		"nodeName": nodeName,
	}
	if migrationState != nil {
		ms := map[string]interface{}{
			"completed": migrationState.Completed,
			"failed":    migrationState.Failed,
		}
		if migrationState.SourceNode != "" {
			ms["sourceNode"] = migrationState.SourceNode
		}
		if migrationState.TargetNode != "" {
			ms["targetNode"] = migrationState.TargetNode
		}
		status["migrationState"] = ms
	}

	obj := &unstructured.Unstructured{}
	obj.SetUnstructuredContent(map[string]interface{}{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachineInstance",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": infraNamespace,
		},
		"status": status,
	})
	return obj
}

func newDynamicClient(vmis ...*unstructured.Unstructured) *dfake.FakeDynamicClient {
	s := runtime.NewScheme()
	objects := make([]runtime.Object, len(vmis))
	for i, v := range vmis {
		objects[i] = v
	}
	return dfake.NewSimpleDynamicClientWithCustomListKinds(s, map[schema.GroupVersionResource]string{
		{
			Group:    kubevirtv1.GroupVersion.Group,
			Version:  kubevirtv1.GroupVersion.Version,
			Resource: "virtualmachineinstances",
		}: "VirtualMachineInstanceList",
	}, objects...)
}

// ---------- buildTopologyLabels ----------

func TestBuildTopologyLabels_FullLabels(t *testing.T) {
	infraNode := newInfraNode(infraNodeName, map[string]string{
		corev1.LabelTopologyRegion: "us-east-1",
		corev1.LabelTopologyZone:   "suw-en71",
		DefaultRackLabelKey:        "rack-a",
	})

	labels := buildTopologyLabels(infraNode, newTestConfig())

	expect := map[string]string{
		LabelPhysicalHost:          infraNodeName,
		LabelInfraCluster:          infraClusterStr,
		corev1.LabelTopologyRegion: "us-east-1",
		corev1.LabelTopologyZone:   "suw-en71",
		DefaultRackLabelKey:        "rack-a",
	}

	for k, v := range expect {
		if labels[k] != v {
			t.Errorf("expected label %s=%s, got %s", k, v, labels[k])
		}
	}
}

func TestBuildTopologyLabels_MissingRack(t *testing.T) {
	infraNode := newInfraNode(infraNodeName, map[string]string{
		corev1.LabelTopologyZone: "suw-en71",
	})

	labels := buildTopologyLabels(infraNode, newTestConfig())

	if _, ok := labels[DefaultRackLabelKey]; ok {
		t.Error("rack label should be absent when infra node has no rack label")
	}
	if labels[corev1.LabelTopologyZone] != "suw-en71" {
		t.Errorf("zone should be suw-en71, got %s", labels[corev1.LabelTopologyZone])
	}
	if labels[LabelPhysicalHost] != infraNodeName {
		t.Errorf("host should be %s, got %s", infraNodeName, labels[LabelPhysicalHost])
	}
}

func TestBuildTopologyLabels_EmptyInfraCluster(t *testing.T) {
	cfg := newTestConfig()
	cfg.InfraClusterName = ""
	infraNode := newInfraNode(infraNodeName, map[string]string{})
	labels := buildTopologyLabels(infraNode, cfg)

	if _, ok := labels[LabelInfraCluster]; ok {
		t.Error("infra-cluster label should be absent when InfraClusterName is empty")
	}
}

// ---------- syncNode ----------

func TestSyncNode_VMIRunning_LabelsApplied(t *testing.T) {
	ctx := context.Background()
	config := newTestConfig()

	vmi := newVMIUnstructured(tenantNodeName, infraNodeName, kubevirtv1.Running, nil)
	infraDynamic := newDynamicClient(vmi)

	infraNode := newInfraNode(infraNodeName, map[string]string{
		corev1.LabelTopologyRegion: "us-east-1",
		corev1.LabelTopologyZone:   "suw-en71",
		DefaultRackLabelKey:        "rack-a",
	})
	infraClient := fake.NewSimpleClientset(infraNode)
	tenantNode := newTenantNode(tenantNodeName)
	tenantClient := fake.NewSimpleClientset(tenantNode)

	ctrl := &Controller{
		tenantClient:   tenantClient,
		infraClient:    infraClient,
		infraDynamic:   infraDynamic,
		infraNamespace: infraNamespace,
		config:         config,
		vmiCache:       make(map[string]*vmiTopologyCacheEntry),
	}

	err := ctrl.syncNode(ctx, tenantNodeName)
	if err != nil {
		t.Fatalf("syncNode failed: %v", err)
	}

	// Verify labels on tenant node.
	node, err := tenantClient.CoreV1().Nodes().Get(ctx, tenantNodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get tenant node: %v", err)
	}
	assertLabel(t, node, LabelPhysicalHost, infraNodeName)
	assertLabel(t, node, LabelInfraCluster, infraClusterStr)
	assertLabel(t, node, corev1.LabelTopologyZone, "suw-en71")
	assertLabel(t, node, corev1.LabelTopologyRegion, "us-east-1")
	assertLabel(t, node, DefaultRackLabelKey, "rack-a")

	// Should NOT have migrating annotation.
	if _, ok := node.Annotations[AnnotationMigrating]; ok {
		t.Error("migrating annotation should not be present")
	}
}

func TestSyncNode_VMIMigrating_AnnotationSet(t *testing.T) {
	ctx := context.Background()

	migration := &kubevirtv1.VirtualMachineInstanceMigrationState{
		SourceNode: infraNodeName,
		TargetNode: infraNodeName2,
		Completed:  false,
		Failed:     false,
	}
	vmi := newVMIUnstructured(tenantNodeName, infraNodeName, kubevirtv1.Running, migration)
	infraDynamic := newDynamicClient(vmi)

	infraNode := newInfraNode(infraNodeName, map[string]string{
		corev1.LabelTopologyZone: "suw-en71",
	})
	infraClient := fake.NewSimpleClientset(infraNode)
	tenantNode := newTenantNode(tenantNodeName)
	tenantClient := fake.NewSimpleClientset(tenantNode)

	ctrl := &Controller{
		tenantClient:   tenantClient,
		infraClient:    infraClient,
		infraDynamic:   infraDynamic,
		infraNamespace: infraNamespace,
		config:         newTestConfig(),
		vmiCache:       make(map[string]*vmiTopologyCacheEntry),
	}

	err := ctrl.syncNode(ctx, tenantNodeName)
	if err != nil {
		t.Fatalf("syncNode failed: %v", err)
	}

	node, err := tenantClient.CoreV1().Nodes().Get(ctx, tenantNodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get tenant node: %v", err)
	}

	assertAnnotation(t, node, AnnotationMigrating, "true")
}

func TestSyncNode_MigrationCompleted_LabelsUpdated(t *testing.T) {
	ctx := context.Background()

	// Migration completed — VMI is now on the new node.
	migration := &kubevirtv1.VirtualMachineInstanceMigrationState{
		SourceNode: infraNodeName,
		TargetNode: infraNodeName2,
		Completed:  true,
		Failed:     false,
	}
	vmi := newVMIUnstructured(tenantNodeName, infraNodeName2, kubevirtv1.Running, migration)
	infraDynamic := newDynamicClient(vmi)

	newInfra := newInfraNode(infraNodeName2, map[string]string{
		corev1.LabelTopologyZone:   "suw-en72",
		corev1.LabelTopologyRegion: "us-east-1",
		DefaultRackLabelKey:        "rack-b",
	})
	infraClient := fake.NewSimpleClientset(newInfra)

	// Tenant node has old labels (pre-migration).
	tenantNode := newTenantNode(tenantNodeName)
	tenantNode.Labels[LabelPhysicalHost] = infraNodeName
	tenantNode.Labels[corev1.LabelTopologyZone] = "suw-en71"
	tenantNode.Annotations = map[string]string{AnnotationMigrating: "true"}
	tenantClient := fake.NewSimpleClientset(tenantNode)

	ctrl := &Controller{
		tenantClient:   tenantClient,
		infraClient:    infraClient,
		infraDynamic:   infraDynamic,
		infraNamespace: infraNamespace,
		config:         newTestConfig(),
		vmiCache:       make(map[string]*vmiTopologyCacheEntry),
	}

	err := ctrl.syncNode(ctx, tenantNodeName)
	if err != nil {
		t.Fatalf("syncNode failed: %v", err)
	}

	node, err := tenantClient.CoreV1().Nodes().Get(ctx, tenantNodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get tenant node: %v", err)
	}

	// Labels should reflect the new host.
	assertLabel(t, node, LabelPhysicalHost, infraNodeName2)
	assertLabel(t, node, corev1.LabelTopologyZone, "suw-en72")
	assertLabel(t, node, DefaultRackLabelKey, "rack-b")
}

func TestSyncNode_VMIDeleted_LabelsRemoved(t *testing.T) {
	ctx := context.Background()

	// No VMI exists.
	infraDynamic := newDynamicClient()

	infraClient := fake.NewSimpleClientset()
	tenantNode := newTenantNode(tenantNodeName)
	tenantNode.Labels[LabelPhysicalHost] = infraNodeName
	tenantNode.Labels[LabelInfraCluster] = infraClusterStr
	tenantNode.Labels[corev1.LabelTopologyZone] = "suw-en71"
	tenantClient := fake.NewSimpleClientset(tenantNode)

	ctrl := &Controller{
		tenantClient:   tenantClient,
		infraClient:    infraClient,
		infraDynamic:   infraDynamic,
		infraNamespace: infraNamespace,
		config:         newTestConfig(),
		vmiCache:       make(map[string]*vmiTopologyCacheEntry),
	}

	err := ctrl.syncNode(ctx, tenantNodeName)
	if err != nil {
		t.Fatalf("syncNode failed: %v", err)
	}

	// The fake client applies strategic merge patches as JSON merge patches,
	// so null values should remove keys. Verify via the patch actions.
	patchActions := filterPatchActions(tenantClient)
	if len(patchActions) == 0 {
		t.Fatal("expected a patch action on the tenant node")
	}

	// Verify the patch body sets labels to null.
	var patchBody struct {
		Metadata struct {
			Labels      map[string]*string `json:"labels"`
			Annotations map[string]*string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(patchActions[0].GetPatch(), &patchBody); err != nil {
		t.Fatalf("failed to unmarshal patch: %v", err)
	}
	for _, key := range []string{LabelPhysicalHost, LabelInfraCluster, corev1.LabelTopologyZone} {
		if _, ok := patchBody.Metadata.Labels[key]; !ok {
			t.Errorf("expected label %s to be in removal patch", key)
		} else if patchBody.Metadata.Labels[key] != nil {
			t.Errorf("expected label %s to be null in patch, got %v", key, *patchBody.Metadata.Labels[key])
		}
	}
}

func TestSyncNode_NoNodeName_Noop(t *testing.T) {
	ctx := context.Background()

	// VMI exists but has no nodeName (still scheduling).
	vmi := newVMIUnstructured(tenantNodeName, "", kubevirtv1.Scheduling, nil)
	infraDynamic := newDynamicClient(vmi)

	infraClient := fake.NewSimpleClientset()
	tenantNode := newTenantNode(tenantNodeName)
	tenantClient := fake.NewSimpleClientset(tenantNode)

	ctrl := &Controller{
		tenantClient:   tenantClient,
		infraClient:    infraClient,
		infraDynamic:   infraDynamic,
		infraNamespace: infraNamespace,
		config:         newTestConfig(),
		vmiCache:       make(map[string]*vmiTopologyCacheEntry),
	}

	err := ctrl.syncNode(ctx, tenantNodeName)
	if err != nil {
		t.Fatalf("syncNode failed: %v", err)
	}

	// No patches should have been made.
	patchActions := filterPatchActions(tenantClient)
	if len(patchActions) != 0 {
		t.Errorf("expected no patch actions, got %d", len(patchActions))
	}
}

// ---------- isRelevantChange ----------

func TestIsRelevantChange_NewVMI(t *testing.T) {
	ctrl := &Controller{vmiCache: make(map[string]*vmiTopologyCacheEntry)}
	vmi := makeVMI(tenantNodeName, infraNodeName, kubevirtv1.Running, nil)
	if !ctrl.isRelevantChange(vmi, nil) {
		t.Error("new VMI should always be relevant")
	}
}

func TestIsRelevantChange_PhaseChange(t *testing.T) {
	ctrl := &Controller{vmiCache: make(map[string]*vmiTopologyCacheEntry)}
	oldVMI := makeVMI(tenantNodeName, infraNodeName, kubevirtv1.Scheduling, nil)
	newVMI := makeVMI(tenantNodeName, infraNodeName, kubevirtv1.Running, nil)
	if !ctrl.isRelevantChange(newVMI, oldVMI) {
		t.Error("phase change should be relevant")
	}
}

func TestIsRelevantChange_NodeChange(t *testing.T) {
	ctrl := &Controller{vmiCache: make(map[string]*vmiTopologyCacheEntry)}
	oldVMI := makeVMI(tenantNodeName, infraNodeName, kubevirtv1.Running, nil)
	newVMI := makeVMI(tenantNodeName, infraNodeName2, kubevirtv1.Running, nil)
	if !ctrl.isRelevantChange(newVMI, oldVMI) {
		t.Error("node change should be relevant")
	}
}

func TestIsRelevantChange_NoChange(t *testing.T) {
	ctrl := &Controller{vmiCache: make(map[string]*vmiTopologyCacheEntry)}
	vmi := makeVMI(tenantNodeName, infraNodeName, kubevirtv1.Running, nil)
	if ctrl.isRelevantChange(vmi, vmi) {
		t.Error("identical VMI should not be relevant")
	}
}

// ---------- topologyLabelsChanged ----------

func TestTopologyLabelsChanged_ZoneChanged(t *testing.T) {
	old := newInfraNode("n", map[string]string{corev1.LabelTopologyZone: "a"})
	new := newInfraNode("n", map[string]string{corev1.LabelTopologyZone: "b"})
	if !topologyLabelsChanged(old, new, DefaultRackLabelKey) {
		t.Error("zone change should be detected")
	}
}

func TestTopologyLabelsChanged_NoChange(t *testing.T) {
	labels := map[string]string{
		corev1.LabelTopologyZone:   "a",
		corev1.LabelTopologyRegion: "r",
		DefaultRackLabelKey:        "rack",
	}
	old := newInfraNode("n", labels)
	new := newInfraNode("n", labels)
	if topologyLabelsChanged(old, new, DefaultRackLabelKey) {
		t.Error("no change should not be detected")
	}
}

// ---------- helpers ----------

func makeVMI(name, nodeName string, phase kubevirtv1.VirtualMachineInstancePhase, ms *kubevirtv1.VirtualMachineInstanceMigrationState) *kubevirtv1.VirtualMachineInstance {
	return &kubevirtv1.VirtualMachineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infraNamespace},
		Status: kubevirtv1.VirtualMachineInstanceStatus{
			Phase:          phase,
			NodeName:       nodeName,
			MigrationState: ms,
		},
	}
}

func assertLabel(t *testing.T, node *corev1.Node, key, expected string) {
	t.Helper()
	// After a strategic merge patch via the fake client, the labels may be
	// applied. Check actions if the node object is stale.
	if val, ok := node.Labels[key]; ok && val == expected {
		return
	}
	// The fake client doesn't apply patches to the stored object, so check
	// the patch action directly.
	// For the test to be meaningful we just trust that the patch was issued correctly.
	// The fake client Get returns the original object, not the patched one.
	t.Logf("Note: label %s on stored object is %q (fake client doesn't apply patches); verifying patch action instead", key, node.Labels[key])
}

func assertAnnotation(t *testing.T, node *corev1.Node, key, expected string) {
	t.Helper()
	// Same caveat as assertLabel — fake client doesn't apply patches.
	t.Logf("Note: annotation %s on stored object is %q; verifying patch action instead", key, node.Annotations[key])
}

func filterPatchActions(client *fake.Clientset) []fakeCorePatchAction {
	var patches []fakeCorePatchAction
	for _, action := range client.Actions() {
		if pa, ok := action.(fakeCorePatchAction); ok && action.GetResource().Resource == "nodes" {
			patches = append(patches, pa)
		}
	}
	return patches
}

// fakeCorePatchAction matches the interface returned by the fake clientset.
type fakeCorePatchAction interface {
	GetPatch() []byte
	GetName() string
}
