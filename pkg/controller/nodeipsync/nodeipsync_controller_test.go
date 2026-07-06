package nodeipsync

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func node(name string, addrs ...corev1.NodeAddress) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.NodeStatus{Addresses: addrs},
	}
}

func hostname(n string) corev1.NodeAddress {
	return corev1.NodeAddress{Type: corev1.NodeHostName, Address: n}
}
func internalIP(ip string) corev1.NodeAddress {
	return corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: ip}
}

// vmiUnstructured builds a minimal Running VMI with a single "default" interface IP.
func vmiUnstructured(name, ns, defaultIP string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "kubevirt.io/v1",
		"kind":       "VirtualMachineInstance",
		"metadata":   map[string]interface{}{"name": name, "namespace": ns},
		"spec":       map[string]interface{}{},
		"status": map[string]interface{}{
			"phase": "Running",
			"interfaces": []interface{}{
				map[string]interface{}{"name": "default", "ipAddress": defaultIP, "ipAddresses": []interface{}{defaultIP}},
			},
		},
	}}
}

func newTestController(t *testing.T, ns string, objs []runtime.Object, vmis ...*unstructured.Unstructured) *Controller {
	t.Helper()
	scheme := runtime.NewScheme()
	gvr := schema.GroupVersionResource{Group: "kubevirt.io", Version: "v1", Resource: "virtualmachineinstances"}
	listKinds := map[schema.GroupVersionResource]string{gvr: "VirtualMachineInstanceList"}
	dynObjs := make([]runtime.Object, 0, len(vmis))
	for _, v := range vmis {
		dynObjs = append(dynObjs, v)
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, dynObjs...)
	return &Controller{
		tenantClient:   k8sfake.NewSimpleClientset(objs...),
		infraDynamic:   dyn,
		infraNamespace: ns,
	}
}

// TestSync_UpdatesNodeIPOnChange: the node's stale InternalIP is replaced with the VMI's current default-
// interface IP, and the Hostname address is preserved (the live-migration / restart / stop-start case).
func TestSync_UpdatesNodeIPOnChange(t *testing.T) {
	ns := "it-test-suw9"
	nodeName := "it-test-suw9-md-0-abc"
	c := newTestController(t, ns,
		[]runtime.Object{node(nodeName, hostname(nodeName), internalIP("10.180.6.42"))}, // OLD launcher-pod IP
		vmiUnstructured(nodeName, ns, "10.180.6.99"),                                    // NEW IP after migration
	)
	if err := c.sync(context.Background(), nodeName); err != nil {
		t.Fatalf("sync: %v", err)
	}
	got, _ := c.tenantClient.CoreV1().Nodes().Get(context.Background(), nodeName, metav1.GetOptions{})
	var ip, host string
	for _, a := range got.Status.Addresses {
		switch a.Type {
		case corev1.NodeInternalIP:
			ip = a.Address
		case corev1.NodeHostName:
			host = a.Address
		}
	}
	if ip != "10.180.6.99" {
		t.Errorf("node InternalIP = %q, want the new launcher-pod IP 10.180.6.99", ip)
	}
	if host != nodeName {
		t.Errorf("Hostname address must be preserved, got %q", host)
	}
}

// TestSync_NoopWhenUnchanged: when the node already has the VMI's IP, no update (idempotent).
func TestSync_NoopWhenUnchanged(t *testing.T) {
	ns := "t"
	n := "t-md-0-x"
	c := newTestController(t, ns,
		[]runtime.Object{node(n, hostname(n), internalIP("10.0.0.5"))},
		vmiUnstructured(n, ns, "10.0.0.5"),
	)
	if err := c.sync(context.Background(), n); err != nil {
		t.Fatalf("sync: %v", err)
	}
	got, _ := c.tenantClient.CoreV1().Nodes().Get(context.Background(), n, metav1.GetOptions{})
	if len(got.Status.Addresses) != 2 {
		t.Errorf("addresses should be unchanged (hostname+internalIP), got %v", got.Status.Addresses)
	}
}

// TestSync_SkipsWhenNoNode: a VMI whose name matches no tenant node is a no-op (periodic loop covers it).
func TestSync_SkipsWhenNoNode(t *testing.T) {
	ns := "t"
	c := newTestController(t, ns, nil, vmiUnstructured("t-md-0-orphan", ns, "10.0.0.9"))
	if err := c.sync(context.Background(), "t-md-0-orphan"); err != nil {
		t.Fatalf("sync must be a no-op when the node is absent, got: %v", err)
	}
}

func TestMergeInternalIPs(t *testing.T) {
	cur := []corev1.NodeAddress{hostname("n1"), internalIP("1.1.1.1")}
	des := []corev1.NodeAddress{internalIP("2.2.2.2")}
	got := mergeInternalIPs(cur, des)
	if len(got) != 2 || got[0] != hostname("n1") || got[1] != internalIP("2.2.2.2") {
		t.Errorf("mergeInternalIPs must preserve hostname + replace InternalIP, got %v", got)
	}
}
