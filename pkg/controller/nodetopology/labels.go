package nodetopology

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// LabelPhysicalHost is the infra node name where the VMI is running.
	LabelPhysicalHost = "node.kubevirt.io/physical-host"

	// LabelInfraCluster identifies which infra cluster the VM belongs to.
	LabelInfraCluster = "node.kubevirt.io/infra-cluster"

	// AnnotationMigrating is set on the tenant node while a live migration
	// is in progress, so that workloads can react to the transient state.
	AnnotationMigrating = "node.kubevirt.io/migrating"

	// DefaultRackLabelKey is the default label key on infra nodes that
	// carries rack information. Configurable via NodeTopologyConfig.
	DefaultRackLabelKey = "topology.kubernetes.io/rack"
)

// topologyLabels are the label keys managed by this controller.
// They are removed as a group when a VMI is deleted.
var topologyLabels = []string{
	LabelPhysicalHost,
	LabelInfraCluster,
	corev1.LabelTopologyRegion,
	corev1.LabelTopologyZone,
	DefaultRackLabelKey,
}

// buildTopologyLabels extracts physical topology information from an infra node
// and returns the labels to apply to the corresponding tenant node.
func buildTopologyLabels(infraNode *corev1.Node, config NodeTopologyConfig) map[string]string {
	labels := map[string]string{
		LabelPhysicalHost: infraNode.Name,
	}

	if config.InfraClusterName != "" {
		labels[LabelInfraCluster] = config.InfraClusterName
	}

	// Standard topology labels — refresh from infra node so they stay
	// accurate after live migration.
	if val, ok := infraNode.Labels[corev1.LabelTopologyRegion]; ok {
		labels[corev1.LabelTopologyRegion] = val
	}
	if val, ok := infraNode.Labels[corev1.LabelTopologyZone]; ok {
		labels[corev1.LabelTopologyZone] = val
	}

	// Rack — use the configured label key (falls back to DefaultRackLabelKey).
	rackKey := config.RackLabelKey
	if rackKey == "" {
		rackKey = DefaultRackLabelKey
	}
	if val, ok := infraNode.Labels[rackKey]; ok {
		labels[DefaultRackLabelKey] = val
	}

	return labels
}

// patchNodeLabelsAndAnnotations applies a strategic-merge-patch to the tenant
// node, setting the given labels and annotations. nil map values in the patch
// body cause the key to be removed.
func patchNodeLabelsAndAnnotations(ctx context.Context, client kubernetes.Interface, nodeName string, labels map[string]*string, annotations map[string]*string) error {
	patch := struct {
		Metadata struct {
			Labels      map[string]*string `json:"labels,omitempty"`
			Annotations map[string]*string `json:"annotations,omitempty"`
		} `json:"metadata"`
	}{}
	patch.Metadata.Labels = labels
	patch.Metadata.Annotations = annotations

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal patch for node %s: %w", nodeName, err)
	}

	klog.V(4).Infof("Patching tenant node %s: %s", nodeName, string(patchBytes))

	_, err = client.CoreV1().Nodes().Patch(ctx, nodeName, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch node %s: %w", nodeName, err)
	}
	return nil
}

// applyTopologyLabels sets topology labels on the tenant node.
func applyTopologyLabels(ctx context.Context, client kubernetes.Interface, nodeName string, labels map[string]string) error {
	patchLabels := make(map[string]*string, len(labels))
	for k, v := range labels {
		val := v
		patchLabels[k] = &val
	}
	return patchNodeLabelsAndAnnotations(ctx, client, nodeName, patchLabels, nil)
}

// removeTopologyLabels removes all physical topology labels and the migrating
// annotation from the tenant node.
func removeTopologyLabels(ctx context.Context, client kubernetes.Interface, nodeName string, rackLabelKey string) error {
	patchLabels := make(map[string]*string)
	for _, key := range topologyLabels {
		patchLabels[key] = nil
	}
	// If a custom rack key was configured, also clean that up
	if rackLabelKey != "" && rackLabelKey != DefaultRackLabelKey {
		patchLabels[rackLabelKey] = nil
	}

	patchAnnotations := map[string]*string{
		AnnotationMigrating: nil,
	}

	return patchNodeLabelsAndAnnotations(ctx, client, nodeName, patchLabels, patchAnnotations)
}

// setMigratingAnnotation adds or removes the migrating annotation on the tenant node.
func setMigratingAnnotation(ctx context.Context, client kubernetes.Interface, nodeName string, migrating bool) error {
	annotations := make(map[string]*string)
	if migrating {
		val := "true"
		annotations[AnnotationMigrating] = &val
	} else {
		annotations[AnnotationMigrating] = nil
	}
	return patchNodeLabelsAndAnnotations(ctx, client, nodeName, nil, annotations)
}
