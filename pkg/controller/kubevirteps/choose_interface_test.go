package kubevirteps

import (
	"testing"

	kubevirtv1 "kubevirt.io/api/core/v1"
)

// TestChooseVMIInterface pins the prefer-"pod"-else-"default" selection that used to be a build-time sed
// patch in the iac image pipeline and now lives here as tested code.
func TestChooseVMIInterface(t *testing.T) {
	iface := func(name string) kubevirtv1.VirtualMachineInstanceNetworkInterface {
		return kubevirtv1.VirtualMachineInstanceNetworkInterface{Name: name}
	}
	for _, tc := range []struct {
		name string
		ifs  []kubevirtv1.VirtualMachineInstanceNetworkInterface
		want string // expected chosen interface name; "" = nil
	}{
		{"pod present (dual-NIC, pod first)", []kubevirtv1.VirtualMachineInstanceNetworkInterface{iface("pod"), iface("default")}, "pod"},
		{"pod present but default listed first (dual-NIC) — pod still wins", []kubevirtv1.VirtualMachineInstanceNetworkInterface{iface("default"), iface("pod")}, "pod"},
		{"only default (pod-only worker, masquerade named default)", []kubevirtv1.VirtualMachineInstanceNetworkInterface{iface("default")}, "default"},
		{"only pod", []kubevirtv1.VirtualMachineInstanceNetworkInterface{iface("pod")}, "pod"},
		{"neither pod nor default", []kubevirtv1.VirtualMachineInstanceNetworkInterface{iface("eth1"), iface("net2")}, ""},
		{"empty", nil, ""},
		{"default + unrelated, no pod", []kubevirtv1.VirtualMachineInstanceNetworkInterface{iface("eth1"), iface("default")}, "default"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := chooseVMIInterface(tc.ifs)
			if tc.want == "" {
				if got != nil {
					t.Errorf("want nil, got %q", got.Name)
				}
				return
			}
			if got == nil || got.Name != tc.want {
				t.Errorf("want %q, got %v", tc.want, got)
			}
		})
	}
}
