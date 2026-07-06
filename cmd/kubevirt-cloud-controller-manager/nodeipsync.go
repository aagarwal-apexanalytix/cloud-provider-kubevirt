package main

import (
	"context"
	"fmt"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/cloud-provider/app"
	"k8s.io/cloud-provider/app/config"
	genericcontrollermanager "k8s.io/controller-manager/app"
	"k8s.io/controller-manager/controller"
	"k8s.io/klog/v2"
	"kubevirt.io/cloud-provider-kubevirt/pkg/controller/nodeipsync"
	kubevirt "kubevirt.io/cloud-provider-kubevirt/pkg/provider"
)

// StartNodeIPSyncControllerWrapper adapts startNodeIPSyncController to the cloud-provider InitFunc contract.
func StartNodeIPSyncControllerWrapper(initContext app.ControllerInitContext, completedConfig *config.CompletedConfig, cloud cloudprovider.Interface) app.InitFunc {
	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startNodeIPSyncController(controllerContext, completedConfig, cloud)
	}
}

// startNodeIPSyncController wires the event-driven node-IP fast-sync controller. It only runs when
// instancesV2 is enabled (the controller is meaningless otherwise — instancesV2 is what sets node addresses).
// The client bootstrap mirrors the kubevirteps controller (tenant kubeconfig + infra dynamic client).
func startNodeIPSyncController(
	controllerContext genericcontrollermanager.ControllerContext,
	ccmConfig *config.CompletedConfig,
	cloud cloudprovider.Interface) (controller.Interface, bool, error) {

	klog.Infof("Starting %s.", nodeipsync.ControllerName)

	kubevirtCloud, ok := cloud.(*kubevirt.Cloud)
	if !ok {
		return nil, false, fmt.Errorf("%s does not support %v provider", nodeipsync.ControllerName, cloud.ProviderName())
	}

	if !kubevirtCloud.GetCloudConfig().InstancesV2.Enabled {
		klog.Infof("%s is disabled: instancesV2 is not enabled (node addresses are not managed by this CCM).", nodeipsync.ControllerName)
		return nil, false, nil
	}

	// Tenant client (patches tenant Nodes) — same kubeconfig the cloud-node-controller uses.
	tenantClient, err := kubernetes.NewForConfig(ccmConfig.Complete().Kubeconfig)
	if err != nil {
		return nil, false, err
	}

	// Infra rest config (watches VMIs), from the infra kubeconfig or in-cluster — mirrors kubevirteps.go.
	var restConfig *rest.Config
	if kubevirtCloud.GetCloudConfig().Kubeconfig == "" {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			klog.Errorf("Failed to get in-cluster config: %v", err)
			return nil, false, err
		}
	} else {
		infraKubeConfig, kerr := kubevirtCloud.GetInfraKubeconfig()
		if kerr != nil {
			klog.Errorf("Failed to get infra kubeconfig: %v", kerr)
			return nil, false, kerr
		}
		clientConfig, cerr := clientcmd.NewClientConfigFromBytes([]byte(infraKubeConfig))
		if cerr != nil {
			klog.Errorf("Failed to create client config from infra kubeconfig: %v", cerr)
			return nil, false, cerr
		}
		restConfig, err = clientConfig.ClientConfig()
		if err != nil {
			klog.Errorf("Failed to create rest config for infra cluster: %v", err)
			return nil, false, err
		}
	}

	infraDynamic, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		klog.Errorf("Failed to create dynamic client for infra cluster: %v", err)
		return nil, false, err
	}

	c := nodeipsync.NewNodeIPSyncController(tenantClient, infraDynamic, kubevirtCloud.Namespace())
	klog.Infof("Running %s", nodeipsync.ControllerName)
	go c.Run(1, controllerContext.Stop)

	return nil, false, nil
}
