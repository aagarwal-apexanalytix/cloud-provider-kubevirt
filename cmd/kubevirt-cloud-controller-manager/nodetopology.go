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
	"kubevirt.io/cloud-provider-kubevirt/pkg/controller/nodetopology"
	kubevirt "kubevirt.io/cloud-provider-kubevirt/pkg/provider"
)

func StartNodeTopologyControllerWrapper(initContext app.ControllerInitContext, completedConfig *config.CompletedConfig, cloud cloudprovider.Interface) app.InitFunc {
	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startNodeTopologyController(controllerContext, completedConfig, cloud)
	}
}

func startNodeTopologyController(
	controllerContext genericcontrollermanager.ControllerContext,
	ccmConfig *config.CompletedConfig,
	cloud cloudprovider.Interface,
) (controller.Interface, bool, error) {

	klog.Infof("Starting %s.", nodetopology.ControllerName)

	kubevirtCloud, ok := cloud.(*kubevirt.Cloud)
	if !ok {
		return nil, false, fmt.Errorf("%s does not support %v provider", nodetopology.ControllerName, cloud.ProviderName())
	}

	cfg := kubevirtCloud.GetCloudConfig().NodeTopology
	if !cfg.Enabled {
		klog.Infof("%s is not enabled.", nodetopology.ControllerName)
		return nil, false, nil
	}

	// --- Tenant client (in-cluster) ---
	tenantClient, err := kubernetes.NewForConfig(ccmConfig.Complete().Kubeconfig)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create tenant client: %w", err)
	}

	// --- Infra client ---
	var restConfig *rest.Config
	if kubevirtCloud.GetCloudConfig().Kubeconfig == "" {
		restConfig, err = rest.InClusterConfig()
		if err != nil {
			return nil, false, fmt.Errorf("failed to get in-cluster config for infra: %w", err)
		}
	} else {
		var infraKubeConfig string
		infraKubeConfig, err = kubevirtCloud.GetInfraKubeconfig()
		if err != nil {
			return nil, false, fmt.Errorf("failed to get infra kubeconfig: %w", err)
		}
		var clientConfig clientcmd.ClientConfig
		clientConfig, err = clientcmd.NewClientConfigFromBytes([]byte(infraKubeConfig))
		if err != nil {
			return nil, false, fmt.Errorf("failed to parse infra kubeconfig: %w", err)
		}
		restConfig, err = clientConfig.ClientConfig()
		if err != nil {
			return nil, false, fmt.Errorf("failed to create rest config for infra: %w", err)
		}
	}

	infraClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create infra client: %w", err)
	}

	infraDynamic, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create infra dynamic client: %w", err)
	}

	// Convert provider config to controller config.
	controllerCfg := nodetopology.NodeTopologyConfig{
		Enabled:          cfg.Enabled,
		InfraClusterName: cfg.InfraClusterName,
		RackLabelKey:     cfg.RackLabelKey,
	}

	ctrl := nodetopology.NewController(
		tenantClient,
		infraClient,
		infraDynamic,
		kubevirtCloud.Namespace(),
		controllerCfg,
	)

	if err := ctrl.Init(); err != nil {
		return nil, false, fmt.Errorf("failed to initialize %s: %w", nodetopology.ControllerName, err)
	}

	klog.Infof("Running %s", nodetopology.ControllerName)
	go ctrl.Run(1, controllerContext.Stop, controllerContext.ControllerManagerMetrics)

	return nil, false, nil
}
