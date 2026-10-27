/*
SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and kafka-operator contributors
SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"flag"
	"os"

	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cskafkav1alpha1 "github.com/sap/kafka-operator/api/v1alpha1"
	aclcontroller "github.com/sap/kafka-operator/internal/controllers/acl"
	topiccontroller "github.com/sap/kafka-operator/internal/controllers/topic"
)

const (
	operatorName = "kafka-operator.cs.sap.com"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cskafkav1alpha1.AddToScheme(scheme))
}

// TODO: add metrics
// TODO: add more logs
// TODO: add events

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var leaderElectionId string

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	pflag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	pflag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	pflag.StringVar(&leaderElectionId, "leader-election-id", operatorName, "Leader election ID.")

	zapFlags := flag.NewFlagSet("", flag.ExitOnError)
	zapOptions := zap.Options{
		Development: false,
	}
	zapOptions.BindFlags(zapFlags)
	pflag.CommandLine.AddGoFlagSet(zapFlags)

	pflag.CommandLine.SortFlags = false
	pflag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOptions)))
	klog.SetLogger(ctrl.Log)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Client: client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{&cskafkav1alpha1.Topic{}, &cskafkav1alpha1.Acl{}},
			},
		},
		LeaderElection:                enableLeaderElection,
		LeaderElectionID:              leaderElectionId,
		LeaderElectionReleaseOnCancel: true,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		PprofBindAddress:       "0",
		Controller: config.Controller{
			// TODO: should we change that and recover panics?
			RecoverPanic: new(false),
		},
	})
	if err != nil {
		setupLog.Error(err, "error creating manager")
		os.Exit(1)
	}

	if err := topiccontroller.NewReconciler(operatorName, mgr.GetClient(), mgr.GetEventRecorderFor(operatorName)).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "error registering reconciler with manager")
		os.Exit(1)
	}

	if err := aclcontroller.NewReconciler(operatorName, mgr.GetClient(), mgr.GetEventRecorderFor(operatorName)).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "error registering reconciler with manager")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "error setting up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "error setting up readiness check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "error starting manager")
		os.Exit(1)
	}
}
