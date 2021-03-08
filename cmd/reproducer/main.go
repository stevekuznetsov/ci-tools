package main

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	coreapi "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openshift/ci-tools/pkg/util"
)

func main() {
	clusterConfig, err := util.LoadClusterConfig()
	if err != nil {
		logrus.WithError(err).Fatal("failed to load cluster config")
	}

	crclient, err := ctrlruntimeclient.New(clusterConfig, ctrlruntimeclient.Options{})
	if err != nil {
		logrus.WithError(err).Fatal("failed to construct client")
	}

	logrus.Info("Looking for prior pod...")
	existing := &coreapi.Pod{}
	if getErr := crclient.Get(
		context.TODO(),
		ctrlruntimeclient.ObjectKey{Namespace: "skuznets", Name: "termination-debug"},
		existing,
	); getErr != nil && !kerrors.IsNotFound(getErr) {
		logrus.WithError(getErr).Fatal("could not look for pod")
	}
	if existing.Name != "" {
		logrus.Info("Removing prior pod...")
		if err := crclient.Delete(
			context.TODO(),
			existing,
			ctrlruntimeclient.Preconditions{},
		); err != nil && !kerrors.IsNotFound(err) {
			logrus.WithError(err).Fatal("could not delete previous pod")
		}
	}

	var gp int64 = 150
	pod := &coreapi.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "termination-debug",
			Namespace: "skuznets",
			Labels: map[string]string{
				"termination-debug-pod": "true",
			},
		},
		Spec: coreapi.PodSpec{
			Containers: []coreapi.Container{{
				Name:    "shell",
				Command: []string{"/bin/bash"},
				Args: []string{"-c", `set -o xtrace
function cleanup() {
  echo "Cleaning ui ..."
  sleep 25
  echo "Cleaning up finished!"
  exit 0
}
trap cleanup EXIT
sleep 3600`},
				Image: "registry.ci.openshift.org/ci/centos:7",
			}},
			TerminationGracePeriodSeconds: &gp,
		},
	}
	if err := crclient.Create(context.TODO(), pod); err != nil {
		logrus.WithError(err).Fatal("could not create pod")
	}
	logrus.Info("oc --context build02 --namespace ci logs -c shell -f pod/termination-debug")
	time.Sleep(10 * time.Second)
	logrus.Info("Deleting...")
	if err := crclient.DeleteAllOf(
		context.TODO(),
		&coreapi.Pod{},
		ctrlruntimeclient.InNamespace("skuznets"),
		ctrlruntimeclient.MatchingLabels{"termination-debug-pod": "true"},
	); err != nil && !kerrors.IsNotFound(err) {
		logrus.WithError(err).Fatal("failed to delete pods with label %s=%s: %w", "termination-debug-pod", "true", err)
	}
}
