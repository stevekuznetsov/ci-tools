// +build e2e

// This tool can run the pod-scaler locally, setting up dependencies in an ephemeral manner
// and mimicking what the end-to-end tests do in order to facilitate high-fidelity local dev.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"time"

	prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/test-infra/prow/interrupts"
	"k8s.io/utils/pointer"

	"github.com/openshift/library-go/pkg/crypto"

	"github.com/openshift/ci-tools/pkg/psql"
	"github.com/openshift/ci-tools/pkg/testhelper"
)

func main() {
	logger := logrus.WithField("component", "local-pod-scaler")
	tmpDir, err := ioutil.TempDir("", "podscaler")
	if err != nil {
		logger.WithError(err).Fatal("Failed to create temporary directory.")
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			logger.WithError(err).Error("Failed to clean up temporary directory.")
		}
	}()
	t := &fakeT{
		tmpDir: tmpDir,
		logger: logger,
	}
	prometheusDir, err := ioutil.TempDir(tmpDir, "prometheus")
	if err != nil {
		logger.WithError(err).Fatal("Failed to create temporary directory for Prometheus.")
	}
	if err := os.Chmod(prometheusDir, 0777); err != nil {
		logger.WithError(err).Fatal("Failed to open permissions in temporary directory for Prometheus.")
	}
	logger.Infof("Prometheus data will be in %s", prometheusDir)

	prometheusConfig := filepath.Join(prometheusDir, "prometheus.yaml")
	if err := ioutil.WriteFile(prometheusConfig, []byte(`global:
  scrape_interval: 15s`), 0666); err != nil {
		logger.WithError(err).Fatal("Could not write Prometheus config file.")
	}

	prometheusHostname := "0.0.0.0"
	prometheusFlags := []string{
		"--config.file", prometheusConfig,
		"--storage.tsdb.path", prometheusDir,
		"--storage.tsdb.retention.time", "20d",
		"--storage.tsdb.allow-overlapping-blocks",
		"--log.level", "info",
	}
	prometheusCtx, prometheusCancel := context.WithCancel(interrupts.Context())
	prometheusInitOutput := &bytes.Buffer{}
	prometheusInit := exec.CommandContext(prometheusCtx, "prometheus",
		append(prometheusFlags, "--web.listen-address", prometheusHostname+":"+testhelper.GetFreePort(t))...,
	)
	prometheusInit.Stdout = prometheusInitOutput
	prometheusInit.Stderr = prometheusInitOutput
	if err := prometheusInit.Start(); err != nil {
		logrus.WithError(err).Fatal("Failed to initialize Prometheus.")
	}

	logger.Info("Backfilling Prometheus data.")
	prometheusBackfillFile, err := ioutil.TempFile(prometheusDir, "backfill")
	if err != nil {
		logger.WithError(err).Fatal("Could not create temporary file for Prometheus backfill.")
	}
	encoder := expfmt.NewEncoder(prometheusBackfillFile, expfmt.FmtOpenMetrics)
	metricTypePtr := func(metric prometheus_client.MetricType) *prometheus_client.MetricType { return &metric }

	type metricMetadata struct {
		metricType prometheus_client.MetricType
		addValue   func(*prometheus_client.Metric, float64, float64)
	}
	rand.Seed(time.Now().UnixNano())
	kubePodLabels := prometheus_client.MetricFamily{
		Name:   pointer.StringPtr("kube_pod_labels"),
		Type:   metricTypePtr(prometheus_client.MetricType_COUNTER),
		Metric: []*prometheus_client.Metric{},
	}
	for metric, metadata := range map[string]metricMetadata{
		"container_cpu_usage_seconds_total": {
			metricType: prometheus_client.MetricType_COUNTER,
			addValue: func(metric *prometheus_client.Metric, f, j float64) {
				metric.Counter = &prometheus_client.Counter{Value: pointer.Float64Ptr(f * j)}
			},
		},
		"container_memory_working_set_bytes": {
			metricType: prometheus_client.MetricType_GAUGE,
			addValue: func(metric *prometheus_client.Metric, f, _ float64) {
				metric.Gauge = &prometheus_client.Gauge{Value: pointer.Float64Ptr(f)}
			},
		},
	} {
		family := prometheus_client.MetricFamily{
			Name:   pointer.StringPtr(metric),
			Type:   metricTypePtr(metadata.metricType),
			Metric: []*prometheus_client.Metric{},
		}
		for i, labelSet := range []map[string]string{
			{
				"label_created_by_ci":                    "true",
				"label_ci_openshift_io_metadata_org":     "org",
				"label_ci_openshift_io_metadata_repo":    "repo",
				"label_ci_openshift_io_metadata_branch":  "branch",
				"label_ci_openshift_io_metadata_variant": "variant",
				"label_ci_openshift_io_metadata_target":  "target",
				"label_ci_openshift_io_metadata_step":    "step",
				"pod":                                    "pod",
				"container":                              "container",
			}, {
				"label_created_by_ci":                    "true",
				"label_ci_openshift_io_metadata_org":     "org",
				"label_ci_openshift_io_metadata_repo":    "repo",
				"label_ci_openshift_io_metadata_branch":  "branch",
				"label_ci_openshift_io_metadata_variant": "variant",
				"label_ci_openshift_io_metadata_target":  "target",
				"label_openshift_io_build_name":          "src",
				"pod":                                    "src-build",
				"container":                              "container",
			},
			{
				"label_created_by_ci":                    "true",
				"label_ci_openshift_io_metadata_org":     "org",
				"label_ci_openshift_io_metadata_repo":    "repo",
				"label_ci_openshift_io_metadata_branch":  "branch",
				"label_ci_openshift_io_metadata_variant": "variant",
				"label_ci_openshift_io_metadata_target":  "target",
				"label_ci_openshift_io_release":          "latest",
				"pod":                                    "release-latest-cli",
				"container":                              "container",
			},
			{
				"label_created_by_ci":                    "true",
				"label_ci_openshift_io_metadata_org":     "org",
				"label_ci_openshift_io_metadata_repo":    "repo",
				"label_ci_openshift_io_metadata_branch":  "branch",
				"label_ci_openshift_io_metadata_variant": "variant",
				"label_ci_openshift_io_metadata_target":  "target",
				"label_app":                              "rpm-repo",
				"pod":                                    "rpm-repo-5d88d4fc4c-jg2xb",
				"container":                              "rpm-repo",
			},
			{
				"label_created_by_prow":           "true",
				"label_prow_k8s_io_refs_org":      "org",
				"label_prow_k8s_io_refs_repo":     "repo",
				"label_prow_k8s_io_refs_base_ref": "branch",
				"label_prow_k8s_io_context":       "context",
				"pod":                             "d316d4cc-a437-11eb-b35f-0a580a800e92",
				"container":                       "container",
			},
			{
				"label_created_by_prow":           "true",
				"label_prow_k8s_io_refs_org":      "org",
				"label_prow_k8s_io_refs_repo":     "repo",
				"label_prow_k8s_io_refs_base_ref": "branch",
				"label_prow_k8s_io_context":       "",
				"label_prow_k8s_io_job":           "periodic-ci-org-repo-branch-context",
				"label_prow_k8s_io_type":          "periodic",
				"pod":                             "d316d4cc-a437-11eb-b35f-0a580a800e92",
				"container":                       "container",
			},
			{
				"label_created_by_prow":     "true",
				"label_prow_k8s_io_context": "",
				"label_prow_k8s_io_job":     "periodic-handwritten-prowjob",
				"label_prow_k8s_io_type":    "periodic",
				"pod":                       "d316d4cc-a437-11eb-b35f-0a580a800e92",
				"container":                 "container",
			},
		} {
			for j := 0; j < 10; j++ {
				localSet := map[string]string{}
				for key, value := range labelSet {
					localSet[key] = value
				}
				for _, required := range []string{"namespace", "pod", "container"} {
					if _, set := localSet[required]; set {
						continue
					}
					localSet[required] = uuid.NewV4().String()
				}
				var labels []*prometheus_client.LabelPair
				for key, value := range localSet {
					labels = append(labels, &prometheus_client.LabelPair{
						Name:  pointer.StringPtr(key),
						Value: pointer.StringPtr(value),
					})
				}
				start := time.Now().Add(-time.Duration(rand.Float64()*float64(20*24*time.Hour-125*30*time.Second)) - 125*30*time.Second).Round(1 * time.Minute)
				for k := 0; k < 125; k++ {
					ts := pointer.Int64Ptr(start.Add(time.Duration(k)*30*time.Second).UnixNano() / 1e6)
					m := &prometheus_client.Metric{
						Label:       labels,
						TimestampMs: ts,
					}
					metadata.addValue(m, float64(i+1), float64(k))
					family.Metric = append(family.Metric, m)

					kubePodLabels.Metric = append(kubePodLabels.Metric, &prometheus_client.Metric{
						Label:       labels,
						TimestampMs: ts,
						Counter:     &prometheus_client.Counter{Value: pointer.Float64Ptr(1)},
					})
				}
			}
		}
		if err := encoder.Encode(&family); err != nil {
			logger.WithError(err).Fatal("Failed to write Prometheus backfill data.")
		}
	}
	if err := encoder.Encode(&kubePodLabels); err != nil {
		logger.WithError(err).Fatal("Failed to write Prometheus backfill data.")
	}
	closer := encoder.(expfmt.Closer)
	if err := closer.Close(); err != nil {
		logger.WithError(err).Fatal("Failed to close Prometheus backfill data.")
	}
	if err := prometheusBackfillFile.Close(); err != nil {
		logger.WithError(err).Fatal("Failed to close temporary Prometheus backfill file.")
	}

	backfillStart := time.Now()
	backfill := exec.Command("promtool", "tsdb", "create-blocks-from", "openmetrics",
		prometheusBackfillFile.Name(),
		prometheusDir,
	)
	if out, err := backfill.CombinedOutput(); err != nil {
		logrus.WithError(err).Fatalf("Failed to backfill Prometheus: %v: %v", err, string(out))
	} else {
		logrus.Trace(string(out))
	}
	logrus.WithField("duration", time.Since(backfillStart)).Info("Backfilled Prometheus data.")
	// restart Prometheus to reload TSDB, by default this can take minutes without a restart
	prometheusCancel()
	if err := prometheusInit.Wait(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ProcessState.String() == "signal: killed" {
			// this was us killing the process, ignore
		} else {
			logrus.WithError(err).Fatalf("Failed to initialize Prometheus: %v: %v %T", err, prometheusInitOutput.String(), err)
		}
	}

	prometheus := testhelper.NewAccessory("prometheus", prometheusFlags,
		func(port, _ string) []string {
			logger.Infof("Prometheus starting on port %s", port)
			return []string{"--web.listen-address", fmt.Sprintf("%s:%s", prometheusHostname, port)}
		}, func(port, _ string) []string {
			return []string{"--prometheus-endpoint", fmt.Sprintf("%s:%s", prometheusHostname, port)}
		},
	)
	prometheus.RunFromFrameworkRunner(t, interrupts.Context())
	// TODO: wait more intelligently
	time.Sleep(1 * time.Second)
	prometheusConnectionFlags := prometheus.ClientFlags()
	// this is a hack, but whatever
	prometheusAddr := prometheusConnectionFlags[1]
	prometheusHost := "http://" + prometheusAddr
	logger.Infof("Prometheus is running at %s", prometheusHost)

	k8sPort := testhelper.GetFreePort(t)
	k8sAddr := "0.0.0.0:" + k8sPort
	fakeKubernetes(k8sAddr, prometheusAddr)

	kubeconfig := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"default": {
				Server: k8sAddr,
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			"default": {
				Cluster:   "default",
				AuthInfo:  "user",
				Namespace: "ns",
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"user": {
				Username: "user",
				Password: "pass",
			},
		},
	}

	kubeconfigFile, err := ioutil.TempFile(tmpDir, "kubeconfig")
	if err != nil {
		logger.WithError(err).Fatal("Failed to create temporary kubeconfig file.")
	}
	if err := kubeconfigFile.Close(); err != nil {
		logger.WithError(err).Fatal("Failed to close temporary kubeconfig file.")
	}
	if err := clientcmd.ModifyConfig(&clientcmd.PathOptions{
		GlobalFile: kubeconfigFile.Name(),
		LoadingRules: &clientcmd.ClientConfigLoadingRules{
			ExplicitPath: kubeconfigFile.Name(),
		},
	}, kubeconfig, false); err != nil {
		logger.WithError(err).Fatal("Failed to write temporary kubeconfig file.")
	}

	postgresDir, err := ioutil.TempDir(tmpDir, "postgres")
	if err != nil {
		logger.WithError(err).Fatal("Failed to create temporary directory for Postgres.")
	}
	if err := os.Chmod(postgresDir, 0777); err != nil {
		logger.WithError(err).Fatal("Failed to open permissions in temporary directory for Postgres.")
	}
	logger.Infof("Postgres data will be in %s", postgresDir)
	postgresUser := "postgres"
	cmd := exec.Command("pg_ctl", "init",
		"-D", postgresDir,
		"-o", fmt.Sprintf("--username=%s", postgresUser),
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		logrus.WithError(err).Fatalf("Failed to initialize Postgres: %v: %v", err, string(out))
	}
	authFile, err := os.OpenFile(path.Join(postgresDir, "pg_hba.conf"), os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		logrus.WithError(err).Fatal("Could not open Postgres auth configuration file.")
	}
	if _, err := authFile.Write([]byte("hostssl all all 0.0.0.0/0 cert\n")); err != nil {
		logrus.WithError(err).Fatal("Could not write Postgres auth configuration file.")
	}
	if err := authFile.Close(); err != nil {
		logrus.WithError(err).Fatal("Could not close Postgres auth configuration file.")
	}
	authDir, err := ioutil.TempDir(tmpDir, "auth")
	if err != nil {
		logger.WithError(err).Fatal("Failed to create temporary directory for auth.")
	}
	caCertFile := path.Join(authDir, "ca.crt")
	caKeyFile := path.Join(authDir, "ca.key")
	caSerialFile := path.Join(authDir, "ca.serial")
	ca, err := crypto.MakeSelfSignedCA(caCertFile, caKeyFile, caSerialFile, "selfca", 10)
	if err != nil {
		logger.WithError(err).Fatal("Failed to generate self-signed CA for postgres auth.")
	}
	serverHostname := "127.0.0.1"
	serverCertFile := path.Join(authDir, "server.crt")
	serverKeyFile := path.Join(authDir, "server.key")
	if _, _, err = ca.EnsureServerCert(serverCertFile, serverKeyFile, sets.NewString(serverHostname), 10); err != nil {
		logger.WithError(err).Fatal("Failed to ensure server cert and key for postgres auth.")
	}
	postgres := testhelper.NewAccessory("postgres", []string{
		"-D", postgresDir,
		"-c", "log_destination=stderr",
		"-c", "logging_collector=false",
		"-c", fmt.Sprintf("unix_socket_directories=%s", postgresDir),
		"-c", "listen_addresses=*",
		"-c", "ssl=true",
		"-c", fmt.Sprintf("ssl_ca_file=%s", caCertFile),
		"-c", fmt.Sprintf("ssl_cert_file=%s", serverCertFile),
		"-c", fmt.Sprintf("ssl_key_file=%s", serverKeyFile),
	}, func(port, _ string) []string {
		logger.Infof("Postgres starting on port %s", port)
		return []string{"-c", fmt.Sprintf("port=%s", port)}
	}, func(port, _ string) []string {
		return []string{"--sql-endpoint", fmt.Sprintf("postgres://%s:%s", serverHostname, port)}
	})
	postgres.RunFromFrameworkRunner(t, interrupts.Context())
	// TODO: ping for ready
	time.Sleep(1 * time.Second)

	clientCertFile := path.Join(authDir, "client.crt")
	clientKeyFile := path.Join(authDir, "client.key")
	if _, _, err = ca.EnsureClientCertificate(clientCertFile, clientKeyFile, &user.DefaultInfo{
		Name: postgresUser,
	}, 10); err != nil {
		logger.WithError(err).Fatal("Failed to ensure client cert and key for postgres auth.")
	}

	postgresConnectionFlags := postgres.ClientFlags()
	// this is a hack, but whatever
	u, err := url.Parse(postgresConnectionFlags[1])
	if err != nil {
		logrus.WithError(err).Fatal("Could not parse postgres URL.")
	}
	connConfig := psql.ConnectionConfig{
		URL:            u,
		User:           postgresUser,
		ClientCertFile: clientCertFile,
		ClientKeyFile:  clientKeyFile,
		RootCAFile:     caCertFile,
	}
	logrus.Infof("Connect with `psql \"%s\"`", connConfig.Config())

	dataDir, err := ioutil.TempDir(tmpDir, "data")
	if err != nil {
		logger.WithError(err).Fatal("Failed to create temporary directory for data.")
	}
	podScalerFlags := []string{
		"--kubeconfig", kubeconfigFile.Name(),
		"--loglevel=info",
		"--log-style=text",
		"--cache-dir", dataDir,
		"--mode=producer",
		"--sql-user", postgresUser,
		"--sql-ca-cert", caCertFile,
		"--sql-client-cert", clientCertFile,
		"--sql-client-key", clientKeyFile,
	}
	podScalerFlags = append(podScalerFlags, postgresConnectionFlags...)
	logger.Infof("Running pod-scaler %v", podScalerFlags)
	podScaler := exec.CommandContext(interrupts.Context(), "pod-scaler", podScalerFlags...)
	podScaler.Stdout = os.Stdout
	podScaler.Stderr = os.Stderr

	if err := podScaler.Run(); err != nil {
		logrus.WithError(err).Fatal("Failed to run pod-scaler.")
	}
	interrupts.WaitForGracefulShutdown()
}

// fakeKubernetes serves fake data as if it were a k8s apiserver
func fakeKubernetes(addr, prometheusAddr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/apis/route.openshift.io/v1/namespaces/openshift-monitoring/routes/prometheus-k8s", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if _, err := writer.Write([]byte(fmt.Sprintf(`{"spec":{"host":"%s"}}`, prometheusAddr))); err != nil {
			http.Error(writer, err.Error(), 500)
			return
		}
	})
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	interrupts.ListenAndServe(server, 10*time.Second)
}

type fakeT struct {
	tmpDir string
	logger *logrus.Entry
}

var _ testhelper.TestingTInterface = &fakeT{}

func (t *fakeT) Cleanup(f func())                          { logrus.RegisterExitHandler(f) }
func (t *fakeT) Deadline() (deadline time.Time, ok bool)   { return time.Time{}, false }
func (t *fakeT) Error(args ...interface{})                 { t.logger.Error(args...) }
func (t *fakeT) Errorf(format string, args ...interface{}) { t.logger.Errorf(format, args...) }
func (t *fakeT) Fail()                                     { t.logger.Fatal("Fail called!") }
func (t *fakeT) FailNow()                                  { t.logger.Fatal("FailNow called!") }
func (t *fakeT) Failed() bool                              { return false }
func (t *fakeT) Fatal(args ...interface{})                 { t.logger.Fatal(args...) }
func (t *fakeT) Fatalf(format string, args ...interface{}) { t.logger.Fatalf(format, args...) }
func (t *fakeT) Helper()                                   {}
func (t *fakeT) Log(args ...interface{})                   { t.logger.Info(args...) }
func (t *fakeT) Logf(format string, args ...interface{})   { t.logger.Infof(format, args...) }
func (t *fakeT) Name() string                              { return "pod-scaler/local" }
func (t *fakeT) Parallel()                                 {}
func (t *fakeT) Skip(args ...interface{})                  { t.logger.Info(args...) }
func (t *fakeT) SkipNow()                                  {}
func (t *fakeT) Skipf(format string, args ...interface{})  { t.logger.Infof(format, args...) }
func (t *fakeT) Skipped() bool                             { return false }
func (t *fakeT) TempDir() string {
	dir, err := ioutil.TempDir(t.tmpDir, "")
	if err != nil {
		t.logger.WithError(err).Fatal("Could not create temporary directory.")
	}
	return dir
}
