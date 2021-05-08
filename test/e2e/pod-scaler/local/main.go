// +build e2e

// This tool can run the pod-scaler locally, setting up dependencies in an ephemeral manner
// and mimicking what the end-to-end tests do in order to facilitate high-fidelity local dev.
package main

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/openshift/ci-tools/pkg/psql"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/test-infra/prow/interrupts"

	"github.com/openshift/library-go/pkg/crypto"

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
	out, err := cmd.CombinedOutput()
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to initialize Postgres: %v", string(out))
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

	connectionFlags := postgres.ClientFlags()
	// this is a hack, but whatever
	u, err := url.Parse(connectionFlags[1])
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
	podScalerFlags := []string{
		"--kubeconfig=/tmp/kubeconfig",
		"--loglevel=debug",
		"--log-style=text",
		"--cache-dir=/home/stevekuznetsov/cache",
		"--mode=producer",
		"--sql-user", postgresUser,
		"--sql-ca-cert", caCertFile,
		"--sql-client-cert", clientCertFile,
		"--sql-client-key", clientKeyFile,
	}
	podScalerFlags = append(podScalerFlags, connectionFlags...)
	logger.Infof("Running pod-scaler %v", podScalerFlags)
	podScaler := exec.CommandContext(interrupts.Context(), "pod-scaler", podScalerFlags...)
	podScaler.Stdout = os.Stdout
	podScaler.Stderr = os.Stderr

	if err := podScaler.Run(); err != nil {
		logrus.WithError(err).Fatal("Failed to run pod-scaler.")
	}
	interrupts.WaitForGracefulShutdown()
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
