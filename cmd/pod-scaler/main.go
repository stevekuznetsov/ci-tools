package main

import (
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/logrusadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	prometheusclient "github.com/prometheus/client_golang/api"
	prometheusapi "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"gopkg.in/fsnotify.v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
	prowflagutil "k8s.io/test-infra/prow/flagutil"
	"k8s.io/test-infra/prow/interrupts"
	"k8s.io/test-infra/prow/logrusutil"
	pprofutil "k8s.io/test-infra/prow/pjutil/pprof"
	"k8s.io/test-infra/prow/version"

	buildclientset "github.com/openshift/client-go/build/clientset/versioned/typed/build/v1"
	routeclientset "github.com/openshift/client-go/route/clientset/versioned/typed/route/v1"

	"github.com/openshift/ci-tools/pkg/psql"
	"github.com/openshift/ci-tools/pkg/util"
)

type options struct {
	mode string
	producerOptions
	consumerOptions

	instrumentationOptions prowflagutil.InstrumentationOptions

	loglevel string
	logStyle string

	cacheDir           string
	sqlHost            string
	sqlUser            string
	sqlCACertFile      string
	sqlClientCertFile  string
	sqlClientKeyFile   string
	cacheBucket        string
	gcsCredentialsFile string
}

type producerOptions struct {
	kubeconfig string
}

type consumerOptions struct {
	port   int
	uiPort int

	certDir string
}

func bindOptions(fs *flag.FlagSet) *options {
	o := options{producerOptions: producerOptions{}}
	o.instrumentationOptions.AddFlags(fs)
	fs.StringVar(&o.mode, "mode", "", "Which mode to run in.")
	fs.StringVar(&o.kubeconfig, "kubeconfig", "", "Path to a ~/.kube/config to use for querying Prometheuses. Each context will be considered a cluster to query.")
	fs.IntVar(&o.port, "port", 0, "Port to serve admission webhooks on.")
	fs.IntVar(&o.uiPort, "ui-port", 0, "Port to serve frontend on.")
	fs.StringVar(&o.certDir, "serving-cert-dir", "", "Path to directory with serving certificate and key for the admission webhook server.")
	fs.StringVar(&o.loglevel, "loglevel", "debug", "Logging level.")
	fs.StringVar(&o.logStyle, "log-style", "json", "Logging style: json or text.")
	fs.StringVar(&o.cacheDir, "cache-dir", "", "Local directory holding cache data (for development mode).")
	fs.StringVar(&o.cacheBucket, "cache-bucket", "", "GCS bucket name holding cached Prometheus data.")
	fs.StringVar(&o.gcsCredentialsFile, "gcs-credentials-file", "", "File where GCS credentials are stored.")
	fs.StringVar(&o.sqlHost, "sql-endpoint", "", "Host for the SQL database.")
	fs.StringVar(&o.sqlUser, "sql-user", "", "User for the SQL database.")
	fs.StringVar(&o.sqlCACertFile, "sql-ca-cert", "", "File holding the CA cert to connect to the SQL server.")
	fs.StringVar(&o.sqlClientCertFile, "sql-client-cert", "", "File holding the client cert to connect to the SQL server.")
	fs.StringVar(&o.sqlClientKeyFile, "sql-client-key", "", "File holding the client key to connect to the SQL server.")
	return &o
}

const (
	logStyleJson = "json"
	logStyleText = "text"
)

func (o *options) validate() error {
	switch o.mode {
	case "producer":
		_, kubeconfigSet := os.LookupEnv("KUBECONFIG")
		if o.kubeconfig == "" && !kubeconfigSet {
			return errors.New("--kubeconfig or $KUBECONFIG is required")
		}
	case "consumer.ui":
		if o.uiPort == 0 {
			return errors.New("--ui-port is required")
		}
	case "consumer.admission":
		if o.port == 0 {
			return errors.New("--port is required")
		}
		if o.certDir == "" {
			return errors.New("--serving-cert-dir is required")
		}
	default:
		return errors.New("--mode must be either \"producer\", \"consumer.ui\", or \"consumer.admission\"")
	}
	if o.cacheDir == "" {
		if o.cacheBucket == "" {
			return errors.New("--cache-bucket is required")
		}
		if o.gcsCredentialsFile == "" {
			return errors.New("--gcs-credentials-file is required")
		}
	}
	for key, val := range map[string]string{
		"sql-endpoint":    o.sqlHost,
		"sql-user":        o.sqlUser,
		"sql-ca-cert":     o.sqlCACertFile,
		"sql-client-cert": o.sqlClientCertFile,
		"sql-client-key":  o.sqlClientKeyFile,
	} {
		if val == "" {
			return fmt.Errorf("--%s is required", key)
		}
	}

	if o.logStyle != logStyleJson && o.logStyle != logStyleText {
		return fmt.Errorf("--log-style must be one of %s or %s, not %s", logStyleText, logStyleJson, o.logStyle)
	}
	if level, err := logrus.ParseLevel(o.loglevel); err != nil {
		return fmt.Errorf("--loglevel invalid: %w", err)
	} else {
		logrus.SetLevel(level)
	}

	return o.instrumentationOptions.Validate(false)
}

var (
	//go:embed init.sql
	initSQL string
)

func main() {
	flagSet := flag.NewFlagSet("", flag.ExitOnError)
	opts := bindOptions(flagSet)
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		logrus.WithError(err).Fatal("failed to parse flags")
	}
	if err := opts.validate(); err != nil {
		logrus.WithError(err).Fatal("Failed to validate flags")
	}
	switch opts.logStyle {
	case logStyleJson:
		logrusutil.ComponentInit()
	case logStyleText:
		logrus.SetFormatter(&logrus.TextFormatter{
			ForceColors:     true,
			DisableQuote:    true,
			FullTimestamp:   true,
			TimestampFormat: time.RFC3339,
		})
	}
	logrus.Infof("%s version %s", version.Name, version.Version)

	pprofutil.Instrument(opts.instrumentationOptions)

	u, err := url.Parse(opts.sqlHost)
	if err != nil {
		logrus.WithError(err).Fatal("Could not parse SQL server URL.")
	}

	connConfig, err := pgxpool.ParseConfig(psql.ConnectionConfig{
		URL:            u,
		User:           opts.sqlUser,
		ClientCertFile: opts.sqlClientCertFile,
		ClientKeyFile:  opts.sqlClientKeyFile,
		RootCAFile:     opts.sqlCACertFile,
	}.Config())
	if err != nil {
		logrus.WithError(err).Fatal("Could not create SQL client connection config.")
	}
	connConfig.ConnConfig.Logger = logrusadapter.NewLogger(logrus.WithField("client", "postgres"))
	connConfig.ConnConfig.LogLevel = pgx.LogLevelWarn

	pool, err := pgxpool.ConnectConfig(interrupts.Context(), connConfig)
	if err != nil {
		logrus.WithError(err).Fatal("Could not create SQL client.")
	}

	if err := pool.Ping(interrupts.Context()); err != nil {
		logrus.WithError(err).Fatal("Could not ping SQL server.")
	}

	var cache cache
	if opts.cacheDir != "" {
		cache = &localCache{dir: opts.cacheDir}
	} else {
		gcsClient, err := storage.NewClient(interrupts.Context(), option.WithCredentialsFile(opts.gcsCredentialsFile))
		if err != nil {
			logrus.WithError(err).Fatal("Could not initialize GCS client.")
		}
		bucket := gcsClient.Bucket(opts.cacheBucket)
		cache = &bucketCache{bucket: bucket}
	}

	switch opts.mode {
	case "producer":
		mainProduce(opts, cache, pool)
	case "consumer.ui":
		// TODO
	case "consumer.admission":
		mainAdmission(opts)
	}
	interrupts.WaitForGracefulShutdown()
}

func mainProduce(opts *options, cache cache, sqlClient *pgxpool.Pool) {
	out, err := sqlClient.Exec(interrupts.Context(), initSQL)
	if err != nil {
		logrus.WithError(err).Fatalf("Could not initialize SQL database: %v.", string(out))
	}

	kubeconfigChangedCallBack := func(e fsnotify.Event) {
		logrus.WithField("event", e.String()).Fatal("Kubeconfig changed, exiting to get restarted by Kubelet and pick up the changes")
	}

	kubeconfigs, _, err := util.LoadKubeConfigs(opts.kubeconfig, kubeconfigChangedCallBack)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to load kubeconfigs")
	}

	clients := map[string]prometheusapi.API{}
	for cluster, config := range kubeconfigs {
		logger := logrus.WithField("cluster", cluster)
		client, err := routeclientset.NewForConfig(config)
		if err != nil {
			logger.WithError(err).Fatal("Failed to construct client.")
		}
		route, err := client.Routes("openshift-monitoring").Get(interrupts.Context(), "prometheus-k8s", metav1.GetOptions{})
		if err != nil {
			logger.WithError(err).Fatal("Failed to get Prometheus route.")
		}
		promClient, err := prometheusclient.NewClient(prometheusclient.Config{
			Address:      "https://" + route.Spec.Host,
			RoundTripper: transport.NewBearerAuthRoundTripper(config.BearerToken, prometheusclient.DefaultRoundTripper),
		})
		if err != nil {
			logger.WithError(err).Fatal("Failed to get Prometheus client.")
		}
		clients[cluster] = prometheusapi.NewAPI(promClient)
		logger.Debugf("Loaded Prometheus client.")
	}

	go produce(clients, cache, sqlClient)
}

func mainAdmission(opts *options) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		logrus.WithError(err).Fatal("Failed to load in-cluster config.")
	}
	client, err := buildclientset.NewForConfig(restConfig)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to construct client.")
	}
	go admit(opts.port, opts.instrumentationOptions.HealthPort, opts.certDir, client)
}
