package load

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"k8s.io/test-infra/prow/interrupts"

	"github.com/openshift/ci-tools/pkg/api"
	"github.com/openshift/ci-tools/pkg/coalescer"
	"github.com/openshift/ci-tools/pkg/config"
)

// ConfigAgent is an interface that can load configs from disk into
// memory and retrieve them when provided with a config.Info.
type ConfigAgent interface {
	GetConfig(config.Info) (api.ReleaseBuildConfiguration, error)
	GetAll() FilenameToConfig
	GetGeneration() int
}

type FilenameToConfig map[string]api.ReleaseBuildConfiguration

type configAgent struct {
	lock         *sync.RWMutex
	configs      FilenameToConfig
	configPath   string
	cycle        time.Duration
	generation   int
	errorMetrics *prometheus.CounterVec
}

var configReloadTimeMetric = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "configresolver_config_reload_duration_seconds",
		Help:    "config reload duration in seconds",
		Buckets: []float64{0.5, 0.75, 1, 1.25, 1.5, 2, 2.5, 3, 4, 5, 6},
	},
)

func init() {
	prometheus.MustRegister(configReloadTimeMetric)
}

// NewConfigAgent returns a ConfigAgent interface that automatically reloads when
// configs are changed on disk as well as on a period specified with a time.Duration.
func NewConfigAgent(configPath string, cycle time.Duration, errorMetrics *prometheus.CounterVec) (ConfigAgent, error) {
	a := &configAgent{configPath: configPath, cycle: cycle, lock: &sync.RWMutex{}, errorMetrics: errorMetrics}
	configCoalescer := coalescer.NewCoalescer(a.loadFilenameToConfig)
	err := configCoalescer.Run()
	if err != nil {
		return nil, fmt.Errorf("Failed to load configs: %v", err)
	}

	// periodic reload
	interrupts.TickLiteral(func() {
		if err := configCoalescer.Run(); err != nil {
			log.WithError(err).Error("Failed to reload configs")
		}
	}, a.cycle)

	err = startWatchers(a.configPath, configCoalescer, a.recordError)
	return a, err
}

func (a *configAgent) recordError(label string) {
	labels := prometheus.Labels{"error": label}
	a.errorMetrics.With(labels).Inc()
}

func (a *configAgent) GetConfig(info config.Info) (api.ReleaseBuildConfiguration, error) {
	a.lock.RLock()
	defer a.lock.RUnlock()
	config, ok := a.configs[info.Basename()]
	if !ok {
		return api.ReleaseBuildConfiguration{}, fmt.Errorf("Could not find config %s", info.Basename())
	}
	return config, nil
}

func (a *configAgent) GetAll() FilenameToConfig {
	return a.configs
}

func (a *configAgent) GetGeneration() int {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.generation
}

// loadFilenameToConfig generates a new filenameToConfig map.
func (a *configAgent) loadFilenameToConfig() error {
	log.Debug("Reloading configs")
	startTime := time.Now()
	configs := FilenameToConfig{}
	err := filepath.Walk(a.configPath, func(path string, info os.FileInfo, err error) error {
		if info == nil {
			return err
		}
		if strings.HasPrefix(info.Name(), "..") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(path)
		if info != nil && !info.IsDir() && (ext == ".yml" || ext == ".yaml") {
			configSpec, err := Config(path, nil)
			if err != nil {
				a.recordError("failed to load ci-operator config")
				return fmt.Errorf("failed to load ci-operator config (%v)", err)
			}

			if err := configSpec.ValidateAtRuntime(); err != nil {
				a.recordError("invalid ci-operator config")
				return fmt.Errorf("invalid ci-operator config: %v", err)
			}
			log.Tracef("Adding %s to filenameToConfig", filepath.Base(path))
			configs[filepath.Base(path)] = *configSpec
		}
		return nil
	})
	if err != nil {
		return err
	}
	a.lock.Lock()
	a.configs = configs
	a.generation++
	a.lock.Unlock()
	duration := time.Since(startTime)
	configReloadTimeMetric.Observe(float64(duration.Seconds()))
	log.WithField("duration", duration).Info("Configs reloaded")
	return nil
}
