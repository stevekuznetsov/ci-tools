package psql

import (
	"fmt"
	"net/url"
	"strings"
)

type ConnectionConfig struct {
	URL            *url.URL
	User           string
	ClientCertFile string
	ClientKeyFile  string
	RootCAFile     string
}

// Config returns a connection string for use with e.g. psql
func (c ConnectionConfig) Config() string {
	var pairs []string
	for key, value := range map[string]string{
		"host":        c.URL.Hostname(),
		"port":        c.URL.Port(),
		"user":        c.User,
		"sslmode":     "verify-full",
		"sslcert":     c.ClientCertFile,
		"sslkey":      c.ClientKeyFile,
		"sslrootcert": c.RootCAFile,
	} {
		pairs = append(pairs, fmt.Sprintf("%s=%s", key, value))
	}
	return strings.Join(pairs, " ")
}
