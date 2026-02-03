// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp091

import (
	"crypto/tls"
	"net/url"
	"strings"
	"time"
)

// Default values.
const (
	DefaultAddress     = "localhost:5682"
	DefaultDialTimeout = 10 * time.Second
	DefaultHeartbeat   = 60 * time.Second
)

// Options configures the AMQP 0.9.1 client.
type Options struct {
	// Connection
	URL         string      // Full AMQP URL (overrides Address/Username/Password/Vhost)
	Address     string      // Broker address (host:port)
	Username    string      // Username for PLAIN auth
	Password    string      // Password for PLAIN auth
	Vhost       string      // Virtual host (default "/")
	TLSConfig   *tls.Config // TLS configuration (nil for plain TCP)
	DialTimeout time.Duration
	Heartbeat   time.Duration

	// Channel QoS
	PrefetchCount int // Maximum unacked deliveries
	PrefetchSize  int // Maximum bytes in-flight
}

// NewOptions creates Options with sensible defaults.
func NewOptions() *Options {
	return &Options{
		Address:     DefaultAddress,
		Username:    "guest",
		Password:    "guest",
		Vhost:       "/",
		DialTimeout: DefaultDialTimeout,
		Heartbeat:   DefaultHeartbeat,
	}
}

// SetAddress sets the broker address (host:port).
func (o *Options) SetAddress(addr string) *Options {
	o.Address = addr
	return o
}

// SetCredentials sets username and password.
func (o *Options) SetCredentials(username, password string) *Options {
	o.Username = username
	o.Password = password
	return o
}

// SetVhost sets the virtual host.
func (o *Options) SetVhost(vhost string) *Options {
	o.Vhost = vhost
	return o
}

// SetTLSConfig sets TLS configuration.
func (o *Options) SetTLSConfig(cfg *tls.Config) *Options {
	o.TLSConfig = cfg
	return o
}

// SetDialTimeout sets the dial timeout.
func (o *Options) SetDialTimeout(d time.Duration) *Options {
	o.DialTimeout = d
	return o
}

// SetHeartbeat sets the heartbeat interval.
func (o *Options) SetHeartbeat(d time.Duration) *Options {
	o.Heartbeat = d
	return o
}

// SetPrefetch sets channel prefetch limits.
func (o *Options) SetPrefetch(count, size int) *Options {
	o.PrefetchCount = count
	o.PrefetchSize = size
	return o
}

// Validate checks the options for errors.
func (o *Options) Validate() error {
	if o.URL == "" && o.Address == "" {
		return ErrNoAddress
	}
	return nil
}

func (o *Options) dialURL() (string, error) {
	if o.URL != "" {
		return o.URL, nil
	}

	scheme := "amqp"
	if o.TLSConfig != nil {
		scheme = "amqps"
	}

	vhost := strings.TrimPrefix(o.Vhost, "/")
	u := &url.URL{
		Scheme: scheme,
		Host:   o.Address,
		Path:   "/" + vhost,
	}

	if o.Username != "" {
		u.User = url.UserPassword(o.Username, o.Password)
	}

	return u.String(), nil
}
