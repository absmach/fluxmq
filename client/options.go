// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"crypto/tls"
	"time"
)

// Default values.
const (
	DefaultKeepAlive       = 60 * time.Second
	DefaultConnectTimeout  = 10 * time.Second
	DefaultWriteTimeout    = 5 * time.Second
	DefaultAckTimeout      = 10 * time.Second
	DefaultPingTimeout     = 5 * time.Second
	DefaultReconnectMin    = 1 * time.Second
	DefaultReconnectMax    = 2 * time.Minute
	DefaultMaxInflight     = 100
	DefaultMessageChanSize = 256
)

// WillMessage represents a last will and testament message.
type WillMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

// Options configures the MQTT client.
type Options struct {
	// Connection
	Servers         []string      // List of broker addresses (host:port)
	ClientID        string        // Client identifier
	Username        string        // Optional username
	Password        string        // Optional password
	TLSConfig       *tls.Config   // TLS configuration (nil for plain TCP)
	ConnectTimeout  time.Duration // Timeout for connection attempts
	WriteTimeout    time.Duration // Timeout for write operations
	KeepAlive       time.Duration // Keep-alive interval (0 to disable)
	PingTimeout     time.Duration // Timeout waiting for PINGRESP

	// Session
	CleanSession   bool          // Start with clean session
	SessionExpiry  uint32        // Session expiry interval (MQTT 5.0, seconds)
	ProtocolVersion byte         // 4 for MQTT 3.1.1, 5 for MQTT 5.0

	// Will
	Will *WillMessage // Last will and testament

	// QoS
	AckTimeout  time.Duration // Timeout waiting for PUBACK/SUBACK
	MaxInflight int           // Maximum inflight messages

	// Reconnection
	AutoReconnect    bool          // Enable automatic reconnection
	ReconnectBackoff time.Duration // Initial reconnect delay
	MaxReconnectWait time.Duration // Maximum reconnect delay

	// Callbacks
	OnConnect         func()                                    // Called on successful connection
	OnConnectionLost  func(error)                               // Called when connection is lost
	OnReconnecting    func(attempt int)                         // Called before each reconnect attempt
	OnMessage         func(topic string, payload []byte, qos byte) // Called for incoming messages

	// Advanced
	MessageChanSize int          // Size of internal message channel
	OrderMatters    bool         // Maintain message order (may reduce throughput)
	Store           MessageStore // Message store for QoS 1/2 (nil = in-memory)
}

// NewOptions creates Options with sensible defaults.
func NewOptions() *Options {
	return &Options{
		Servers:          []string{"localhost:1883"},
		ProtocolVersion:  4, // MQTT 3.1.1
		CleanSession:     true,
		KeepAlive:        DefaultKeepAlive,
		ConnectTimeout:   DefaultConnectTimeout,
		WriteTimeout:     DefaultWriteTimeout,
		AckTimeout:       DefaultAckTimeout,
		PingTimeout:      DefaultPingTimeout,
		AutoReconnect:    true,
		ReconnectBackoff: DefaultReconnectMin,
		MaxReconnectWait: DefaultReconnectMax,
		MaxInflight:      DefaultMaxInflight,
		MessageChanSize:  DefaultMessageChanSize,
	}
}

// SetServers sets the broker addresses.
func (o *Options) SetServers(servers ...string) *Options {
	o.Servers = servers
	return o
}

// SetClientID sets the client identifier.
func (o *Options) SetClientID(id string) *Options {
	o.ClientID = id
	return o
}

// SetCredentials sets username and password.
func (o *Options) SetCredentials(username, password string) *Options {
	o.Username = username
	o.Password = password
	return o
}

// SetTLSConfig sets TLS configuration.
func (o *Options) SetTLSConfig(cfg *tls.Config) *Options {
	o.TLSConfig = cfg
	return o
}

// SetCleanSession sets the clean session flag.
func (o *Options) SetCleanSession(clean bool) *Options {
	o.CleanSession = clean
	return o
}

// SetKeepAlive sets the keep-alive interval.
func (o *Options) SetKeepAlive(d time.Duration) *Options {
	o.KeepAlive = d
	return o
}

// SetConnectTimeout sets the connection timeout.
func (o *Options) SetConnectTimeout(d time.Duration) *Options {
	o.ConnectTimeout = d
	return o
}

// SetAckTimeout sets the acknowledgment timeout.
func (o *Options) SetAckTimeout(d time.Duration) *Options {
	o.AckTimeout = d
	return o
}

// SetProtocolVersion sets MQTT protocol version (4 or 5).
func (o *Options) SetProtocolVersion(v byte) *Options {
	o.ProtocolVersion = v
	return o
}

// SetWill sets the last will and testament.
func (o *Options) SetWill(topic string, payload []byte, qos byte, retain bool) *Options {
	o.Will = &WillMessage{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
		Retain:  retain,
	}
	return o
}

// SetAutoReconnect enables or disables automatic reconnection.
func (o *Options) SetAutoReconnect(enable bool) *Options {
	o.AutoReconnect = enable
	return o
}

// SetMaxInflight sets the maximum number of inflight messages.
func (o *Options) SetMaxInflight(max int) *Options {
	o.MaxInflight = max
	return o
}

// SetOnConnect sets the connection callback.
func (o *Options) SetOnConnect(fn func()) *Options {
	o.OnConnect = fn
	return o
}

// SetOnConnectionLost sets the connection lost callback.
func (o *Options) SetOnConnectionLost(fn func(error)) *Options {
	o.OnConnectionLost = fn
	return o
}

// SetOnReconnecting sets the reconnecting callback.
func (o *Options) SetOnReconnecting(fn func(attempt int)) *Options {
	o.OnReconnecting = fn
	return o
}

// SetOnMessage sets the message handler callback.
func (o *Options) SetOnMessage(fn func(topic string, payload []byte, qos byte)) *Options {
	o.OnMessage = fn
	return o
}

// SetStore sets the message store for QoS 1/2 persistence.
func (o *Options) SetStore(store MessageStore) *Options {
	o.Store = store
	return o
}

// Validate checks the options for errors.
func (o *Options) Validate() error {
	if len(o.Servers) == 0 {
		return ErrNoServers
	}
	if o.ClientID == "" {
		return ErrEmptyClientID
	}
	if o.ProtocolVersion != 4 && o.ProtocolVersion != 5 {
		return ErrInvalidProtocol
	}
	if o.MaxInflight <= 0 {
		o.MaxInflight = DefaultMaxInflight
	}
	return nil
}
