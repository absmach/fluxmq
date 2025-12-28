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

	// MQTT 5.0 Will Properties
	WillDelayInterval uint32            // Delay before sending will (seconds)
	PayloadFormat     *byte             // 0=bytes, 1=UTF-8
	MessageExpiry     uint32            // Will message lifetime (seconds)
	ContentType       string            // MIME type
	ResponseTopic     string            // Response topic for request/response
	CorrelationData   []byte            // Correlation data for request/response
	UserProperties    map[string]string // User-defined properties
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
	CleanSession    bool   // Start with clean session
	SessionExpiry   uint32 // Session expiry interval (MQTT 5.0, seconds)
	ProtocolVersion byte   // 4 for MQTT 3.1.1, 5 for MQTT 5.0

	// MQTT 5.0 Connect Properties
	ReceiveMaximum        uint16 // Maximum inflight messages client accepts (0 = use default 65535)
	MaximumPacketSize     uint32 // Maximum packet size client accepts (0 = no limit)
	TopicAliasMaximum     uint16 // Maximum topic aliases client accepts (0 = disabled)
	RequestResponseInfo   bool   // Request server to send response information in CONNACK
	RequestProblemInfo    bool   // Request detailed error information (default true)

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
	OnConnect            func()                          // Called on successful connection
	OnConnectionLost     func(error)                     // Called when connection is lost
	OnReconnecting       func(attempt int)               // Called before each reconnect attempt
	OnMessage            func(topic string, payload []byte, qos byte) // Called for incoming messages
	OnServerCapabilities func(*ServerCapabilities)       // Called when server capabilities received (MQTT 5.0)

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
		// MQTT 5.0 defaults
		RequestProblemInfo: true, // Request detailed error information
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

// SetSessionExpiry sets the session expiry interval in seconds (MQTT 5.0).
// 0 means the session expires when the network connection closes.
func (o *Options) SetSessionExpiry(seconds uint32) *Options {
	o.SessionExpiry = seconds
	return o
}

// SetReceiveMaximum sets the maximum inflight messages the client accepts (MQTT 5.0).
// Default is 65535 if not set. Must be > 0.
func (o *Options) SetReceiveMaximum(max uint16) *Options {
	o.ReceiveMaximum = max
	return o
}

// SetMaximumPacketSize sets the maximum packet size the client accepts (MQTT 5.0).
// 0 means no limit beyond protocol maximum (256 MB).
func (o *Options) SetMaximumPacketSize(size uint32) *Options {
	o.MaximumPacketSize = size
	return o
}

// SetTopicAliasMaximum sets the maximum topic aliases the client accepts (MQTT 5.0).
// 0 means topic aliases are disabled. Server cannot use topic aliases if set to 0.
func (o *Options) SetTopicAliasMaximum(max uint16) *Options {
	o.TopicAliasMaximum = max
	return o
}

// SetRequestResponseInfo requests the server to send response information (MQTT 5.0).
// The server may include response information in CONNACK which can be used for
// request/response patterns.
func (o *Options) SetRequestResponseInfo(request bool) *Options {
	o.RequestResponseInfo = request
	return o
}

// SetRequestProblemInfo requests detailed error information from server (MQTT 5.0).
// When true, server includes reason strings and user properties in error responses.
// Default is true.
func (o *Options) SetRequestProblemInfo(request bool) *Options {
	o.RequestProblemInfo = request
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

// SetOnServerCapabilities sets the server capabilities callback (MQTT 5.0).
func (o *Options) SetOnServerCapabilities(fn func(*ServerCapabilities)) *Options {
	o.OnServerCapabilities = fn
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
