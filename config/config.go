// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	mqtttls "github.com/absmach/fluxmq/pkg/tls"
	"github.com/absmach/fluxmq/topics"
	"gopkg.in/yaml.v3"
)

const (
	// MQTT listener protocol modes.
	ProtocolModeAuto = "auto"
	ProtocolModeV3   = "v3"
	ProtocolModeV5   = "v5"

	listenerNamePlain = "plain"
	raftGroupDefault  = "default"

	listenerNameTLS      = "tls"
	listenerNameMTLS     = "mtls"
	listenerNameLocal    = "local"
	listenerNameInternal = "internal"
	listenerNameService  = "service"

	protocolMQTT    = "mqtt"
	protocolAMQP    = "amqp"
	protocolAMQP091 = "amqp091"

	defaultTCPV3Addr = ":1883"
	defaultTCPV5Addr = ":1884"
	defaultWSPath    = "/mqtt"
	defaultNodeID    = "broker-1"

	logLevelDebug = "debug"
	logLevelInfo  = "info"
	logLevelWarn  = "warn"
	logLevelError = "error"

	storageTypeBadger = "badger"

	writePolicyForward = "forward"
	writePolicyLocal   = "local"
	queueModeSync      = "sync"

	authURLField               = "url"
	authTransportField         = "transport"
	authTimeoutField           = "timeout"
	authProtocolsField         = "protocols"
	authIdentityCacheSizeField = "identity_cache_size"
	authIdentityCacheTTLField  = "identity_cache_ttl"
	authTLSField               = "tls"
	clientAuthRequire          = "require"
)

// Config holds all configuration for the MQTT broker.
type Config struct {
	Server       ServerConfig       `yaml:"server"`
	Broker       BrokerConfig       `yaml:"broker"`
	Session      SessionConfig      `yaml:"session"`
	Log          LogConfig          `yaml:"log"`
	Storage      StorageConfig      `yaml:"storage"`
	Cluster      ClusterConfig      `yaml:"cluster"`
	Webhook      WebhookConfig      `yaml:"webhook"`
	RateLimit    RateLimitConfig    `yaml:"ratelimit"`
	QueueManager QueueManagerConfig `yaml:"queue_manager"`
	Queues       []QueueConfig      `yaml:"queues"`
	Auth         AuthConfig         `yaml:"auth"`
	Hooks        HooksConfig        `yaml:"hooks"`
}

// AuthConfig configures external callouts and broker-local principals.
type AuthConfig struct {
	External        ExternalAuthConfig     `yaml:"external"`
	LocalPrincipals []LocalPrincipalConfig `yaml:"local_principals"`
}

// ExternalAuthConfig configures the external authentication/authorization callout.
type ExternalAuthConfig struct {
	// URL is the auth service address (e.g. "http://localhost:9090").
	// When empty, auth callout is disabled.
	URL string `yaml:"url"`
	// Transport selects the callout wire format: "grpc" (default) or "http".
	Transport string        `yaml:"transport"`
	Timeout   time.Duration `yaml:"timeout"`
	// Protocols controls which protocols require auth callout.
	// When empty or nil, all protocols require auth (backward compatible).
	// When set, only protocols mapped to true get auth; others allow all connections.
	// Valid keys: "mqtt", "amqp", "amqp091", "http", "coap".
	Protocols map[string]bool `yaml:"protocols"`

	// IdentityCacheSize bounds the number of cached clientID→external-ID mappings.
	// Zero or negative disables size-based eviction (entries still expire via TTL).
	IdentityCacheSize int `yaml:"identity_cache_size"`
	// IdentityCacheTTL bounds how long a cached identity may live without re-auth.
	// Zero or negative disables TTL eviction.
	IdentityCacheTTL time.Duration `yaml:"identity_cache_ttl"`

	// TLS configures the outbound connection to the auth service. Setting
	// cert_file/key_file makes it mutual, which is how a callout endpoint that
	// checks no bearer token authenticates FluxMQ. Omit for the default
	// transport.
	TLS *mqtttls.ClientConfig `yaml:"tls,omitempty"`
}

// Local principal roles. A role is the capability a principal carries on every
// listener it authenticates to, so it cannot be widened by choosing a port.
const (
	// LocalRolePublisher may only publish. It runs no consumer and may not
	// relay an origin identity, because its publications are its own records.
	LocalRolePublisher = "publisher"
	// LocalRoleService may additionally consume, subject to its subscribe ACL,
	// and may relay the origin identity of messages it did not author.
	LocalRoleService = "service"
)

var knownLocalRoles = map[string]struct{}{
	LocalRolePublisher: {},
	LocalRoleService:   {},
}

// LocalPrincipalConfig configures a principal authenticated by FluxMQ itself.
type LocalPrincipalConfig struct {
	Name              string `yaml:"name"`
	CertificateURISAN string `yaml:"certificate_uri_san"`
	// Role is the principal's capability, defaulting to the least privileged
	// one. It lives here rather than on a listener because nothing binds a
	// principal to a listener: a capability granted by a port would be granted
	// to every principal that can reach it.
	Role               string                 `yaml:"role,omitempty"`
	CurrentSecretFile  string                 `yaml:"current_secret_file"`
	PreviousSecretFile string                 `yaml:"previous_secret_file,omitempty"`
	Permissions        LocalPermissionsConfig `yaml:"permissions"`
}

// EffectiveRole returns the configured role, defaulting to the least
// privileged one when unset.
func (c LocalPrincipalConfig) EffectiveRole() string {
	if c.Role == "" {
		return LocalRolePublisher
	}
	return c.Role
}

// LocalPermissionsConfig contains the publish and subscribe ACLs of one local
// principal. A subscribe entry is a dot-separated queue name or a pattern
// matching queue names; see NormalizeLocalSubscribeEntry.
type LocalPermissionsConfig struct {
	Publish   []LocalPublishPermission `yaml:"publish"`
	Subscribe []string                 `yaml:"subscribe"`
}

// NormalizeLocalSubscribeEntry converts one subscribe ACL entry into the
// canonical form the runtime matches queue names against.
//
// Only the wildcard spelling is normalized: "*" becomes "+". Which of the two a
// service writes follows the protocol it speaks rather than what it is asking
// for, so "m.*.events" and "m.+.events" are one grant.
//
// Separators are deliberately left alone. A queue name is a name, not an
// address: nothing constrains the characters in one, so a queue may legitimately
// be called "audit.events" or "a/b" or "$internal". Translating "." to "/" would
// make "a.b" and "a/b" the same key and let a grant on one authorize the other.
//
// The consequence is that "*", "+" and "#" cannot appear literally in a queue
// name an ACL entry names. That is the same trade every wildcard syntax makes.
//
// Config validation and the runtime store share this function, so a pattern
// cannot pass the startup check and then be matched differently at runtime.
func NormalizeLocalSubscribeEntry(entry string) string {
	if !strings.Contains(entry, "*") {
		return entry
	}
	return strings.ReplaceAll(entry, "*", "+")
}

// LocalSubscribeEntryIsPattern reports whether a normalized subscribe entry
// carries a wildcard and so must be matched rather than looked up.
func LocalSubscribeEntryIsPattern(normalized string) bool {
	return strings.ContainsAny(normalized, "+#")
}

// MatchLocalSubscribeQueue reports whether a normalized subscribe pattern grants
// a queue.
//
// Levels are separated by "."; "+" matches exactly one level and "#" matches
// zero or more trailing levels, so "audit.#" grants "audit" itself. The queue name
// is matched literally, so a "/" or "$" in it is an ordinary character rather
// than structure.
func MatchLocalSubscribeQueue(normalizedPattern, queue string) bool {
	if normalizedPattern == "" || queue == "" {
		return false
	}
	if normalizedPattern == queue {
		return true
	}

	remainingQueue := queue
	queueExhausted := false
	for {
		level, patternRest, patternHasMore := strings.Cut(normalizedPattern, ".")

		if level == "#" {
			return true
		}
		if queueExhausted {
			return false
		}

		queueLevel, queueRest, queueHasMore := strings.Cut(remainingQueue, ".")

		if level != "+" && level != queueLevel {
			return false
		}
		if !patternHasMore {
			return !queueHasMore
		}

		normalizedPattern = patternRest
		if queueHasMore {
			remainingQueue = queueRest
			continue
		}
		queueExhausted = true
	}
}

// localSubscribeQueuePrefix is the address prefix a client uses to reach a
// queue. It is duplicated from the routing resolver rather than imported to
// keep config free of a dependency on the broker packages that read it.
const localSubscribeQueuePrefix = "$queue/"

// validateLocalSubscribeEntry checks wildcard placement in a normalized entry.
// It mirrors MQTT filter rules over "." levels: "#" must be the whole final
// level and "+" must be a whole level.
func validateLocalSubscribeEntry(normalized string) error {
	levels := strings.Split(normalized, ".")
	for i, level := range levels {
		if strings.Contains(level, "#") && (level != "#" || i != len(levels)-1) {
			return fmt.Errorf("%q must be the entire final level", "#")
		}
		if strings.Contains(level, "+") && level != "+" {
			return fmt.Errorf("%q must be an entire level", "+")
		}
	}
	return nil
}

// LocalPublishPermission grants publish access to an AMQP target, named either
// exactly or by routing-key prefix. Exactly one of RoutingKey and
// RoutingKeyPrefix must be set.
//
// An exact permission is what a durable-stream publisher needs: the routing key
// names the queue it appends to, so it must also appear under queues.
//
// A prefix permission exists because a service publishes to topics derived from
// its own runtime data — a tenant identifier, a channel identifier — which
// cannot be enumerated in broker configuration. It grants every routing key
// under the prefix and is checked against no queue, so it authorizes topic
// publishing rather than a durable append. Keep the prefix as narrow as the
// service's topic namespace allows: it is what separates one service's reach
// from another's.
type LocalPublishPermission struct {
	Exchange         string `yaml:"exchange"`
	RoutingKey       string `yaml:"routing_key,omitempty"`
	RoutingKeyPrefix string `yaml:"routing_key_prefix,omitempty"`
}

// IsPrefix reports whether the permission grants a routing-key prefix rather
// than one exact routing key.
func (p LocalPublishPermission) IsPrefix() bool {
	return p.RoutingKeyPrefix != ""
}

// UnmarshalYAML keeps the auth subtree strict without changing the historical
// permissive decoding behavior of unrelated configuration sections.
func (a *AuthConfig) UnmarshalYAML(node *yaml.Node) error {
	if err := validateAuthYAML(node); err != nil {
		return err
	}
	type plainAuthConfig AuthConfig
	var decoded plainAuthConfig
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*a = AuthConfig(decoded)
	return nil
}

func validateAuthYAML(node *yaml.Node) error {
	return validateYAMLMapping(node, "auth", map[string]func(*yaml.Node) error{
		"external": func(external *yaml.Node) error {
			return validateYAMLMapping(external, "auth.external", map[string]func(*yaml.Node) error{
				authURLField:               nil,
				authTransportField:         nil,
				authTimeoutField:           nil,
				authProtocolsField:         nil,
				authIdentityCacheSizeField: nil,
				authIdentityCacheTTLField:  nil,
				authTLSField:               nil,
			})
		},
		"local_principals": func(principals *yaml.Node) error {
			return validateYAMLSequence(principals, "auth.local_principals", validateLocalPrincipalYAML)
		},
	})
}

// ValidateAgainstRuntime checks the rules that depend on what the process is
// actually running rather than on what the new file asks for.
//
// Validate sees one config and answers for a fresh start. A reload is different:
// its runtime-safe fields take effect immediately while restart-required ones do
// not, so the two halves of a cross-field rule can come from different
// configurations. Clustering is restart-required and local principals are
// runtime-safe, so a reload that disables clustering and adds an exact publish
// target in the same edit would pass Validate and then apply the target inside a
// still-clustered runtime, writing records no other node forwards. Ask the
// cluster question of the running config.
func ValidateAgainstRuntime(running, next *Config) error {
	if running == nil || next == nil || !running.Cluster.Enabled {
		return nil
	}
	// Listener changes are restart-required too. Ask whether the running
	// process has a local listener; removing it from the desired file does not
	// stop that listener before the runtime-safe principal snapshot is applied.
	if len(running.Server.AMQP091.LocalListeners()) == 0 {
		return nil
	}
	name, target, found := firstExactPublishTarget(next.Auth.LocalPrincipals)
	if !found {
		return nil
	}
	return fmt.Errorf("auth.local_principals %q grants the exact publish target %q, which cannot be applied while the running node is clustered: clustering is restart-required, so disabling it in the same reload does not take effect until restart; restart the node to change both together", name, target)
}

// firstExactPublishTarget reports the first principal granting an exact publish
// target, naming it so the operator can find the entry to change.
func firstExactPublishTarget(principals []LocalPrincipalConfig) (principal, target string, found bool) {
	for _, candidate := range principals {
		for _, permission := range candidate.Permissions.Publish {
			if !permission.IsPrefix() {
				return candidate.Name, permission.RoutingKey, true
			}
		}
	}
	return "", "", false
}

func validateLocalPrincipalYAML(node *yaml.Node, path string) error {
	return validateYAMLMapping(node, path, map[string]func(*yaml.Node) error{
		"name":                 nil,
		"certificate_uri_san":  nil,
		"role":                 nil,
		"current_secret_file":  nil,
		"previous_secret_file": nil,
		"permissions": func(permissions *yaml.Node) error {
			return validateYAMLMapping(permissions, path+".permissions", map[string]func(*yaml.Node) error{
				"publish": func(publish *yaml.Node) error {
					return validateYAMLSequence(publish, path+".permissions.publish", func(entry *yaml.Node, entryPath string) error {
						return validateYAMLMapping(entry, entryPath, map[string]func(*yaml.Node) error{
							"exchange":           nil,
							"routing_key":        nil,
							"routing_key_prefix": nil,
						})
					})
				},
				"subscribe": func(subscribe *yaml.Node) error {
					return validateYAMLSequence(subscribe, path+".permissions.subscribe", nil)
				},
			})
		},
	})
}

func validateYAMLMapping(node *yaml.Node, path string, fields map[string]func(*yaml.Node) error) error {
	if node.Tag == "!!null" {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("%s must be a mapping", path)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		name := node.Content[i].Value
		validate, ok := fields[name]
		if !ok {
			return fmt.Errorf("%s: field %s not found", path, name)
		}
		if validate != nil {
			if err := validate(node.Content[i+1]); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateYAMLSequence(node *yaml.Node, path string, validate func(*yaml.Node, string) error) error {
	if node.Tag == "!!null" {
		return nil
	}
	if node.Kind != yaml.SequenceNode {
		return fmt.Errorf("%s must be a sequence", path)
	}
	if validate == nil {
		return nil
	}
	for i, entry := range node.Content {
		if err := validate(entry, fmt.Sprintf("%s[%d]", path, i)); err != nil {
			return err
		}
	}
	return nil
}

// HooksConfig configures the optional blocking hook callout.
type HooksConfig struct {
	// URL is the hook service address. Empty disables blocking hooks.
	URL string `yaml:"url"`
	// Transport selects the callout wire format: "grpc" (default) or "http".
	Transport string `yaml:"transport"`
	// Timeout is the per-call timeout. Zero uses the hook client default.
	Timeout time.Duration `yaml:"timeout"`
	// FailMode controls behavior when a blocking hook errors: "deny" (default)
	// blocks the operation; "allow" keeps the original topic/filter.
	FailMode string `yaml:"fail_mode"`
	// Protocols controls which protocols use blocking hooks. Empty or nil means
	// all protocols run hooks when URL is set.
	Protocols map[string]bool `yaml:"protocols"`
	// Events controls which blocking hooks are enabled. Empty or nil means all
	// supported blocking hooks run when URL is set.
	Events map[string]bool `yaml:"events"`
}

// knownAuthProtocols is the set of valid protocol names for auth config.
var knownAuthProtocols = map[string]bool{
	protocolMQTT: true, protocolAMQP: true, protocolAMQP091: true, "http": true, "coap": true,
}

var knownBlockingHooks = map[string]bool{
	"auth_on_register":    true,
	"auth_on_publish":     true,
	"auth_on_subscribe":   true,
	"auth_on_unsubscribe": true,
}

// EnabledFor reports whether the external auth callout is enabled for the given protocol.
// Returns true when auth is globally enabled (URL is set) and either no per-protocol
// filter is configured or the protocol is explicitly set to true.
func (a ExternalAuthConfig) EnabledFor(protocol string) bool {
	if a.URL == "" {
		return false
	}
	if len(a.Protocols) == 0 {
		return true
	}
	return a.Protocols[protocol]
}

// EnabledFor reports whether blocking hooks are enabled for the given protocol.
func (c HooksConfig) EnabledFor(protocol string) bool {
	if c.URL == "" {
		return false
	}
	if len(c.Protocols) == 0 {
		return true
	}
	return c.Protocols[protocol]
}

// QueueConfig defines configuration for a persistent queue.
type QueueConfig struct {
	Name         string           `yaml:"name"`
	Topics       []string         `yaml:"topics"`
	Reserved     bool             `yaml:"reserved"`
	Type         string           `yaml:"type"`
	PrimaryGroup string           `yaml:"primary_group"`
	Retention    QueueRetention   `yaml:"retention"`
	Limits       QueueLimits      `yaml:"limits"`
	Retry        QueueRetry       `yaml:"retry"`
	DLQ          QueueDLQ         `yaml:"dlq"`
	Replication  QueueReplication `yaml:"replication"`
}

// QueueLimits defines resource limits for a queue.
type QueueLimits struct {
	MaxMessageSize int64         `yaml:"max_message_size"`
	MaxDepth       int64         `yaml:"max_depth"`
	MessageTTL     time.Duration `yaml:"message_ttl"`
}

// QueueRetry defines retry policy for failed message delivery.
type QueueRetry struct {
	MaxRetries     int           `yaml:"max_retries"`
	InitialBackoff time.Duration `yaml:"initial_backoff"`
	MaxBackoff     time.Duration `yaml:"max_backoff"`
	Multiplier     float64       `yaml:"multiplier"`
}

// QueueDLQ defines dead-letter queue configuration.
type QueueDLQ struct {
	Enabled bool   `yaml:"enabled"`
	Topic   string `yaml:"topic"`
}

// QueueRetention defines retention policy for a queue.
type QueueRetention struct {
	MaxAge            time.Duration `yaml:"max_age"`
	MaxLengthBytes    int64         `yaml:"max_length_bytes"`
	MaxLengthMessages int64         `yaml:"max_length_messages"`
}

// QueueReplication defines per-queue replication settings.
type QueueReplication struct {
	Enabled           bool          `yaml:"enabled"`
	Group             string        `yaml:"group"`
	ReplicationFactor int           `yaml:"replication_factor"`
	Mode              string        `yaml:"mode"` // sync, async
	MinInSyncReplicas int           `yaml:"min_in_sync_replicas"`
	AckTimeout        time.Duration `yaml:"ack_timeout"`

	// Optional per-queue Raft tuning overrides (zero = use cluster defaults).
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`
	SnapshotInterval  time.Duration `yaml:"snapshot_interval"`
	SnapshotThreshold uint64        `yaml:"snapshot_threshold"`
}

// QueueManagerConfig defines runtime behavior for the queue manager.
type QueueManagerConfig struct {
	// AutoCommitInterval controls how often stream groups auto-commit offsets.
	// Zero means commit on every delivery batch.
	AutoCommitInterval time.Duration `yaml:"auto_commit_interval"`

	// Topic capture runs off the publish path so a stalled queue store cannot
	// delay subscribers. These bound that machinery. Zero selects the default.
	//
	// CaptureQueueDepth counts jobs rather than bytes, so the memory ceiling is
	// capture_workers x capture_queue_depth payloads. A deployment capturing
	// large messages should lower it.
	CaptureWorkers      int           `yaml:"capture_workers"`
	CaptureQueueDepth   int           `yaml:"capture_queue_depth"`
	CaptureDrainTimeout time.Duration `yaml:"capture_drain_timeout"`
}

// RateLimitConfig holds rate limiting configuration.
type RateLimitConfig struct {
	Enabled    bool                      `yaml:"enabled"`
	Connection ConnectionRateLimitConfig `yaml:"connection"`
	Message    MessageRateLimitConfig    `yaml:"message"`
	Subscribe  SubscribeRateLimitConfig  `yaml:"subscribe"`
}

// ConnectionRateLimitConfig holds per-IP connection rate limiting settings.
type ConnectionRateLimitConfig struct {
	Enabled         bool          `yaml:"enabled"`
	Rate            float64       `yaml:"rate"`             // connections per second per IP
	Burst           int           `yaml:"burst"`            // burst allowance
	CleanupInterval time.Duration `yaml:"cleanup_interval"` // cleanup interval for stale entries
}

// MessageRateLimitConfig holds per-client message rate limiting settings.
type MessageRateLimitConfig struct {
	Enabled bool    `yaml:"enabled"`
	Rate    float64 `yaml:"rate"`  // messages per second per client
	Burst   int     `yaml:"burst"` // burst allowance
}

// SubscribeRateLimitConfig holds per-client subscription rate limiting settings.
type SubscribeRateLimitConfig struct {
	Enabled bool    `yaml:"enabled"`
	Rate    float64 `yaml:"rate"`  // subscriptions per second per client
	Burst   int     `yaml:"burst"` // burst allowance
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	MQTT    MQTTConfig    `yaml:"mqtt"`
	HTTP    HTTPConfig    `yaml:"http"`
	CoAP    CoAPConfig    `yaml:"coap"`
	AMQP    AMQPConfig    `yaml:"amqp"`
	AMQP091 AMQP091Config `yaml:"amqp091"`

	HealthAddr      string        `yaml:"health_addr"`
	MetricsAddr     string        `yaml:"metrics_addr"` // Now used for OTLP endpoint
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	HealthEnabled   bool          `yaml:"health_enabled"`
	MetricsEnabled  bool          `yaml:"metrics_enabled"` // Now enables OTel

	// OpenTelemetry configuration
	OtelServiceName     string  `yaml:"otel_service_name"`
	OtelServiceVersion  string  `yaml:"otel_service_version"`
	OtelTracesEnabled   bool    `yaml:"otel_traces_enabled"`
	OtelMetricsEnabled  bool    `yaml:"otel_metrics_enabled"`
	OtelTraceSampleRate float64 `yaml:"otel_trace_sample_rate"` // 0.0 to 1.0

	// OtelInsecure forces a cleartext OTLP/gRPC connection. Default is false:
	// system-trust TLS is used unless OtelCAFile is set. Set true only when
	// the collector is reachable on localhost or a trusted network.
	OtelInsecure bool   `yaml:"otel_insecure"`
	OtelCAFile   string `yaml:"otel_ca_file"`   // optional PEM bundle for verifying the collector
	OtelCertFile string `yaml:"otel_cert_file"` // client cert for mTLS to the collector
	OtelKeyFile  string `yaml:"otel_key_file"`  // client key for mTLS to the collector

	// Admin API server (HTTP + Connect/gRPC). Empty disables the listener.
	AdminAPIAddr string `yaml:"admin_api_addr"`
}

// MQTTTCPListenerConfig holds TCP listener configuration.
type MQTTTCPListenerConfig struct {
	Addr           string         `yaml:"addr"`
	MaxConnections int            `yaml:"max_connections"`
	ReadTimeout    time.Duration  `yaml:"read_timeout"`
	WriteTimeout   time.Duration  `yaml:"write_timeout"`
	Protocol       string         `yaml:"protocol"`
	TLS            mqtttls.Config `yaml:",inline"`
}

// MQTTConfig groups every MQTT listener, by transport. AMQP 0.9.1 and AMQP 1.0
// have their own sections on ServerConfig; keeping MQTT's transports together
// under one key is what distinguishes `server.mqtt.tcp` from a generic TCP
// listener that some other protocol might own.
type MQTTConfig struct {
	TCP       MQTTTCPConfig       `yaml:"tcp"`
	WebSocket MQTTWebSocketConfig `yaml:"websocket"`
}

// MQTTTCPConfig groups MQTT-over-TCP listeners by mode.
type MQTTTCPConfig struct {
	V3   MQTTTCPListenerConfig `yaml:"v3"`
	V5   MQTTTCPListenerConfig `yaml:"v5"`
	TLS  MQTTTCPListenerConfig `yaml:"tls"`
	MTLS MQTTTCPListenerConfig `yaml:"mtls"`
}

// MQTTWebSocketListenerConfig holds WebSocket listener configuration.
type MQTTWebSocketListenerConfig struct {
	Addr           string         `yaml:"addr"`
	Path           string         `yaml:"path"`
	Protocol       string         `yaml:"protocol"`
	MaxConnections int            `yaml:"max_connections"`
	ReadTimeout    time.Duration  `yaml:"read_timeout"`
	WriteTimeout   time.Duration  `yaml:"write_timeout"`
	AllowedOrigins []string       `yaml:"allowed_origins"`
	TLS            mqtttls.Config `yaml:",inline"`
}

// MQTTWebSocketConfig groups WebSocket listeners by mode.
type MQTTWebSocketConfig struct {
	V3   MQTTWebSocketListenerConfig `yaml:"v3"`
	V5   MQTTWebSocketListenerConfig `yaml:"v5"`
	TLS  MQTTWebSocketListenerConfig `yaml:"tls"`
	MTLS MQTTWebSocketListenerConfig `yaml:"mtls"`
}

// HTTPListenerConfig holds HTTP listener configuration.
type HTTPListenerConfig struct {
	Addr string         `yaml:"addr"`
	TLS  mqtttls.Config `yaml:",inline"`
}

// HTTPConfig groups HTTP listeners by mode.
type HTTPConfig struct {
	Plain HTTPListenerConfig `yaml:"plain"`
	TLS   HTTPListenerConfig `yaml:"tls"`
	MTLS  HTTPListenerConfig `yaml:"mtls"`
}

// CoAPListenerConfig holds CoAP listener configuration.
type CoAPListenerConfig struct {
	Addr string         `yaml:"addr"`
	TLS  mqtttls.Config `yaml:",inline"`
}

// CoAPConfig groups CoAP listeners by mode.
type CoAPConfig struct {
	Plain CoAPListenerConfig `yaml:"plain"`
	DTLS  CoAPListenerConfig `yaml:"dtls"`
	MDTLS CoAPListenerConfig `yaml:"mdtls"`
}

// AMQPListenerConfig holds AMQP listener configuration.
type AMQPListenerConfig struct {
	Addr           string         `yaml:"addr"`
	MaxConnections int            `yaml:"max_connections"`
	TLS            mqtttls.Config `yaml:",inline"`
}

// AMQPConfig groups AMQP listeners by mode.
type AMQPConfig struct {
	Plain AMQPListenerConfig `yaml:"plain"`
	TLS   AMQPListenerConfig `yaml:"tls"`
	MTLS  AMQPListenerConfig `yaml:"mtls"`
}

// AMQP091ListenerConfig holds AMQP 0.9.1 listener configuration.
type AMQP091ListenerConfig struct {
	Addr           string         `yaml:"addr"`
	MaxConnections int            `yaml:"max_connections"`
	TLS            mqtttls.Config `yaml:",inline"`
}

// AMQP091Config groups AMQP 0.9.1 listeners by mode.
type AMQP091Config struct {
	Plain AMQP091ListenerConfig `yaml:"plain"`
	TLS   AMQP091ListenerConfig `yaml:"tls"`
	MTLS  AMQP091ListenerConfig `yaml:"mtls"`
	// Local admits principals declared in auth.local_principals over mTLS. It
	// confers no capability of its own: what a principal may do comes from its
	// role. Configure more than one only to place them on separate networks.
	Local AMQP091ListenerConfig `yaml:"local"`
	// Internal and Service are deprecated aliases for Local, kept so existing
	// configurations keep working. They behave identically to it and to each
	// other; new configurations should use Local.
	Internal AMQP091ListenerConfig `yaml:"internal"`
	Service  AMQP091ListenerConfig `yaml:"service"`
}

// LocalListeners returns every configured local-principal listener with the
// configuration key that named it, so callers do not have to know which of the
// deprecated aliases an operator used.
func (c AMQP091Config) LocalListeners() []NamedAMQP091Listener {
	candidates := []NamedAMQP091Listener{
		{Name: listenerNameLocal, Config: c.Local},
		{Name: listenerNameInternal, Config: c.Internal},
		{Name: listenerNameService, Config: c.Service},
	}
	listeners := make([]NamedAMQP091Listener, 0, len(candidates))
	for _, candidate := range candidates {
		if hasAddr(candidate.Config.Addr) {
			listeners = append(listeners, candidate)
		}
	}
	return listeners
}

// DeprecatedLocalListenerNames returns the deprecated keys an operator used, so
// startup can name them in a warning.
func (c AMQP091Config) DeprecatedLocalListenerNames() []string {
	var names []string
	if hasAddr(c.Internal.Addr) {
		names = append(names, listenerNameInternal)
	}
	if hasAddr(c.Service.Addr) {
		names = append(names, listenerNameService)
	}
	return names
}

// NamedAMQP091Listener pairs a listener with the configuration key that named it.
type NamedAMQP091Listener struct {
	Name   string
	Config AMQP091ListenerConfig
}

// DefaultMaxInflightMessages is the fallback for Session.MaxInflightMessages
// when it is unset (<= 0). Shared by the broker and the session so the
// send-quota clamp is consistent across the first CONNECT.
const DefaultMaxInflightMessages = 256

// InflightOverflowMode controls what happens when a session's inflight window is full.
type InflightOverflowMode int

const (
	// InflightOverflowBackpressure blocks the caller until a slot opens.
	// In sync fan-out mode this stalls the publisher's read loop.
	// In async fan-out mode this stalls the pool worker for that subscriber while
	// other workers continue, providing natural flow control.
	InflightOverflowBackpressure InflightOverflowMode = iota

	// InflightOverflowQueue buffers excess messages in a per-session pending queue.
	// The pool worker moves on immediately; the subscriber drains the queue as ACKs arrive.
	// On disconnect, pending messages are promoted to the offline queue (QoS > 0 only).
	InflightOverflowQueue
)

// BrokerConfig holds broker-specific settings.
type BrokerConfig struct {
	// MaxMessageSize is the maximum message payload in bytes. It also bounds
	// what a peer may make the broker buffer: AMQP 0.9.1 rejects a larger
	// advertised body, and the MQTT listeners reject a packet whose remaining
	// length exceeds this size plus an allowance for topic and properties.
	MaxMessageSize int `yaml:"max_message_size"`

	// Retained message limits
	MaxRetainedMessages int `yaml:"max_retained_messages"`

	// QoS retry settings
	RetryInterval time.Duration `yaml:"retry_interval"`
	MaxRetries    int           `yaml:"max_retries"`

	// Maximum QoS level supported (0, 1, or 2). Default: 2
	// Server will downgrade publish QoS to this level per MQTT 5.0 spec
	MaxQoS int `yaml:"max_qos"`

	// AsyncFanOut decouples subscriber distribution from the publisher handshake.
	// When true, PUBCOMP is sent to the publisher immediately after PUBREL is
	// processed and message ownership is confirmed; fan-out to subscribers runs
	// in a bounded worker pool. This prevents slow or numerous subscribers from
	// blocking publisher throughput.
	// When false (default), PUBCOMP is sent only after all local subscribers have
	// been queued, preserving strict ordering between publisher ack and delivery.
	AsyncFanOut bool `yaml:"async_fan_out"`

	// FanOutWorkers is the number of goroutines in the async fan-out pool.
	// 0 (default) uses GOMAXPROCS. Only effective when AsyncFanOut is true.
	FanOutWorkers int `yaml:"fan_out_workers"`
}

// Offline queue policy values for SessionConfig.OfflineQueuePolicy.
const (
	OfflineQueuePolicyEvict  = "evict"  // drop oldest message when queue is full
	OfflineQueuePolicyReject = "reject" // reject new message when queue is full
)

// SessionConfig holds session management settings.
type SessionConfig struct {
	// Maximum sessions allowed
	MaxSessions int `yaml:"max_sessions"`

	// Default expiry interval (seconds) if client doesn't specify
	DefaultExpiryInterval uint32 `yaml:"default_expiry_interval"`

	// Maximum queued messages per offline client
	MaxOfflineQueueSize int `yaml:"max_offline_queue_size"`

	// Maximum inflight messages per session
	MaxInflightMessages int `yaml:"max_inflight_messages"`

	// Offline queue eviction policy: "evict" (drop oldest) or "reject" (reject new)
	OfflineQueuePolicy string `yaml:"offline_queue_policy"`

	// MaxSendQueueSize controls per-connection outbound send queue depth.
	// 0 keeps synchronous writes; values > 0 enable asynchronous send queues.
	MaxSendQueueSize int `yaml:"max_send_queue_size"`

	// DisconnectOnFull controls behavior when send queue is full in async mode.
	// false blocks the producer (backpressure), true disconnects the slow client.
	DisconnectOnFull bool `yaml:"disconnect_on_full"`

	// InflightOverflow controls what happens when a subscriber's inflight window
	// (bounded by ReceiveMaximum) is full during fan-out.
	// 0 (InflightOverflowBackpressure, default): block the caller until a slot opens.
	// 1 (InflightOverflowQueue): overflow into a per-session bounded pending queue;
	//   the subscriber drains it as ACKs arrive.
	InflightOverflow InflightOverflowMode `yaml:"inflight_overflow"`

	// PendingQueueSize is the per-session pending message queue capacity.
	// Only used when InflightOverflow is InflightOverflowQueue. Default: 1000.
	PendingQueueSize int `yaml:"pending_queue_size"`
}

// LogConfig holds logging configuration.
type LogConfig struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // text, json
}

// StorageConfig holds storage backend configuration.
type StorageConfig struct {
	Type string `yaml:"type"` // memory, badger

	// BadgerDB settings. BadgerSyncWrites fsyncs every write to the broker
	// key-value store, which holds retained messages and sessions. It does not
	// reach the queue append-only log: queue durability is a separate engine,
	// and the acknowledgement policy for it is not configurable yet. The key is
	// named for the engine it configures so the two cannot be confused.
	BadgerDir        string `yaml:"badger_dir"`
	BadgerSyncWrites bool   `yaml:"badger_sync_writes"`

	// RecoverOnStartup runs segment recovery before loading queues.
	// Corrupted segments are truncated at the last valid batch and indexes
	// are rebuilt. Disabled by default to avoid unexpected data loss.
	RecoverOnStartup bool `yaml:"recover_on_startup"`
}

// ClusterConfig holds clustering configuration.
type ClusterConfig struct {
	Enabled bool   `yaml:"enabled"`
	NodeID  string `yaml:"node_id"`

	// Embedded etcd settings
	Etcd EtcdConfig `yaml:"etcd"`

	// Inter-broker transport
	Transport TransportConfig `yaml:"transport"`

	// Raft replication for queue data
	Raft RaftConfig `yaml:"raft"`
}

// RaftConfig holds Raft replication configuration for queue data.
type RaftConfig struct {
	Enabled             bool              `yaml:"enabled"`
	AutoProvisionGroups bool              `yaml:"auto_provision_groups"` // Dynamically provision groups not listed in `groups`
	ReplicationFactor   int               `yaml:"replication_factor"`    // Number of replicas per partition (default: 3)
	SyncMode            bool              `yaml:"sync_mode"`             // true=wait for quorum, false=async
	MinInSyncReplicas   int               `yaml:"min_in_sync_replicas"`
	AckTimeout          time.Duration     `yaml:"ack_timeout"`
	WritePolicy         string            `yaml:"write_policy"`      // local, reject, forward
	DistributionMode    string            `yaml:"distribution_mode"` // forward, replicate
	BindAddr            string            `yaml:"bind_addr"`         // Base address for Raft (e.g., "127.0.0.1:7100")
	DataDir             string            `yaml:"data_dir"`          // Directory for Raft data
	Peers               map[string]string `yaml:"peers"`             // Map of nodeID -> raft base address

	// Raft tuning
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`
	SnapshotInterval  time.Duration `yaml:"snapshot_interval"`
	SnapshotThreshold uint64        `yaml:"snapshot_threshold"`

	// Optional per-group overrides for true multi-group replication.
	// The key "default" overrides the base group above.
	Groups map[string]RaftGroupConfig `yaml:"groups"`
}

// RaftGroupConfig defines overrides for an individual Raft replication group.
type RaftGroupConfig struct {
	Enabled *bool `yaml:"enabled"`

	// Group network/storage endpoints.
	BindAddr string            `yaml:"bind_addr"` // Raft bind address for this group
	DataDir  string            `yaml:"data_dir"`  // Data dir for this group
	Peers    map[string]string `yaml:"peers"`     // nodeID -> raft bind address for this group

	// Optional per-group replication behavior overrides (zero/nil = inherit base RaftConfig).
	ReplicationFactor int           `yaml:"replication_factor"`
	SyncMode          *bool         `yaml:"sync_mode"`
	MinInSyncReplicas int           `yaml:"min_in_sync_replicas"`
	AckTimeout        time.Duration `yaml:"ack_timeout"`

	// Optional per-group Raft tuning overrides.
	HeartbeatTimeout  time.Duration `yaml:"heartbeat_timeout"`
	ElectionTimeout   time.Duration `yaml:"election_timeout"`
	SnapshotInterval  time.Duration `yaml:"snapshot_interval"`
	SnapshotThreshold uint64        `yaml:"snapshot_threshold"`
}

// EtcdConfig holds embedded etcd configuration.
type EtcdConfig struct {
	DataDir        string `yaml:"data_dir"`
	BindAddr       string `yaml:"bind_addr"`       // Peer address (e.g., "0.0.0.0:2380")
	ClientAddr     string `yaml:"client_addr"`     // Client address (e.g., "0.0.0.0:2379")
	InitialCluster string `yaml:"initial_cluster"` // "node1=http://host1:2380,node2=http://host2:2380"
	Bootstrap      bool   `yaml:"bootstrap"`       // true only for first node

	// Hybrid retained message storage threshold (in bytes)
	// Messages smaller than this are replicated to all nodes via etcd
	// Messages larger than this are stored on owner node and fetched on-demand via gRPC
	// Default: 1024 (1KB)
	HybridRetainedSizeThreshold int `yaml:"hybrid_retained_size_threshold"`
}

// TransportConfig holds inter-broker transport configuration.
type TransportConfig struct {
	BindAddr string            `yaml:"bind_addr"` // gRPC address (e.g., "0.0.0.0:7948")
	Peers    map[string]string `yaml:"peers"`     // Map of nodeID -> transport address for peers

	// Inter-node routing batch policy.
	// route_batch_max_size controls flush size.
	// route_batch_max_delay controls max wait before flushing a partial batch.
	// route_batch_flush_workers controls the number of concurrent flush
	// goroutines per remote node. Higher values increase throughput when
	// gRPC calls are slow but use more goroutines. Default: 4.
	RouteBatchMaxSize      int           `yaml:"route_batch_max_size"`
	RouteBatchMaxDelay     time.Duration `yaml:"route_batch_max_delay"`
	RouteBatchFlushWorkers int           `yaml:"route_batch_flush_workers"`

	// RoutePublishTimeout is the maximum time to wait for a cross-cluster
	// publish to complete (including retries). Zero uses the default (15s).
	RoutePublishTimeout time.Duration `yaml:"route_publish_timeout"`

	// TLS configuration for inter-broker communication
	TLSEnabled  bool   `yaml:"tls_enabled"`   // Enable TLS for gRPC transport
	TLSCertFile string `yaml:"tls_cert_file"` // Server certificate file
	TLSKeyFile  string `yaml:"tls_key_file"`  // Server private key file
	TLSCAFile   string `yaml:"tls_ca_file"`   // CA certificate for verifying peer certificates
}

// WebhookConfig holds webhook notification configuration.
type WebhookConfig struct {
	Enabled         bool              `yaml:"enabled"`
	QueueSize       int               `yaml:"queue_size"`
	DropPolicy      string            `yaml:"drop_policy"`      // "oldest" or "newest"
	Workers         int               `yaml:"workers"`          // Number of worker goroutines
	IncludePayload  bool              `yaml:"include_payload"`  // Include message payload in events
	ShutdownTimeout time.Duration     `yaml:"shutdown_timeout"` // Graceful shutdown timeout
	Defaults        WebhookDefaults   `yaml:"defaults"`
	Endpoints       []WebhookEndpoint `yaml:"endpoints"`
}

// WebhookDefaults holds default settings for webhook endpoints.
type WebhookDefaults struct {
	Timeout        time.Duration        `yaml:"timeout"`
	Retry          RetryConfig          `yaml:"retry"`
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
}

// RetryConfig holds retry configuration for webhook delivery.
type RetryConfig struct {
	MaxAttempts     int           `yaml:"max_attempts"`
	InitialInterval time.Duration `yaml:"initial_interval"`
	MaxInterval     time.Duration `yaml:"max_interval"`
	Multiplier      float64       `yaml:"multiplier"`
}

// CircuitBreakerConfig holds circuit breaker configuration.
type CircuitBreakerConfig struct {
	FailureThreshold int           `yaml:"failure_threshold"`
	ResetTimeout     time.Duration `yaml:"reset_timeout"`
}

// WebhookEndpoint defines a single webhook endpoint configuration.
type WebhookEndpoint struct {
	Name         string            `yaml:"name"`
	Type         string            `yaml:"type"` // "http" (future: "grpc")
	URL          string            `yaml:"url"`
	Events       []string          `yaml:"events"`        // Event type filter (empty = all)
	TopicFilters []string          `yaml:"topic_filters"` // Topic pattern filter (empty = all)
	Headers      map[string]string `yaml:"headers"`
	Timeout      time.Duration     `yaml:"timeout,omitempty"` // Override default
	Retry        *RetryConfig      `yaml:"retry,omitempty"`   // Override default
}

// Default returns a configuration with sensible defaults.
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			MQTT: MQTTConfig{
				TCP: MQTTTCPConfig{
					V3: MQTTTCPListenerConfig{
						Addr:           defaultTCPV3Addr,
						MaxConnections: 10000,
						ReadTimeout:    60 * time.Second,
						WriteTimeout:   60 * time.Second,
						Protocol:       ProtocolModeV3,
					},
					V5: MQTTTCPListenerConfig{
						Addr:           defaultTCPV5Addr,
						MaxConnections: 10000,
						ReadTimeout:    60 * time.Second,
						WriteTimeout:   60 * time.Second,
						Protocol:       ProtocolModeV5,
					},
					TLS: MQTTTCPListenerConfig{
						MaxConnections: 10000,
						ReadTimeout:    60 * time.Second,
						WriteTimeout:   60 * time.Second,
						Protocol:       ProtocolModeAuto,
					},
					MTLS: MQTTTCPListenerConfig{
						MaxConnections: 10000,
						ReadTimeout:    60 * time.Second,
						WriteTimeout:   60 * time.Second,
						Protocol:       ProtocolModeAuto,
					},
				},
				WebSocket: MQTTWebSocketConfig{
					V3: MQTTWebSocketListenerConfig{
						Addr:           ":8083",
						Path:           defaultWSPath,
						Protocol:       ProtocolModeV3,
						MaxConnections: 10000,
						ReadTimeout:    60 * time.Second,
						WriteTimeout:   60 * time.Second,
					},
					V5: MQTTWebSocketListenerConfig{
						Addr:           ":8084",
						Path:           defaultWSPath,
						Protocol:       ProtocolModeV5,
						MaxConnections: 10000,
						ReadTimeout:    60 * time.Second,
						WriteTimeout:   60 * time.Second,
					},
					TLS: MQTTWebSocketListenerConfig{
						Path:           defaultWSPath,
						Protocol:       ProtocolModeAuto,
						MaxConnections: 10000,
						ReadTimeout:    60 * time.Second,
						WriteTimeout:   60 * time.Second,
					},
					MTLS: MQTTWebSocketListenerConfig{
						Path:           defaultWSPath,
						Protocol:       ProtocolModeAuto,
						MaxConnections: 10000,
						ReadTimeout:    60 * time.Second,
						WriteTimeout:   60 * time.Second,
					},
				},
			},
			HTTP: HTTPConfig{
				Plain: HTTPListenerConfig{},
				TLS:   HTTPListenerConfig{},
				MTLS:  HTTPListenerConfig{},
			},
			CoAP: CoAPConfig{
				Plain: CoAPListenerConfig{},
				DTLS:  CoAPListenerConfig{},
				MDTLS: CoAPListenerConfig{},
			},
			AMQP: AMQPConfig{
				Plain: AMQPListenerConfig{
					Addr:           ":5672",
					MaxConnections: 10000,
				},
				TLS: AMQPListenerConfig{
					MaxConnections: 10000,
				},
				MTLS: AMQPListenerConfig{
					MaxConnections: 10000,
				},
			},
			AMQP091: AMQP091Config{
				Plain: AMQP091ListenerConfig{
					Addr:           ":5682",
					MaxConnections: 10000,
				},
				TLS: AMQP091ListenerConfig{
					MaxConnections: 10000,
				},
				MTLS: AMQP091ListenerConfig{
					MaxConnections: 10000,
				},
				Local:    AMQP091ListenerConfig{},
				Internal: AMQP091ListenerConfig{},
				Service:  AMQP091ListenerConfig{},
			},
			HealthAddr:      ":8081",
			HealthEnabled:   true,
			AdminAPIAddr:    ":8082",
			MetricsAddr:     "localhost:4317",
			MetricsEnabled:  false,
			ShutdownTimeout: 30 * time.Second,

			// OpenTelemetry defaults
			OtelServiceName:     "fluxmq",
			OtelServiceVersion:  "1.0.0",
			OtelMetricsEnabled:  true,
			OtelTracesEnabled:   false, // Disabled by default for performance
			OtelTraceSampleRate: 0.1,   // 10% sampling when enabled
		},
		Broker: BrokerConfig{
			MaxMessageSize:      1024 * 1024, // 1MB
			MaxRetainedMessages: 10000,
			RetryInterval:       20 * time.Second,
			MaxRetries:          0, // Infinite retries
			MaxQoS:              2, // Support all QoS levels
			AsyncFanOut:         false,
			FanOutWorkers:       0, // 0 = GOMAXPROCS
		},
		Session: SessionConfig{
			MaxSessions:           10000,
			DefaultExpiryInterval: 300, // 5 minutes
			MaxOfflineQueueSize:   1000,
			MaxInflightMessages:   256,
			OfflineQueuePolicy:    OfflineQueuePolicyEvict,
			MaxSendQueueSize:      0,
			DisconnectOnFull:      false,
			InflightOverflow:      InflightOverflowBackpressure,
			PendingQueueSize:      1000,
		},
		Log: LogConfig{
			Level:  logLevelInfo,
			Format: "text",
		},
		Storage: StorageConfig{
			Type:      storageTypeBadger,
			BadgerDir: "/tmp/fluxmq/data",
		},
		Cluster: ClusterConfig{
			Enabled: true,
			NodeID:  defaultNodeID,
			Etcd: EtcdConfig{
				DataDir:        "/tmp/fluxmq/etcd",
				BindAddr:       "0.0.0.0:2380",
				ClientAddr:     "0.0.0.0:2379",
				InitialCluster: "broker-1=http://0.0.0.0:2380",
				Bootstrap:      true,
			},
			Transport: TransportConfig{
				BindAddr: "0.0.0.0:7948",
				// Keep batches modest by default and latency low.
				RouteBatchMaxSize:      256,
				RouteBatchMaxDelay:     5 * time.Millisecond,
				RouteBatchFlushWorkers: 4,
				RoutePublishTimeout:    15 * time.Second,
			},
			Raft: RaftConfig{
				Enabled:             false, // Disabled by default
				AutoProvisionGroups: true,
				ReplicationFactor:   3,
				SyncMode:            true,
				MinInSyncReplicas:   2,
				AckTimeout:          5 * time.Second,
				WritePolicy:         writePolicyForward,
				DistributionMode:    "replicate",
				BindAddr:            "127.0.0.1:7100",
				DataDir:             "/tmp/fluxmq/raft",
				Peers:               map[string]string{},
				HeartbeatTimeout:    1 * time.Second,
				ElectionTimeout:     3 * time.Second,
				SnapshotInterval:    5 * time.Minute,
				SnapshotThreshold:   8192,
			},
		},
		Webhook: WebhookConfig{
			Enabled:         false,
			QueueSize:       10000,
			DropPolicy:      "oldest",
			Workers:         5,
			IncludePayload:  false,
			ShutdownTimeout: 30 * time.Second,
			Defaults: WebhookDefaults{
				Timeout: 5 * time.Second,
				Retry: RetryConfig{
					MaxAttempts:     3,
					InitialInterval: 1 * time.Second,
					MaxInterval:     30 * time.Second,
					Multiplier:      2.0,
				},
				CircuitBreaker: CircuitBreakerConfig{
					FailureThreshold: 5,
					ResetTimeout:     60 * time.Second,
				},
			},
			Endpoints: []WebhookEndpoint{},
		},
		RateLimit: RateLimitConfig{
			Enabled: false,
			Connection: ConnectionRateLimitConfig{
				Enabled:         true,
				Rate:            100.0 / 60.0, // 100 connections per minute per IP
				Burst:           20,
				CleanupInterval: 5 * time.Minute,
			},
			Message: MessageRateLimitConfig{
				Enabled: true,
				Rate:    1000, // 1000 messages per second per client
				Burst:   100,
			},
			Subscribe: SubscribeRateLimitConfig{
				Enabled: true,
				Rate:    100, // 100 subscriptions per second per client
				Burst:   10,
			},
		},
		QueueManager: QueueManagerConfig{
			AutoCommitInterval: 5 * time.Second,
		},
		Queues: []QueueConfig{
			{
				Name:     protocolMQTT,
				Topics:   []string{"$queue/#"},
				Reserved: true,
				Limits: QueueLimits{
					MaxMessageSize: 10 * 1024 * 1024, // 10MB
					MaxDepth:       100000,
					MessageTTL:     7 * 24 * time.Hour,
				},
				Retry: QueueRetry{
					MaxRetries:     10,
					InitialBackoff: 5 * time.Second,
					MaxBackoff:     5 * time.Minute,
					Multiplier:     2.0,
				},
				DLQ: QueueDLQ{
					Enabled: true,
				},
				Replication: QueueReplication{
					Enabled:           false,
					ReplicationFactor: 3,
					Mode:              queueModeSync,
					MinInSyncReplicas: 2,
					AckTimeout:        5 * time.Second,
				},
			},
		},
	}
}

// ErrConfigNotFound reports a config file that was named but does not exist.
// Callers that genuinely want defaults on a missing file use LoadOptional.
var ErrConfigNotFound = errors.New("config file not found")

// Load reads and validates a configuration file.
//
// A named file that does not exist is an error: silently falling back to
// defaults turns a typo in a unit file or chart into a broker running with
// none of the operator's settings, including their authentication and TLS
// settings. Use LoadOptional for the opt-in fallback.
//
// Decoding is strict. An unknown, misspelled, or misplaced key fails the load
// rather than being discarded, so a mistyped key can never silently drop the
// protection it was meant to configure.
func Load(filename string) (*Config, error) {
	if filename == "" {
		return Default(), nil
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrConfigNotFound, filename)
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	return parse(data)
}

// LoadOptional behaves like Load, except that a missing file yields the
// default configuration instead of an error. Every other failure — unreadable
// file, unknown key, invalid value — is still reported.
func LoadOptional(filename string) (*Config, error) {
	if filename == "" {
		return Default(), nil
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return Default(), nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	return parse(data)
}

func parse(data []byte) (*Config, error) {
	cfg := Default()
	if err := rejectLegacyAuthKeys(data); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	// io.EOF means an empty document, which leaves the defaults in place.
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// validateCalloutTLS rejects a TLS block that cannot do what it looks like it
// does. The pairing that matters is TLS material against an http:// URL: the
// connection would be cleartext while the config advertises a client
// certificate, so the operator would believe the callout is mutually
// authenticated when nothing about it is authenticated at all.
func validateCalloutTLS(path, rawURL string, tlsConfig *mqtttls.ClientConfig) error {
	if tlsConfig == nil || !tlsConfig.Configured() {
		return nil
	}
	if err := tlsConfig.Validate(); err != nil {
		return fmt.Errorf("%s.tls: %w", path, err)
	}
	if strings.HasPrefix(strings.TrimSpace(rawURL), "http://") {
		return fmt.Errorf("%s.tls is set but %s.url is http://; use https:// or remove the tls block", path, path)
	}
	return nil
}

var legacyAuthKeys = map[string]string{
	authURLField:               "auth.external.url",
	authTransportField:         "auth.external.transport",
	authTimeoutField:           "auth.external.timeout",
	authProtocolsField:         "auth.external.protocols",
	authIdentityCacheSizeField: "auth.external.identity_cache_size",
	authIdentityCacheTTLField:  "auth.external.identity_cache_ttl",
}

func rejectLegacyAuthKeys(data []byte) error {
	var document yaml.Node
	if err := yaml.Unmarshal(data, &document); err != nil {
		return err
	}
	if len(document.Content) == 0 {
		return nil
	}

	root := document.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "auth" {
			continue
		}
		auth := root.Content[i+1]
		if auth.Kind != yaml.MappingNode {
			return nil
		}
		for j := 0; j+1 < len(auth.Content); j += 2 {
			key := auth.Content[j].Value
			if replacement, ok := legacyAuthKeys[key]; ok {
				return fmt.Errorf("auth.%s is no longer supported; use %s", key, replacement)
			}
		}
		return nil
	}
	return nil
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	tcpSlots := []struct {
		name              string
		cfg               MQTTTCPListenerConfig
		requireClientAuth bool
		requireTLS        bool
		fixedProtocol     string
	}{
		{name: "v3", cfg: c.Server.MQTT.TCP.V3, fixedProtocol: ProtocolModeV3},
		{name: "v5", cfg: c.Server.MQTT.TCP.V5, fixedProtocol: ProtocolModeV5},
		{name: listenerNameTLS, cfg: c.Server.MQTT.TCP.TLS, requireClientAuth: false, requireTLS: true},
		{name: listenerNameMTLS, cfg: c.Server.MQTT.TCP.MTLS, requireClientAuth: true, requireTLS: true},
	}

	wsSlots := []struct {
		name              string
		cfg               MQTTWebSocketListenerConfig
		requireTLS        bool
		requireClientAuth bool
		fixedProtocol     string
	}{
		{name: "v3", cfg: c.Server.MQTT.WebSocket.V3, fixedProtocol: ProtocolModeV3},
		{name: "v5", cfg: c.Server.MQTT.WebSocket.V5, fixedProtocol: ProtocolModeV5},
		{name: listenerNameTLS, cfg: c.Server.MQTT.WebSocket.TLS, requireTLS: true},
		{name: listenerNameMTLS, cfg: c.Server.MQTT.WebSocket.MTLS, requireTLS: true, requireClientAuth: true},
	}

	httpSlots := []struct {
		name              string
		cfg               HTTPListenerConfig
		requireClientAuth bool
	}{
		{name: listenerNamePlain, cfg: c.Server.HTTP.Plain, requireClientAuth: false},
		{name: listenerNameTLS, cfg: c.Server.HTTP.TLS, requireClientAuth: false},
		{name: listenerNameMTLS, cfg: c.Server.HTTP.MTLS, requireClientAuth: true},
	}

	hasMessagingListener := false

	for _, slot := range tcpSlots {
		if err := validateListenerProtocol("server.mqtt.tcp."+slot.name+".protocol", slot.cfg.Protocol); err != nil {
			return err
		}
		mode := NormalizeProtocolMode(slot.cfg.Protocol)
		if slot.fixedProtocol != "" && mode != slot.fixedProtocol {
			return fmt.Errorf("server.mqtt.tcp.%s.protocol must be %q", slot.name, slot.fixedProtocol)
		}
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && !slot.requireTLS {
				return fmt.Errorf("server.mqtt.tcp.%s TLS fields are not supported for non-TLS listeners", slot.name)
			}
			continue
		}

		hasMessagingListener = true
		if slot.cfg.MaxConnections < 0 {
			return fmt.Errorf("server.mqtt.tcp.%s.max_connections cannot be negative", slot.name)
		}
		if slot.cfg.ReadTimeout < 0 {
			return fmt.Errorf("server.mqtt.tcp.%s.read_timeout cannot be negative", slot.name)
		}
		if slot.cfg.WriteTimeout < 0 {
			return fmt.Errorf("server.mqtt.tcp.%s.write_timeout cannot be negative", slot.name)
		}
		if slot.requireTLS {
			if err := validateListenerTLS("server.mqtt.tcp."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
		} else if tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.mqtt.tcp.%s TLS fields are not supported for non-TLS listeners", slot.name)
		}
	}

	for _, slot := range wsSlots {
		if err := validateListenerProtocol("server.mqtt.websocket."+slot.name+".protocol", slot.cfg.Protocol); err != nil {
			return err
		}
		mode := NormalizeProtocolMode(slot.cfg.Protocol)
		if slot.fixedProtocol != "" && mode != slot.fixedProtocol {
			return fmt.Errorf("server.mqtt.websocket.%s.protocol must be %q", slot.name, slot.fixedProtocol)
		}
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && !slot.requireTLS {
				return fmt.Errorf("server.mqtt.websocket.%s TLS fields are not supported for non-TLS listeners", slot.name)
			}
			continue
		}

		hasMessagingListener = true
		if slot.cfg.MaxConnections < 0 {
			return fmt.Errorf("server.mqtt.websocket.%s.max_connections cannot be negative", slot.name)
		}
		if slot.cfg.ReadTimeout < 0 {
			return fmt.Errorf("server.mqtt.websocket.%s.read_timeout cannot be negative", slot.name)
		}
		if slot.cfg.WriteTimeout < 0 {
			return fmt.Errorf("server.mqtt.websocket.%s.write_timeout cannot be negative", slot.name)
		}
		if slot.requireTLS {
			if err := validateListenerTLS("server.mqtt.websocket."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
		} else if tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.mqtt.websocket.%s TLS fields are not supported for non-TLS listeners", slot.name)
		}
	}

	c.Server.AdminAPIAddr = strings.TrimSpace(c.Server.AdminAPIAddr)
	if c.Server.AdminAPIAddr != "" && !hasAddr(c.Server.AdminAPIAddr) {
		return fmt.Errorf("server.admin_api_addr cannot be blank when set")
	}

	for _, slot := range httpSlots {
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && slot.name == listenerNamePlain {
				return fmt.Errorf("server.http.%s TLS fields are not supported for plain listeners", slot.name)
			}
			continue
		}
		hasMessagingListener = true

		if slot.name == listenerNamePlain && tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.http.%s TLS fields are not supported for plain listeners", slot.name)
		}
		if slot.name != listenerNamePlain {
			if err := validateListenerTLS("server.http."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
		}
	}

	coapSlots := []struct {
		name              string
		cfg               CoAPListenerConfig
		requireClientAuth bool
	}{
		{name: listenerNamePlain, cfg: c.Server.CoAP.Plain},
		{name: "dtls", cfg: c.Server.CoAP.DTLS},
		{name: "mdtls", cfg: c.Server.CoAP.MDTLS, requireClientAuth: true},
	}

	for _, slot := range coapSlots {
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && slot.name == listenerNamePlain {
				return fmt.Errorf("server.coap.%s TLS fields are not supported for plain listeners", slot.name)
			}
			continue
		}
		hasMessagingListener = true

		if slot.name == listenerNamePlain && tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.coap.%s TLS fields are not supported for plain listeners", slot.name)
		}
		if slot.name != listenerNamePlain {
			if err := validateListenerTLS("server.coap."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
		}
	}

	// AMQP validation
	amqpSlots := []struct {
		name              string
		cfg               AMQPListenerConfig
		requireClientAuth bool
	}{
		{name: listenerNamePlain, cfg: c.Server.AMQP.Plain, requireClientAuth: false},
		{name: listenerNameTLS, cfg: c.Server.AMQP.TLS, requireClientAuth: false},
		{name: listenerNameMTLS, cfg: c.Server.AMQP.MTLS, requireClientAuth: true},
	}

	for _, slot := range amqpSlots {
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && slot.name == listenerNamePlain {
				return fmt.Errorf("server.amqp.%s TLS fields are not supported for plain listeners", slot.name)
			}
			continue
		}
		hasMessagingListener = true

		if slot.cfg.MaxConnections < 0 {
			return fmt.Errorf("server.amqp.%s.max_connections cannot be negative", slot.name)
		}
		if slot.name == listenerNamePlain && tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.amqp.%s TLS fields are not supported for plain listeners", slot.name)
		}
		if slot.name != listenerNamePlain {
			if err := validateListenerTLS("server.amqp."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
		}
	}

	// AMQP 0.9.1 validation
	amqp091Slots := []struct {
		name                   string
		cfg                    AMQP091ListenerConfig
		requireClientAuth      bool
		requireExactClientAuth bool
	}{
		{name: listenerNamePlain, cfg: c.Server.AMQP091.Plain, requireClientAuth: false},
		{name: listenerNameTLS, cfg: c.Server.AMQP091.TLS, requireClientAuth: false},
		{name: listenerNameMTLS, cfg: c.Server.AMQP091.MTLS, requireClientAuth: true},
		{name: listenerNameLocal, cfg: c.Server.AMQP091.Local, requireClientAuth: true, requireExactClientAuth: true},
		{name: listenerNameInternal, cfg: c.Server.AMQP091.Internal, requireClientAuth: true, requireExactClientAuth: true},
		{name: listenerNameService, cfg: c.Server.AMQP091.Service, requireClientAuth: true, requireExactClientAuth: true},
	}

	for _, slot := range amqp091Slots {
		if !hasAddr(slot.cfg.Addr) {
			if tlsConfigured(slot.cfg.TLS) && slot.name == listenerNamePlain {
				return fmt.Errorf("server.amqp091.%s TLS fields are not supported for plain listeners", slot.name)
			}
			continue
		}
		hasMessagingListener = true

		if slot.requireExactClientAuth && slot.cfg.MaxConnections <= 0 {
			return fmt.Errorf("server.amqp091.%s.max_connections must be positive", slot.name)
		}
		if slot.cfg.MaxConnections < 0 {
			return fmt.Errorf("server.amqp091.%s.max_connections cannot be negative", slot.name)
		}
		if slot.name == listenerNamePlain && tlsConfigured(slot.cfg.TLS) {
			return fmt.Errorf("server.amqp091.%s TLS fields are not supported for plain listeners", slot.name)
		}
		if slot.name != listenerNamePlain {
			if err := validateListenerTLS("server.amqp091."+slot.name, slot.cfg.TLS, slot.requireClientAuth); err != nil {
				return err
			}
			if slot.requireExactClientAuth && strings.ToLower(strings.TrimSpace(slot.cfg.TLS.ClientAuth)) != clientAuthRequire {
				return fmt.Errorf("server.amqp091.%s.client_auth must be \"require\"", slot.name)
			}
		}
	}
	if !hasMessagingListener {
		return fmt.Errorf("at least one messaging listener must be configured")
	}

	if err := c.validateNoDuplicateBinds(); err != nil {
		return err
	}

	for proto := range c.Auth.External.Protocols {
		if !knownAuthProtocols[proto] {
			return fmt.Errorf("auth.external.protocols: unknown protocol %q (valid: mqtt, amqp, amqp091, http, coap)", proto)
		}
	}
	if err := validateCalloutTLS("auth.external", c.Auth.External.URL, c.Auth.External.TLS); err != nil {
		return err
	}
	if err := ValidateLocalPrincipals(c.Auth.LocalPrincipals); err != nil {
		return err
	}
	// Every local-principal listener authenticates against the same store, so a
	// listener under any of the keys requires the store to be populated.
	for _, listener := range c.Server.AMQP091.LocalListeners() {
		if len(c.Auth.LocalPrincipals) == 0 {
			return fmt.Errorf("auth.local_principals must contain at least one principal when server.amqp091.%s.addr is configured", listener.Name)
		}
		// An exact publish target is appended and synced on the receiving node
		// only, and is deliberately never forwarded to other nodes: forwarding
		// would acknowledge a publisher on a barrier no single node established.
		// In a cluster that makes those records unreachable from consumers
		// attached elsewhere, with nothing to signal it, so refuse the
		// combination rather than serve a principal whose records only some
		// readers can see.
		//
		// The permission decides this, not the listener, exactly as it decides
		// how a publication is routed. A prefix permission cannot name a queue,
		// so it never takes that single-node durable path and a principal
		// holding only prefix permissions may run clustered.
		//
		// A prefix publication may still be captured by a queue whose own topics
		// pattern matches it, and that append is likewise not forwarded to nodes
		// that already know the queue — remote consumers are served by the
		// delivery engine instead. That is not what this rule gates: capture
		// applies to every publisher on every protocol, so refusing a local
		// principal for it would single out the one publisher whose behavior is
		// declared in configuration. What is gated here is the durable-stream
		// path, which bypasses cluster distribution entirely by design.
		if c.Cluster.Enabled {
			if name, target, found := firstExactPublishTarget(c.Auth.LocalPrincipals); found {
				return fmt.Errorf("auth.local_principals %q grants the exact publish target %q, which cannot be combined with cluster.enabled: an exact target is durable on the receiving node only and is not forwarded to other nodes; grant permissions.publish[].routing_key_prefix instead, or run server.amqp091.%s on a single-node deployment", name, target, listener.Name)
			}
		}
	}
	for proto := range c.Hooks.Protocols {
		if !knownAuthProtocols[proto] {
			return fmt.Errorf("hooks.protocols: unknown protocol %q (valid: mqtt, amqp, amqp091, http, coap)", proto)
		}
	}
	for hook := range c.Hooks.Events {
		if !knownBlockingHooks[hook] {
			return fmt.Errorf("hooks.events: unknown hook %q (valid: auth_on_register, auth_on_publish, auth_on_subscribe, auth_on_unsubscribe)", hook)
		}
	}
	switch c.Hooks.FailMode {
	case "", "deny", "allow":
	default:
		return fmt.Errorf("hooks.fail_mode must be \"deny\" or \"allow\"")
	}

	if c.Broker.MaxMessageSize < 1024 {
		return fmt.Errorf("broker.max_message_size must be at least 1KB")
	}
	if c.Broker.RetryInterval < time.Second {
		return fmt.Errorf("broker.retry_interval must be at least 1 second")
	}

	if c.Session.MaxSessions < 1 {
		return fmt.Errorf("session.max_sessions must be at least 1")
	}
	if c.Session.MaxOfflineQueueSize < 10 {
		return fmt.Errorf("session.max_offline_queue_size must be at least 10")
	}
	if c.Session.OfflineQueuePolicy != OfflineQueuePolicyEvict && c.Session.OfflineQueuePolicy != OfflineQueuePolicyReject {
		return fmt.Errorf("session.offline_queue_policy must be %q or %q", OfflineQueuePolicyEvict, OfflineQueuePolicyReject)
	}
	if c.Session.MaxSendQueueSize < 0 {
		return fmt.Errorf("session.max_send_queue_size cannot be negative")
	}
	if c.Session.InflightOverflow != InflightOverflowBackpressure && c.Session.InflightOverflow != InflightOverflowQueue {
		return fmt.Errorf("session.inflight_overflow must be 0 (backpressure) or 1 (queue)")
	}
	if c.Session.InflightOverflow == InflightOverflowQueue && c.Session.PendingQueueSize < 1 {
		return fmt.Errorf("session.pending_queue_size must be at least 1 when inflight_overflow is queue")
	}
	if c.Broker.FanOutWorkers < 0 {
		return fmt.Errorf("broker.fan_out_workers cannot be negative")
	}

	validLevels := map[string]bool{logLevelDebug: true, logLevelInfo: true, logLevelWarn: true, logLevelError: true}
	if !validLevels[c.Log.Level] {
		return fmt.Errorf("log.level must be one of: debug, info, warn, error")
	}
	validFormats := map[string]bool{"text": true, "json": true}
	if !validFormats[c.Log.Format] {
		return fmt.Errorf("log.format must be one of: text, json")
	}

	validStorage := map[string]bool{"memory": true, storageTypeBadger: true}
	if !validStorage[c.Storage.Type] {
		return fmt.Errorf("storage.type must be one of: memory, badger")
	}

	if c.Storage.Type == storageTypeBadger && c.Storage.BadgerDir == "" {
		return fmt.Errorf("storage.badger_dir required when type is badger")
	}

	// OpenTelemetry validation (only if metrics enabled)
	if c.Server.MetricsEnabled {
		if c.Server.OtelServiceName == "" {
			return fmt.Errorf("server.otel_service_name cannot be empty when metrics enabled")
		}
		if c.Server.OtelTraceSampleRate < 0.0 || c.Server.OtelTraceSampleRate > 1.0 {
			return fmt.Errorf("server.otel_trace_sample_rate must be between 0.0 and 1.0")
		}
	}

	// Cluster validation (only if enabled)
	if c.Cluster.Enabled {
		if c.Cluster.NodeID == "" {
			return fmt.Errorf("cluster.node_id required when clustering is enabled")
		}
		if c.Cluster.Etcd.DataDir == "" {
			return fmt.Errorf("cluster.etcd.data_dir required when clustering is enabled")
		}
		if c.Cluster.Etcd.BindAddr == "" {
			return fmt.Errorf("cluster.etcd.bind_addr required when clustering is enabled")
		}
		if c.Cluster.Etcd.ClientAddr == "" {
			return fmt.Errorf("cluster.etcd.client_addr required when clustering is enabled")
		}
		if c.Cluster.Transport.BindAddr == "" {
			return fmt.Errorf("cluster.transport.bind_addr required when clustering is enabled")
		}
		if c.Cluster.Transport.RouteBatchMaxSize < 0 {
			return fmt.Errorf("cluster.transport.route_batch_max_size must be >= 0")
		}
		if c.Cluster.Transport.RouteBatchMaxDelay < 0 {
			return fmt.Errorf("cluster.transport.route_batch_max_delay must be >= 0")
		}
		if c.Cluster.Transport.RouteBatchFlushWorkers < 0 {
			return fmt.Errorf("cluster.transport.route_batch_flush_workers must be >= 0")
		}

		// Transport TLS validation
		if c.Cluster.Transport.TLSEnabled {
			if c.Cluster.Transport.TLSCertFile == "" {
				return fmt.Errorf("cluster.transport.tls_cert_file required when transport TLS is enabled")
			}
			if c.Cluster.Transport.TLSKeyFile == "" {
				return fmt.Errorf("cluster.transport.tls_key_file required when transport TLS is enabled")
			}
			if c.Cluster.Transport.TLSCAFile == "" {
				return fmt.Errorf("cluster.transport.tls_ca_file required when transport TLS is enabled")
			}
		}

		if c.Cluster.Raft.WritePolicy != "" {
			switch strings.ToLower(c.Cluster.Raft.WritePolicy) {
			case writePolicyLocal, "reject", writePolicyForward:
			default:
				return fmt.Errorf("cluster.raft.write_policy must be one of: local, reject, forward")
			}
		}
		if c.Cluster.Raft.DistributionMode != "" {
			switch strings.ToLower(c.Cluster.Raft.DistributionMode) {
			case writePolicyForward, "replicate":
			default:
				return fmt.Errorf("cluster.raft.distribution_mode must be one of: forward, replicate")
			}
		}

		if c.Cluster.Raft.Enabled {
			if strings.TrimSpace(c.Cluster.Raft.BindAddr) == "" {
				return fmt.Errorf("cluster.raft.bind_addr required when raft is enabled")
			}
			if strings.TrimSpace(c.Cluster.Raft.DataDir) == "" {
				return fmt.Errorf("cluster.raft.data_dir required when raft is enabled")
			}
			if c.Cluster.Raft.ReplicationFactor < 1 || c.Cluster.Raft.ReplicationFactor > 10 {
				return fmt.Errorf("cluster.raft.replication_factor must be between 1 and 10")
			}
			if c.Cluster.Raft.MinInSyncReplicas < 1 || c.Cluster.Raft.MinInSyncReplicas > c.Cluster.Raft.ReplicationFactor {
				return fmt.Errorf("cluster.raft.min_in_sync_replicas must be between 1 and replication_factor")
			}
			if c.Cluster.Raft.AckTimeout <= 0 {
				return fmt.Errorf("cluster.raft.ack_timeout must be > 0")
			}

			for groupID, groupCfg := range c.Cluster.Raft.Groups {
				gid := strings.TrimSpace(groupID)
				if gid == "" {
					return fmt.Errorf("cluster.raft.groups key cannot be empty")
				}

				groupEnabled := true
				if groupCfg.Enabled != nil {
					groupEnabled = *groupCfg.Enabled
				}
				if !groupEnabled {
					continue
				}

				// Non-default groups must define dedicated endpoints.
				if gid != raftGroupDefault && strings.TrimSpace(groupCfg.BindAddr) == "" {
					return fmt.Errorf("cluster.raft.groups.%s.bind_addr required for non-default group", gid)
				}
				if gid != raftGroupDefault && len(groupCfg.Peers) == 0 {
					return fmt.Errorf("cluster.raft.groups.%s.peers required for non-default group", gid)
				}

				if groupCfg.ReplicationFactor < 0 || groupCfg.ReplicationFactor > 10 {
					return fmt.Errorf("cluster.raft.groups.%s.replication_factor must be between 0 and 10", gid)
				}
				effectiveRF := c.Cluster.Raft.ReplicationFactor
				if groupCfg.ReplicationFactor > 0 {
					effectiveRF = groupCfg.ReplicationFactor
				}

				if groupCfg.MinInSyncReplicas < 0 || groupCfg.MinInSyncReplicas > effectiveRF {
					return fmt.Errorf("cluster.raft.groups.%s.min_in_sync_replicas must be between 0 and effective replication_factor", gid)
				}
				if groupCfg.AckTimeout < 0 {
					return fmt.Errorf("cluster.raft.groups.%s.ack_timeout must be >= 0", gid)
				}
			}
		}
	}

	// Webhook validation (only if enabled)
	if c.Webhook.Enabled {
		if c.Webhook.QueueSize < 100 {
			return fmt.Errorf("webhook.queue_size must be at least 100")
		}
		if c.Webhook.DropPolicy != "oldest" && c.Webhook.DropPolicy != "newest" {
			return fmt.Errorf("webhook.drop_policy must be 'oldest' or 'newest'")
		}
		if c.Webhook.Workers < 1 {
			return fmt.Errorf("webhook.workers must be at least 1")
		}
		if c.Webhook.ShutdownTimeout < time.Second {
			return fmt.Errorf("webhook.shutdown_timeout must be at least 1 second")
		}
		if c.Webhook.Defaults.Timeout < time.Second {
			return fmt.Errorf("webhook.defaults.timeout must be at least 1 second")
		}
		if c.Webhook.Defaults.Retry.MaxAttempts < 1 {
			return fmt.Errorf("webhook.defaults.retry.max_attempts must be at least 1")
		}
		if c.Webhook.Defaults.Retry.Multiplier < 1.0 {
			return fmt.Errorf("webhook.defaults.retry.multiplier must be at least 1.0")
		}
		if c.Webhook.Defaults.CircuitBreaker.FailureThreshold < 1 {
			return fmt.Errorf("webhook.defaults.circuit_breaker.failure_threshold must be at least 1")
		}

		// Validate each endpoint
		for i, endpoint := range c.Webhook.Endpoints {
			if endpoint.Name == "" {
				return fmt.Errorf("webhook.endpoints[%d].name cannot be empty", i)
			}
			if endpoint.Type != "http" {
				return fmt.Errorf("webhook.endpoints[%d].type must be 'http' (grpc not yet supported)", i)
			}
			if endpoint.URL == "" {
				return fmt.Errorf("webhook.endpoints[%d].url cannot be empty", i)
			}
		}
	}

	if c.QueueManager.AutoCommitInterval < 0 {
		return fmt.Errorf("queue_manager.auto_commit_interval must be >= 0")
	}
	if c.QueueManager.CaptureWorkers < 0 {
		return fmt.Errorf("queue_manager.capture_workers must be >= 0")
	}
	if c.QueueManager.CaptureQueueDepth < 0 {
		return fmt.Errorf("queue_manager.capture_queue_depth must be >= 0")
	}
	if c.QueueManager.CaptureDrainTimeout < 0 {
		return fmt.Errorf("queue_manager.capture_drain_timeout must be >= 0")
	}

	// Queue validation
	seenQueues := make(map[string]bool)
	for i, q := range c.Queues {
		if q.Name == "" {
			return fmt.Errorf("queues[%d].name cannot be empty", i)
		}
		if seenQueues[q.Name] {
			return fmt.Errorf("queues[%d].name '%s' is duplicated", i, q.Name)
		}
		seenQueues[q.Name] = true
		if len(q.Topics) == 0 {
			return fmt.Errorf("queues[%d].topics cannot be empty", i)
		}
		// A malformed filter is not a harmless typo: it never matches, so the
		// queue is bound to nothing and silently receives no traffic. Refuse it
		// at load rather than let it be deployed and discovered by absence.
		for j, filter := range q.Topics {
			if err := topics.ValidateTopicFilter(filter); err != nil {
				return fmt.Errorf(
					"queues[%d].topics[%d] %q is not a valid topic filter: %w; %q must be the final level and %q must occupy a whole level",
					i, j, filter, err, "#", "+")
			}
		}
		if q.Replication.Enabled {
			if q.Replication.Group != "" && strings.TrimSpace(q.Replication.Group) == "" {
				return fmt.Errorf("queues[%d].replication.group cannot be only whitespace", i)
			}

			if c.Cluster.Enabled && c.Cluster.Raft.Enabled && !c.Cluster.Raft.AutoProvisionGroups {
				groupID := strings.TrimSpace(q.Replication.Group)
				if groupID == "" {
					groupID = raftGroupDefault
				}
				if groupID != raftGroupDefault {
					if _, ok := c.Cluster.Raft.Groups[groupID]; !ok {
						return fmt.Errorf("queues[%d].replication.group '%s' is not configured under cluster.raft.groups and auto_provision_groups is disabled", i, groupID)
					}
				}
			}

			if q.Replication.ReplicationFactor < 1 || q.Replication.ReplicationFactor > 10 {
				return fmt.Errorf("queues[%d].replication.replication_factor must be between 1 and 10", i)
			}
			if q.Replication.MinInSyncReplicas < 1 || q.Replication.MinInSyncReplicas > q.Replication.ReplicationFactor {
				return fmt.Errorf("queues[%d].replication.min_in_sync_replicas must be between 1 and replication_factor", i)
			}
			switch strings.ToLower(q.Replication.Mode) {
			case queueModeSync, "async":
			default:
				return fmt.Errorf("queues[%d].replication.mode must be one of: sync, async", i)
			}
			if q.Replication.AckTimeout <= 0 {
				return fmt.Errorf("queues[%d].replication.ack_timeout must be > 0", i)
			}
		}
	}

	return nil
}

func validateListenerTLS(prefix string, cfg mqtttls.Config, requireCA bool) error {
	if cfg.CertFile == "" {
		return fmt.Errorf("%s.cert_file required", prefix)
	}
	if cfg.KeyFile == "" {
		return fmt.Errorf("%s.key_file required", prefix)
	}
	if requireCA && cfg.ClientCAFile == "" {
		return fmt.Errorf("%s.ca_file required", prefix)
	}
	return nil
}

// ValidateLocalPrincipals checks the declarative rules for local principals:
// unique non-blank names and absolute URI SANs, roles, named secret files, and
// exact-or-prefix publish permissions. It is the single definition of those
// rules, shared with the runtime store that loads the same section, so startup
// validation and a SIGHUP reload cannot drift apart.
//
// It deliberately does not open the secret files. Their contents are checked
// where the credential material is actually loaded, in broker/localauth, which
// runs at startup and on every reload. Keeping the filesystem out of this
// function is what lets `fluxmq config validate` check a production file on a
// workstation that has no /run/secrets.
func ValidateLocalPrincipals(principals []LocalPrincipalConfig) error {
	names := make(map[string]struct{}, len(principals))
	uriSANs := make(map[string]struct{}, len(principals))

	for i, principal := range principals {
		prefix := fmt.Sprintf("auth.local_principals[%d]", i)
		name := strings.TrimSpace(principal.Name)
		if name == "" {
			return fmt.Errorf("%s.name cannot be empty", prefix)
		}
		if name != principal.Name {
			return fmt.Errorf("%s.name cannot have leading or trailing whitespace", prefix)
		}
		if _, exists := names[name]; exists {
			return fmt.Errorf("%s.name %q is duplicated", prefix, name)
		}
		names[name] = struct{}{}

		if _, known := knownLocalRoles[principal.EffectiveRole()]; !known {
			return fmt.Errorf("%s.role %q is unknown (valid: %s, %s)", prefix, principal.Role, LocalRolePublisher, LocalRoleService)
		}
		if principal.EffectiveRole() == LocalRolePublisher && len(principal.Permissions.Subscribe) != 0 {
			return fmt.Errorf("%s.permissions.subscribe requires role %q; a publisher runs no consumer", prefix, LocalRoleService)
		}

		uriSAN := strings.TrimSpace(principal.CertificateURISAN)
		if uriSAN == "" {
			return fmt.Errorf("%s.certificate_uri_san cannot be empty", prefix)
		}
		if uriSAN != principal.CertificateURISAN {
			return fmt.Errorf("%s.certificate_uri_san cannot have leading or trailing whitespace", prefix)
		}
		parsedURI, err := url.Parse(uriSAN)
		if err != nil || parsedURI.Scheme == "" {
			return fmt.Errorf("%s.certificate_uri_san must be an absolute URI", prefix)
		}
		if _, exists := uriSANs[uriSAN]; exists {
			return fmt.Errorf("%s.certificate_uri_san %q is duplicated", prefix, uriSAN)
		}
		uriSANs[uriSAN] = struct{}{}

		if strings.TrimSpace(principal.CurrentSecretFile) == "" {
			return fmt.Errorf("%s.current_secret_file cannot be empty", prefix)
		}
		if principal.PreviousSecretFile != "" && strings.TrimSpace(principal.PreviousSecretFile) == "" {
			return fmt.Errorf("%s.previous_secret_file cannot be empty", prefix)
		}

		publishTargets := make(map[LocalPublishPermission]struct{}, len(principal.Permissions.Publish))
		for j, permission := range principal.Permissions.Publish {
			permissionPrefix := fmt.Sprintf("%s.permissions.publish[%d]", prefix, j)
			if permission.Exchange != "" {
				return fmt.Errorf("%s.exchange must be empty; local principals may publish only through the AMQP default exchange", permissionPrefix)
			}
			if containsWildcard(permission.Exchange) {
				return fmt.Errorf("%s.exchange must be an exact value without wildcards", permissionPrefix)
			}
			if permission.RoutingKey != "" && permission.RoutingKeyPrefix != "" {
				return fmt.Errorf("%s cannot set both routing_key and routing_key_prefix", permissionPrefix)
			}
			// A prefix is a wildcard by construction, so it must never be written
			// as one: accepting "m.#" would silently grant the literal "#" too.
			if permission.IsPrefix() {
				if containsWildcard(permission.RoutingKeyPrefix) {
					return fmt.Errorf("%s.routing_key_prefix must not contain wildcards; it already matches every routing key beneath it", permissionPrefix)
				}
				if strings.TrimSpace(permission.RoutingKeyPrefix) != permission.RoutingKeyPrefix {
					return fmt.Errorf("%s.routing_key_prefix cannot have leading or trailing whitespace", permissionPrefix)
				}
			} else {
				if permission.RoutingKey == "" {
					return fmt.Errorf("%s must set either routing_key or routing_key_prefix", permissionPrefix)
				}
				if containsWildcard(permission.RoutingKey) {
					return fmt.Errorf("%s.routing_key must be an exact value without wildcards", permissionPrefix)
				}
			}
			if _, exists := publishTargets[permission]; exists {
				return fmt.Errorf("%s duplicates an earlier publish permission", permissionPrefix)
			}
			publishTargets[permission] = struct{}{}
		}

		// Entries are deduplicated on their normalized form, so the same grant
		// written in two spellings is rejected rather than counted twice.
		subscribeQueues := make(map[string]struct{}, len(principal.Permissions.Subscribe))
		for j, queue := range principal.Permissions.Subscribe {
			permissionPrefix := fmt.Sprintf("%s.permissions.subscribe[%d]", prefix, j)
			if queue == "" {
				return fmt.Errorf("%s cannot be empty", permissionPrefix)
			}
			if strings.TrimSpace(queue) != queue {
				return fmt.Errorf("%s cannot have leading or trailing whitespace", permissionPrefix)
			}
			// The ACL names queues, and a client reaches one through the queue
			// prefix, so "$queue/m" here would name no queue and grant nothing at
			// all. Only that prefix is rejected: "$" is otherwise an ordinary
			// character and a queue may legitimately be named "$internal".
			if rest, isAddress := strings.CutPrefix(queue, localSubscribeQueuePrefix); isAddress {
				return fmt.Errorf("%s must name a queue rather than a queue address; write %q, not %q",
					permissionPrefix, strings.TrimSuffix(rest, "/#"), queue)
			}
			normalized := NormalizeLocalSubscribeEntry(queue)
			if err := validateLocalSubscribeEntry(normalized); err != nil {
				return fmt.Errorf("%s is not a valid queue pattern: %w", permissionPrefix, err)
			}
			if _, exists := subscribeQueues[normalized]; exists {
				return fmt.Errorf("%s duplicates an earlier subscribe permission", permissionPrefix)
			}
			subscribeQueues[normalized] = struct{}{}
		}
	}

	return nil
}

func containsWildcard(value string) bool {
	return strings.ContainsAny(value, "#*+")
}

// NormalizeProtocolMode normalizes and defaults MQTT listener protocol mode.
func NormalizeProtocolMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", ProtocolModeAuto:
		return ProtocolModeAuto
	case ProtocolModeV3:
		return ProtocolModeV3
	case ProtocolModeV5:
		return ProtocolModeV5
	default:
		return ProtocolModeAuto
	}
}

func validateListenerProtocol(field, mode string) error {
	switch NormalizeProtocolMode(mode) {
	case ProtocolModeAuto, ProtocolModeV3, ProtocolModeV5:
		return nil
	default:
		return fmt.Errorf("%s must be one of: auto, v3, v5", field)
	}
}

func tlsConfigured(cfg mqtttls.Config) bool {
	return cfg.CertFile != "" ||
		cfg.KeyFile != "" ||
		cfg.ServerCAFile != "" ||
		cfg.ClientCAFile != "" ||
		cfg.ClientAuth != "" ||
		cfg.MinVersion != "" ||
		len(cfg.CipherSuites) > 0 ||
		cfg.PreferServerCipherSuites != nil
}

func hasAddr(addr string) bool {
	return strings.TrimSpace(addr) != ""
}

// listenerBinding is one configured listener, named by its config path.
type listenerBinding struct {
	path string
	addr string
}

// validateListenAddress checks the shape of a "host:port" listen address. The
// host may be empty, meaning every interface, and is deliberately not resolved:
// validation has to work on a machine that cannot see the deployment's DNS, so
// ":1883", "127.0.0.1:1883", "[::1]:1883" and "broker.internal:1883" all pass.
//
// Without this, every malformed form is accepted — a port above 65535, a
// negative port, a non-numeric port, a bare "1883" with no colon — and the
// first sign of the typo is a broker that logs a bind failure and exits.
func validateListenAddress(path, address string) error {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return fmt.Errorf("%s %q is not a host:port address; use \":1883\" for every interface or \"127.0.0.1:1883\" for loopback", path, address)
	}
	if host != "" && strings.ContainsAny(host, " \t") {
		return fmt.Errorf("%s host %q contains whitespace", path, host)
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("%s port %q is not a number", path, port)
	}
	if number == 0 {
		return fmt.Errorf("%s port 0 asks the kernel for an arbitrary free port, which no client can be told about; choose a fixed port", path)
	}
	if !validPort(number) {
		return fmt.Errorf("%s port %d is out of range; ports run from 1 to 65535", path, number)
	}
	return nil
}

// validateNoDuplicateBinds checks every address the broker binds: each must be
// a well-formed host:port, and no two may race for the same socket. Without the
// second half the loser fails at startup with a bare "address already in use"
// naming no config key, and a listener the operator never declared — one left
// at its default — can silently shadow one they did.
//
// UDP and TCP are checked separately: CoAP may reuse a TCP port number.
func (c *Config) validateNoDuplicateBinds() error {
	tcp := []listenerBinding{
		{"server.mqtt.tcp.v3.addr", c.Server.MQTT.TCP.V3.Addr},
		{"server.mqtt.tcp.v5.addr", c.Server.MQTT.TCP.V5.Addr},
		{"server.mqtt.tcp.tls.addr", c.Server.MQTT.TCP.TLS.Addr},
		{"server.mqtt.tcp.mtls.addr", c.Server.MQTT.TCP.MTLS.Addr},
		{"server.mqtt.websocket.v3.addr", c.Server.MQTT.WebSocket.V3.Addr},
		{"server.mqtt.websocket.v5.addr", c.Server.MQTT.WebSocket.V5.Addr},
		{"server.mqtt.websocket.tls.addr", c.Server.MQTT.WebSocket.TLS.Addr},
		{"server.mqtt.websocket.mtls.addr", c.Server.MQTT.WebSocket.MTLS.Addr},
		{"server.http.plain.addr", c.Server.HTTP.Plain.Addr},
		{"server.http.tls.addr", c.Server.HTTP.TLS.Addr},
		{"server.http.mtls.addr", c.Server.HTTP.MTLS.Addr},
		{"server.amqp.plain.addr", c.Server.AMQP.Plain.Addr},
		{"server.amqp.tls.addr", c.Server.AMQP.TLS.Addr},
		{"server.amqp.mtls.addr", c.Server.AMQP.MTLS.Addr},
		{"server.amqp091.plain.addr", c.Server.AMQP091.Plain.Addr},
		{"server.amqp091.tls.addr", c.Server.AMQP091.TLS.Addr},
		{"server.amqp091.mtls.addr", c.Server.AMQP091.MTLS.Addr},
		{"server.amqp091.local.addr", c.Server.AMQP091.Local.Addr},
		{"server.admin_api_addr", c.Server.AdminAPIAddr},
		{"server.health_addr", c.Server.HealthAddr},
	}
	udp := []listenerBinding{
		{"server.coap.plain.addr", c.Server.CoAP.Plain.Addr},
		{"server.coap.dtls.addr", c.Server.CoAP.DTLS.Addr},
		{"server.coap.mdtls.addr", c.Server.CoAP.MDTLS.Addr},
	}

	for _, set := range []struct {
		network  string
		bindings []listenerBinding
	}{{"tcp", tcp}, {"udp", udp}} {
		if err := checkBindConflicts(set.network, set.bindings); err != nil {
			return err
		}
	}
	return nil
}

func checkBindConflicts(network string, bindings []listenerBinding) error {
	active := make([]listenerBinding, 0, len(bindings))
	for _, b := range bindings {
		if !hasAddr(b.addr) {
			continue
		}
		if err := validateListenAddress(b.path, b.addr); err != nil {
			return err
		}
		active = append(active, b)
	}

	for i, a := range active {
		for _, b := range active[i+1:] {
			if bindsConflict(a.addr, b.addr) {
				return fmt.Errorf(
					"%s and %s both listen on %s (%s); give each listener its own address, or set one to \"\" to disable it",
					a.path, b.path, b.addr, network)
			}
		}
	}
	return nil
}

// bindsConflict reports whether two listen addresses would contend for the
// same socket. Ports must match; hosts conflict when they are equal or when
// either side is a wildcard, since a wildcard bind covers every interface.
func bindsConflict(a, b string) bool {
	hostA, portA, okA := splitListenAddr(a)
	hostB, portB, okB := splitListenAddr(b)
	if !okA || !okB || portA != portB {
		return false
	}
	if isWildcardHost(hostA) || isWildcardHost(hostB) {
		return true
	}
	return strings.EqualFold(hostA, hostB)
}

func splitListenAddr(addr string) (host, port string, ok bool) {
	addr = strings.TrimSpace(addr)
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return "", "", false
	}
	host = strings.Trim(addr[:idx], "[]")
	port = addr[idx+1:]
	if port == "" {
		return "", "", false
	}
	return host, port, true
}

func validPort(port int) bool {
	return port > 0 && port <= 65535
}

func isWildcardHost(host string) bool {
	switch strings.TrimSpace(host) {
	case "", "0.0.0.0", "::", "*":
		return true
	default:
		return false
	}
}

// Save writes the configuration to a YAML file.
func (c *Config) Save(filename string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(filename, data, 0o644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}
