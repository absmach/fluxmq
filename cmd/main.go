// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/absmach/fluxmq"
	amqpbroker "github.com/absmach/fluxmq/amqp/broker"
	amqp1broker "github.com/absmach/fluxmq/amqp1/broker"
	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/broker/authcallout"
	"github.com/absmach/fluxmq/broker/hook"
	"github.com/absmach/fluxmq/broker/localauth"
	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/broker/webhook"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/internal/httpclient"
	"github.com/absmach/fluxmq/internal/wiring"
	logStorage "github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/message"
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/broker"
	mqtttls "github.com/absmach/fluxmq/pkg/tls"
	"github.com/absmach/fluxmq/queue"
	qraft "github.com/absmach/fluxmq/queue/raft"
	queueStorage "github.com/absmach/fluxmq/queue/storage"
	queueTypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/ratelimit"
	"github.com/absmach/fluxmq/reload"
	amqpserver "github.com/absmach/fluxmq/server/amqp"
	amqp1server "github.com/absmach/fluxmq/server/amqp1"
	"github.com/absmach/fluxmq/server/api"
	"github.com/absmach/fluxmq/server/coap"
	"github.com/absmach/fluxmq/server/health"
	"github.com/absmach/fluxmq/server/http"
	"github.com/absmach/fluxmq/server/otel"
	"github.com/absmach/fluxmq/server/tcp"
	"github.com/absmach/fluxmq/server/websocket"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/badger"
	"github.com/absmach/fluxmq/storage/memory"
	piondtls "github.com/pion/dtls/v3"
	oteltrace "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Listener mode names.
const (
	listenerPlain = "plain"
	listenerTLS   = "tls"
	listenerMTLS  = "mtls"
)

// valueOr returns what the operator wrote, or the built-in default when the
// key was omitted. Configuration keeps absent and zero distinct so a written
// value is never silently replaced by a different one.
func valueOr[T any](configured *T, fallback T) T {
	if configured == nil {
		return fallback
	}
	return *configured
}

func protocolVersionForMode(mode string) int {
	switch config.NormalizeProtocolMode(mode) {
	case config.ProtocolModeV3:
		return core.ProtocolV3
	case config.ProtocolModeV5:
		return core.ProtocolV5
	default:
		return core.ProtocolAuto
	}
}

// mqttPacketHeadroom is added to the configured maximum message size to derive
// the maximum accepted MQTT packet size. broker.max_message_size bounds the
// application payload, while an MQTT packet also carries the topic name (up to
// 64 KiB), a packet identifier, and — for v5 — properties, so a payload of
// exactly max_message_size must still fit.
const mqttPacketHeadroom = 64 * 1024

// maxMQTTRemainingLength is the protocol ceiling on an MQTT packet's remaining
// length, imposed by its 4-byte variable byte integer encoding.
const maxMQTTRemainingLength = 268435455

// maxMQTTPacketSize converts the configured maximum message size into the
// remaining-length limit enforced by the MQTT decoders. A non-positive size, or
// one already at the protocol ceiling, leaves packets unbounded.
func maxMQTTPacketSize(maxMessageSize int) int {
	if maxMessageSize <= 0 || maxMessageSize >= maxMQTTRemainingLength-mqttPacketHeadroom {
		return 0
	}
	return maxMessageSize + mqttPacketHeadroom
}

type brokerDeliveryTarget struct {
	mqtt    *broker.Broker
	amqp    *amqp1broker.Broker
	amqp091 *amqpbroker.Broker
}

type localAMQPPolicy struct {
	store *localauth.Store
}

func (p *localAMQPPolicy) AuthenticateLocal(_ context.Context, _ string, username, secret string, peer amqpbroker.VerifiedPeerIdentity) (amqpbroker.LocalAuthentication, bool, error) {
	if p == nil || p.store == nil {
		return amqpbroker.LocalAuthentication{}, false, nil
	}
	for _, uriSAN := range peer.URISANs {
		authentication, ok := p.store.Authenticate(username, secret, uriSAN)
		if !ok {
			continue
		}
		return amqpbroker.LocalAuthentication{
			PrincipalID:            authentication.Principal,
			Role:                   localPrincipalRole(authentication.Role),
			CredentialFingerprint:  hex.EncodeToString(authentication.CredentialFingerprint[:]),
			PermissionsFingerprint: hex.EncodeToString(authentication.PermissionsFingerprint[:]),
			CertificateURI:         authentication.CertificateURISAN,
		}, true, nil
	}
	return amqpbroker.LocalAuthentication{}, false, nil
}

// localPrincipalRole maps a configured role name onto the broker capability.
// An unrecognized name falls back to the least privileged role rather than
// failing open; configuration validation rejects such names at load.
func localPrincipalRole(role string) amqpbroker.LocalPrincipalRole {
	if role == config.LocalRoleService {
		return amqpbroker.LocalRoleService
	}
	return amqpbroker.LocalRolePublisher
}

func (p *localAMQPPolicy) CanPublishLocal(identity amqpbroker.LocalSessionIdentity, exchange, routingKey string) amqpbroker.LocalPublishGrant {
	authentication, ok := localAuthentication(identity)
	if !ok || p == nil || p.store == nil {
		return amqpbroker.LocalPublishGrantNone
	}
	switch p.store.AuthorizePublish(authentication, exchange, routingKey) {
	case localauth.PublishGrantExactTarget:
		return amqpbroker.LocalPublishGrantExactTarget
	case localauth.PublishGrantPrefix:
		return amqpbroker.LocalPublishGrantPrefix
	default:
		return amqpbroker.LocalPublishGrantNone
	}
}

func (p *localAMQPPolicy) CanSubscribeLocal(identity amqpbroker.LocalSessionIdentity, queue string) bool {
	authentication, ok := localAuthentication(identity)
	return ok && p != nil && p.store != nil && p.store.CanSubscribeAuthenticated(authentication, queue)
}

func (p *localAMQPPolicy) IsSessionActive(identity amqpbroker.LocalSessionIdentity) bool {
	if p == nil || p.store == nil {
		return false
	}
	authentication, ok := localAuthentication(identity)
	return ok && p.store.IsActive(authentication)
}

func localAuthentication(identity amqpbroker.LocalSessionIdentity) (localauth.Authentication, bool) {
	rawCredentialFingerprint, err := hex.DecodeString(identity.CredentialFingerprint)
	if err != nil || len(rawCredentialFingerprint) != len(localauth.CredentialFingerprint{}) {
		return localauth.Authentication{}, false
	}
	rawPermissionsFingerprint, err := hex.DecodeString(identity.PermissionsFingerprint)
	if err != nil || len(rawPermissionsFingerprint) != len(localauth.PermissionsFingerprint{}) {
		return localauth.Authentication{}, false
	}
	var credentialFingerprint localauth.CredentialFingerprint
	copy(credentialFingerprint[:], rawCredentialFingerprint)
	var permissionsFingerprint localauth.PermissionsFingerprint
	copy(permissionsFingerprint[:], rawPermissionsFingerprint)
	return localauth.Authentication{
		Principal:              identity.PrincipalID,
		CertificateURISAN:      identity.CertificateURI,
		Role:                   identity.Role.String(),
		CredentialFingerprint:  credentialFingerprint,
		PermissionsFingerprint: permissionsFingerprint,
	}, true
}

func reloadLocalPrincipals(
	ctx context.Context,
	store *localauth.Store,
	principals []config.LocalPrincipalConfig,
	configuredQueues []queueTypes.QueueConfig,
	queueManager *queue.Manager,
) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("local principal store is not configured")
	}
	if queueManager == nil {
		return false, fmt.Errorf("local principal queue manager is not configured")
	}
	contracts, err := localPrincipalPublishTargetContracts(principals, configuredQueues)
	if err != nil {
		return false, err
	}
	if err := validateLocalPrincipalPublishTargets(ctx, principals, configuredQueues, queueManager.QueueStore()); err != nil {
		return false, err
	}

	previousContracts := queueManager.ProtectedQueueContracts()
	unionContracts := mergeQueueContracts(previousContracts, contracts)
	if err := queueManager.ReplaceProtectedQueueContracts(ctx, unionContracts); err != nil {
		return false, err
	}
	changed, err := store.Reload(principals)
	if err != nil {
		if restoreErr := queueManager.ReplaceProtectedQueueContracts(ctx, previousContracts); restoreErr != nil {
			return false, fmt.Errorf("reload local principals: %v; restore protected queue contracts: %w", err, restoreErr)
		}
		return false, err
	}
	if err := queueManager.NarrowProtectedQueueContracts(contracts); err != nil {
		return false, fmt.Errorf("finalize protected queue contracts after local-principal reload: %w", err)
	}
	return changed, nil
}

func mergeQueueContracts(first, second []queueTypes.QueueConfig) []queueTypes.QueueConfig {
	byName := make(map[string]queueTypes.QueueConfig, len(first)+len(second))
	for _, contract := range first {
		byName[contract.Name] = contract
	}
	for _, contract := range second {
		byName[contract.Name] = contract
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	contracts := make([]queueTypes.QueueConfig, 0, len(names))
	for _, name := range names {
		contracts = append(contracts, byName[name])
	}
	return contracts
}

// validateLocalPrincipalPublishTargets verifies the persisted queue topology
// after the queue manager has created its reserved queues. Local publishers do
// not have topology permissions, so FluxMQ must fail startup instead of serving
// a stale or unsafe target definition.
func validateLocalPrincipalPublishTargets(
	ctx context.Context,
	principals []config.LocalPrincipalConfig,
	configuredQueues []queueTypes.QueueConfig,
	queueStore queueStorage.QueueStore,
) error {
	contracts, err := localPrincipalPublishTargetContracts(principals, configuredQueues)
	if err != nil {
		return err
	}
	if len(contracts) == 0 {
		return nil
	}
	if queueStore == nil {
		return fmt.Errorf("local principal publish targets require a queue store")
	}
	durableStore, ok := queueStore.(queueStorage.DurableQueueStore)
	if !ok || !durableStore.SupportsDurableSync() {
		return fmt.Errorf("local principal publish targets require a queue store with durable sync support")
	}

	for _, expected := range contracts {
		persisted, err := queueStore.GetQueue(ctx, expected.Name)
		if err != nil {
			return fmt.Errorf("load persisted local principal publish target %q: %w", expected.Name, err)
		}
		if persisted == nil {
			return fmt.Errorf("load persisted local principal publish target %q: queue not found", expected.Name)
		}
		if err := queue.ValidateProtectedQueueContract(expected, *persisted); err != nil {
			return err
		}
	}
	return nil
}

func localPrincipalPublishTargetContracts(principals []config.LocalPrincipalConfig, configuredQueues []queueTypes.QueueConfig) ([]queueTypes.QueueConfig, error) {
	targets := make(map[string]struct{})
	for _, principal := range principals {
		for _, permission := range principal.Permissions.Publish {
			// A prefix permission authorizes topic publishing, not an append to
			// one durable stream, so there is no single queue to contract with.
			if permission.IsPrefix() {
				continue
			}
			targets[permission.RoutingKey] = struct{}{}
		}
	}

	configuredByName := make(map[string]queueTypes.QueueConfig, len(configuredQueues))
	for _, queueConfig := range configuredQueues {
		configuredByName[queueConfig.Name] = queueConfig
	}
	targetNames := make([]string, 0, len(targets))
	for target := range targets {
		targetNames = append(targetNames, target)
	}
	sort.Strings(targetNames)

	contracts := make([]queueTypes.QueueConfig, 0, len(targetNames))
	for _, target := range targetNames {
		contract, ok := configuredByName[target]
		if !ok {
			return nil, fmt.Errorf("local principal publish target %q has no matching queues entry", target)
		}
		contracts = append(contracts, contract)
	}
	return contracts, nil
}

func (t *brokerDeliveryTarget) Deliver(ctx context.Context, clientID string, msg *message.Envelope) error {
	if amqp1broker.IsAMQPClient(clientID) {
		return t.amqp.DeliverToClient(ctx, clientID, msg)
	}
	if amqpbroker.IsAMQP091Client(clientID) {
		return t.amqp091.DeliverToClient(ctx, clientID, msg)
	}
	return t.mqtt.DeliverToSessionByID(ctx, clientID, msg)
}

func (t *brokerDeliveryTarget) HasDeliveryTarget(clientID string) bool {
	if amqp1broker.IsAMQPClient(clientID) {
		return t.amqp != nil && t.amqp.IsClientConnected(clientID)
	}
	if amqpbroker.IsAMQP091Client(clientID) {
		return t.amqp091 != nil && t.amqp091.IsClientConnected(clientID)
	}
	return t.mqtt != nil && t.mqtt.Get(clientID) != nil
}

// releaseShutdownResources releases resources shared with the queue manager in
// dependency order. The broker has already called Manager.Stop before this
// runs. If capture did not quiesce, every dependency is deliberately left open:
// an in-flight worker may still be appending locally or forwarding through the
// cluster, and the cluster itself owns references into the broker store.
func releaseShutdownResources(
	shutdownComplete bool,
	stopCluster func() error,
	closeQueueLogStore func() error,
	closeBrokerStore func() error,
	logger *slog.Logger,
) {
	if logger == nil {
		logger = slog.Default()
	}
	if !shutdownComplete {
		logger.Error("queue resources left open: capture workers are still running",
			"cluster", "not stopped",
			"queue_log_store", "not closed",
			"broker_store", "not closed")
		return
	}

	// Stop ingress before closing either store: cluster RPC handlers call the
	// queue manager and the cluster's hybrid stores use the broker store.
	if stopCluster != nil {
		if err := stopCluster(); err != nil {
			logger.Error("Failed to stop cluster; dependent stores left open", "error", err)
			return
		}
	}
	if closeQueueLogStore != nil {
		if err := closeQueueLogStore(); err != nil {
			logger.Error("Failed to close queue log storage", "error", err)
		}
	}
	if closeBrokerStore != nil {
		if err := closeBrokerStore(); err != nil {
			logger.Error("Failed to close broker storage", "error", err)
		}
	}
}

func main() {
	configFile := flag.String("config", "", "Path to configuration file")
	configOptional := flag.Bool("config-optional", false,
		"Fall back to built-in defaults when --config names a file that does not exist")
	flag.Parse()

	load := config.Load
	if *configOptional {
		load = config.LoadOptional
	}

	cfg, err := load(*configFile)
	if err != nil {
		if errors.Is(err, config.ErrConfigNotFound) {
			slog.Error("Configuration file not found",
				"path", *configFile,
				"hint", "check the path, or pass --config-optional to start with built-in defaults")
			os.Exit(1)
		}
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	logLevel := slog.LevelInfo
	switch cfg.Log.Level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	}

	var handler slog.Handler
	if cfg.Log.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	}
	nodeID := strings.TrimSpace(cfg.Cluster.NodeID)
	if nodeID == "" {
		nodeID = "single-node"
	}

	logger := slog.New(handler).With("local_node_id", nodeID)
	slog.SetDefault(logger)

	localPrincipalStore, err := localauth.New(cfg.Auth.LocalPrincipals)
	if err != nil {
		slog.Error("Failed to initialize local principals", "error", err)
		os.Exit(1)
	}
	localPolicyAdapter := &localAMQPPolicy{store: localPrincipalStore}

	// Loaded once, and before anything registers a deferred close: every
	// protocol's callout client shares the certificate, and a bad path is a
	// configuration error that must stop the process rather than surface later
	// as a callout failure that trips the circuit breaker.
	var calloutTLS *tls.Config
	if cfg.Auth.External.URL != "" {
		calloutTLS, err = mqtttls.LoadClientTLSConfig(cfg.Auth.External.TLS)
		if err != nil {
			slog.Error("Failed to load auth callout TLS configuration", "error", err)
			os.Exit(1)
		}
	}

	slog.Info("Starting MQTT broker", "version", fluxmq.Version)
	slog.Info("Configuration loaded",
		"tcp_v3_listener", cfg.Server.MQTT.TCP.V3.Addr,
		"tcp_v5_listener", cfg.Server.MQTT.TCP.V5.Addr,
		"tcp_tls_listener", cfg.Server.MQTT.TCP.TLS.Addr,
		"tcp_mtls_listener", cfg.Server.MQTT.TCP.MTLS.Addr,
		"ws_v3_listener", cfg.Server.MQTT.WebSocket.V3.Addr,
		"ws_v5_listener", cfg.Server.MQTT.WebSocket.V5.Addr,
		"ws_tls_listener", cfg.Server.MQTT.WebSocket.TLS.Addr,
		"ws_mtls_listener", cfg.Server.MQTT.WebSocket.MTLS.Addr,
		"http_plain_listener", cfg.Server.HTTP.Plain.Addr,
		"http_tls_listener", cfg.Server.HTTP.TLS.Addr,
		"http_mtls_listener", cfg.Server.HTTP.MTLS.Addr,
		"coap_plain_listener", cfg.Server.CoAP.Plain.Addr,
		"coap_dtls_listener", cfg.Server.CoAP.DTLS.Addr,
		"coap_mdtls_listener", cfg.Server.CoAP.MDTLS.Addr,
		"amqp_plain_listener", cfg.Server.AMQP.Plain.Addr,
		"amqp_tls_listener", cfg.Server.AMQP.TLS.Addr,
		"amqp_mtls_listener", cfg.Server.AMQP.MTLS.Addr,
		"amqp091_plain_listener", cfg.Server.AMQP091.Plain.Addr,
		"amqp091_tls_listener", cfg.Server.AMQP091.TLS.Addr,
		"amqp091_mtls_listener", cfg.Server.AMQP091.MTLS.Addr,
		"amqp091_local_listener", cfg.Server.AMQP091.Local.Addr,
		"amqp091_internal_listener", cfg.Server.AMQP091.Internal.Addr,
		"amqp091_service_listener", cfg.Server.AMQP091.Service.Addr,
		"admin_api_addr", cfg.Server.AdminAPIAddr,
		"health_enabled", cfg.Server.HealthEnabled,
		"cluster_enabled", cfg.Cluster.Enabled,
		"log_level", cfg.Log.Level)

	var (
		store            storage.Store
		closeBrokerStore func() error
	)
	switch cfg.Storage.Type {
	case "memory":
		store = memory.New()
		slog.Info("Using in-memory storage")
	case "badger":
		badgerStore, err := badger.New(badger.Config{
			Dir:        cfg.Storage.BadgerDir,
			SyncWrites: cfg.Storage.BadgerSyncWrites,
		})
		if err != nil {
			slog.Error("Failed to initialize BadgerDB storage", "error", err)
			os.Exit(1)
		}
		store = badgerStore
		closeBrokerStore = store.Close
		slog.Info("Using BadgerDB persistent storage", "dir", cfg.Storage.BadgerDir)
	default:
		slog.Error("Unknown storage type", "type", cfg.Storage.Type)
		os.Exit(1) //nolint:gocritic // exitAfterDefer: defer is in a different branch
	}

	var cl cluster.Cluster
	var etcdCluster *cluster.EtcdCluster
	var clusterTLS *cluster.TransportTLSConfig

	// Declared here rather than beside their construction because the deferred
	// teardown that releases them has to be registered before b.Close, which is
	// what stops the queue manager they belong to.
	var (
		qm            *queue.Manager
		queueLogStore *logStorage.Adapter
		stopCluster   func() error
	)
	if cfg.Cluster.Enabled {
		// Build transport TLS config if enabled
		if cfg.Cluster.Transport.TLSEnabled {
			clusterTLS = &cluster.TransportTLSConfig{
				CertFile: cfg.Cluster.Transport.TLSCertFile,
				KeyFile:  cfg.Cluster.Transport.TLSKeyFile,
				CAFile:   cfg.Cluster.Transport.TLSCAFile,
			}
		}

		etcdCfg := &cluster.EtcdConfig{
			NodeID:                      cfg.Cluster.NodeID,
			DataDir:                     cfg.Cluster.Etcd.DataDir,
			BindAddr:                    cfg.Cluster.Etcd.BindAddr,
			ClientAddr:                  cfg.Cluster.Etcd.ClientAddr,
			AdvertiseAddr:               cfg.Cluster.Etcd.BindAddr, // Use bind addr as advertise for now
			InitialCluster:              cfg.Cluster.Etcd.InitialCluster,
			Bootstrap:                   cfg.Cluster.Etcd.Bootstrap,
			AllowInsecure:               cfg.Cluster.AllowInsecure,
			TransportAddr:               cfg.Cluster.Transport.BindAddr,
			PeerTransports:              cfg.Cluster.Transport.Peers,
			HybridRetainedSizeThreshold: cfg.Cluster.Etcd.HybridRetainedSizeThreshold,
			RouteBatchMaxSize:           cfg.Cluster.Transport.RouteBatchMaxSize,
			RouteBatchMaxDelay:          cfg.Cluster.Transport.RouteBatchMaxDelay,
			RouteBatchFlushWorkers:      cfg.Cluster.Transport.RouteBatchFlushWorkers,
			TransportTLS:                clusterTLS,
		}

		ec, err := cluster.NewEtcdCluster(etcdCfg, store, logger)
		if err != nil {
			slog.Error("Failed to initialize etcd cluster", "error", err)
			os.Exit(1)
		}
		etcdCluster = ec
		cl = etcdCluster
		// Stopped by the gated teardown rather than deferred here: a capture
		// worker that outlives the queue manager may still be forwarding
		// through this cluster.
		stopCluster = cl.Stop

		if err := cl.Start(); err != nil {
			slog.Error("Failed to start cluster", "error", err)
			os.Exit(1)
		}

		slog.Info("Running in cluster mode",
			"node_id", cfg.Cluster.NodeID,
			"etcd_data_dir", cfg.Cluster.Etcd.DataDir,
			"etcd_bind", cfg.Cluster.Etcd.BindAddr)
	} else {
		cl = cluster.NewNoopCluster(cfg.Cluster.NodeID)
		slog.Info("Running in single-node mode", "node_id", cfg.Cluster.NodeID)
	}

	webhookNotifier, err := webhook.NewAtomicNotifier(cfg.Webhook, cfg.Cluster.NodeID, webhook.NewHTTPSender(), logger)
	if err != nil {
		slog.Error("Failed to initialize webhooks", "error", err)
		os.Exit(1)
	}
	var webhooks corebroker.Notifier = webhookNotifier
	if cfg.Webhook.Enabled {
		slog.Info("Webhooks enabled",
			"type", "http",
			"endpoints", len(cfg.Webhook.Endpoints),
			"workers", cfg.Webhook.Workers,
			"queue_size", cfg.Webhook.QueueSize)
	} else {
		slog.Info("Webhooks disabled")
	}

	var otelShutdown func(context.Context) error
	var metrics *otel.Metrics
	var tracer trace.Tracer

	if cfg.Server.MetricsEnabled {
		shutdown, err := otel.InitProvider(cfg.Server, cfg.Cluster.NodeID)
		if err != nil {
			slog.Error("Failed to initialize OpenTelemetry", "error", err)
			os.Exit(1)
		}
		otelShutdown = shutdown
		slog.Info("OpenTelemetry initialized", "endpoint", cfg.Server.MetricsAddr)

		if cfg.Server.OtelMetricsEnabled {
			m, err := otel.NewMetrics()
			if err != nil {
				slog.Error("Failed to create metrics", "error", err)
				os.Exit(1)
			}
			metrics = m
			slog.Info("OTel metrics enabled")
		}

		if cfg.Server.OtelTracesEnabled {
			tracer = oteltrace.Tracer("mqtt-broker")
			slog.Info("Distributed tracing enabled", "sample_rate", cfg.Server.OtelTraceSampleRate)
		} else {
			slog.Info("Distributed tracing disabled (zero overhead)")
		}
	} else {
		slog.Info("OpenTelemetry disabled")
	}

	stats := broker.NewStats()
	b := broker.NewBroker(
		store, cl,
		broker.WithLogger(logger),
		broker.WithStats(stats),
		broker.WithWebhooks(webhooks),
		broker.WithMetrics(metrics),
		broker.WithTracer(tracer),
		broker.WithSessionConfig(cfg.Session),
		broker.WithTransportConfig(cfg.Cluster.Transport),
		broker.WithBrokerConfig(cfg.Broker),
	)
	// Registered before b.Close so that it runs after it: defers are LIFO, and
	// b.Close is what stops the queue manager. Reading the shutdown state any
	// earlier would always see a manager that has not stopped yet.
	//
	// A capture worker can outlive Manager.Stop when an append will not return —
	// the queue store takes no context, so the wait is bounded rather than
	// indefinite. Anything that worker still uses must then be left alone:
	// closing the store underneath it corrupts a segment it holds, and stopping
	// the cluster underneath it pulls out the transport its forward is using.
	// Leaking their dependencies into process exit is the cheaper failure.
	defer func() {
		var closeQueueLogStore func() error
		if queueLogStore != nil {
			closeQueueLogStore = queueLogStore.Close
		}
		shutdownComplete := qm == nil || qm.ShutdownComplete()
		releaseShutdownResources(
			shutdownComplete,
			stopCluster,
			closeQueueLogStore,
			closeBrokerStore,
			logger,
		)
	}()

	defer b.Close()

	// Configure maximum QoS level
	if cfg.Broker.MaxQoS >= 0 && cfg.Broker.MaxQoS <= 2 {
		b.SetMaxQoS(byte(cfg.Broker.MaxQoS))
	}

	// Initialize rate limiting with AtomicManager for hot-reload support.
	rlConfig := ratelimit.Config{
		Enabled: cfg.RateLimit.Enabled,
		Connection: ratelimit.ConnectionConfig{
			Enabled:         cfg.RateLimit.Connection.Enabled,
			Rate:            cfg.RateLimit.Connection.Rate,
			Burst:           cfg.RateLimit.Connection.Burst,
			CleanupInterval: cfg.RateLimit.Connection.CleanupInterval,
		},
		Message: ratelimit.MessageConfig{
			Enabled: cfg.RateLimit.Message.Enabled,
			Rate:    cfg.RateLimit.Message.Rate,
			Burst:   cfg.RateLimit.Message.Burst,
		},
		Subscribe: ratelimit.SubscribeConfig{
			Enabled: cfg.RateLimit.Subscribe.Enabled,
			Rate:    cfg.RateLimit.Subscribe.Rate,
			Burst:   cfg.RateLimit.Subscribe.Burst,
		},
	}
	rateLimitManager := ratelimit.NewAtomicManager(ratelimit.NewManager(rlConfig))
	defer rateLimitManager.Stop()

	b.SetClientRateLimiter(rateLimitManager)

	if cfg.RateLimit.Enabled {
		slog.Info("Rate limiting enabled",
			slog.Bool("connection", cfg.RateLimit.Connection.Enabled),
			slog.Bool("message", cfg.RateLimit.Message.Enabled),
			slog.Bool("subscribe", cfg.RateLimit.Subscribe.Enabled))
	} else {
		slog.Info("Rate limiting disabled")
	}

	// Create AMQP broker (needs queue manager set later)
	amqpStats := amqp1broker.NewStats()
	amqpBroker := amqp1broker.New(nil, amqpStats, logger)
	defer amqpBroker.Close()

	// Create AMQP 0.9.1 broker (needs queue manager set later)
	amqp091Broker := amqpbroker.New(nil, logger)
	defer amqp091Broker.Close()
	var (
		amqp091ExternalAuth  *corebroker.AuthEngine
		amqp091ExternalHooks *corebroker.BlockingHookEngine
	)

	// Configure auth callout
	if cfg.Auth.External.URL != "" {
		transport := cfg.Auth.External.Transport
		if transport == "" {
			transport = "grpc"
		}

		cb := authcallout.DefaultCircuitBreaker(logger)
		sharedOpts := []authcallout.Option{
			authcallout.WithTimeout(cfg.Auth.External.Timeout),
			authcallout.WithLogger(logger),
			authcallout.WithCircuitBreaker(cb),
		}

		newClient := func(proto authcallout.Protocol) (corebroker.Authenticator, corebroker.Authorizer) {
			opts := append(sharedOpts, authcallout.WithProtocol(proto))
			switch transport {
			case "http":
				c := authcallout.NewHTTPClient(
					httpclient.WithTLS(calloutTLS), cfg.Auth.External.URL, opts...,
				)
				return c, c
			default:
				c := authcallout.NewGRPCClient(
					httpclient.GRPCWithTLS(cfg.Auth.External.URL, calloutTLS),
					cfg.Auth.External.URL, opts...,
				)
				return c, c
			}
		}

		// An omitted key takes the built-in default; a written one is used as
		// written. Validation already refused a written zero.
		cacheSize := valueOr(cfg.Auth.External.IdentityCacheSize, corebroker.DefaultIdentityCacheSize)
		cacheTTL := valueOr(cfg.Auth.External.IdentityCacheTTL, corebroker.DefaultIdentityCacheTTL)
		engineOpts := []corebroker.AuthEngineOption{
			corebroker.WithIdentityCache(cacheSize, cacheTTL),
		}

		if cfg.Auth.External.EnabledFor("mqtt") {
			mqttAuthn, mqttAuthz := newClient(authcallout.ProtocolMQTT)
			b.SetAuthEngine(corebroker.NewAuthEngine(mqttAuthn, mqttAuthz, engineOpts...))
			slog.Info("Auth callout enabled for mqtt")
		}

		if cfg.Auth.External.EnabledFor("amqp") {
			amqpAuthn, amqpAuthz := newClient(authcallout.ProtocolAMQP10)
			amqpBroker.SetAuthEngine(corebroker.NewAuthEngine(amqpAuthn, amqpAuthz, engineOpts...))
			slog.Info("Auth callout enabled for amqp")
		}

		if cfg.Auth.External.EnabledFor("amqp091") {
			amqp091Authn, amqp091Authz := newClient(authcallout.ProtocolAMQP091)
			amqp091ExternalAuth = corebroker.NewAuthEngine(amqp091Authn, amqp091Authz, engineOpts...)
			amqp091Broker.SetAuthEngine(amqp091ExternalAuth)
			slog.Info("Auth callout enabled for amqp091")
		}

		slog.Info("Auth callout configured",
			"url", cfg.Auth.External.URL,
			"transport", transport,
			"timeout", cfg.Auth.External.Timeout,
			"protocols", cfg.Auth.External.Protocols)
	} else {
		slog.Info("Auth callout disabled")
	}

	// Configure optional blocking hook callout.
	if cfg.Hooks.URL != "" {
		transport := cfg.Hooks.Transport
		if transport == "" {
			transport = "grpc"
		}

		newHookProvider := func() corebroker.BlockingHookProvider {
			opts := []hook.Option{
				hook.WithTimeout(valueOr(cfg.Hooks.Timeout, 0)),
				hook.WithLogger(logger),
			}
			switch transport {
			case "http":
				return hook.NewHTTPClient(nil, cfg.Hooks.URL, opts...)
			default:
				return hook.NewGRPCClient(nil, cfg.Hooks.URL, opts...)
			}
		}
		newEngine := func() *corebroker.BlockingHookEngine {
			return corebroker.NewBlockingHookEngine(newHookProvider(), cfg.Hooks.FailMode, logger, cfg.Hooks.Protocols, cfg.Hooks.Events)
		}

		b.SetBlockingHooks(newEngine())
		amqpBroker.SetBlockingHooks(newEngine())
		amqp091ExternalHooks = newEngine()
		amqp091Broker.SetBlockingHooks(amqp091ExternalHooks)
		slog.Info("Blocking hooks configured",
			"url", cfg.Hooks.URL,
			"transport", transport,
			"timeout", valueOr(cfg.Hooks.Timeout, 0),
			"fail_mode", cfg.Hooks.FailMode,
			"protocols", cfg.Hooks.Protocols,
			"events", cfg.Hooks.Events)
	} else {
		slog.Info("Blocking hooks disabled")
	}

	// Shared local pub/sub router (MQTT + AMQP 0.9.1 + AMQP 1.0).
	sharedRouter := router.NewRouter()
	b.SetRouter(sharedRouter)
	amqp091Broker.SetRouter(sharedRouter)
	amqpBroker.SetRouter(sharedRouter)

	// qm and queueLogStore are declared with the deferred teardown above.
	var configuredQueueContracts []queueTypes.QueueConfig

	if metrics != nil {
		amqpMetrics, err := amqp1broker.NewMetrics()
		if err != nil {
			slog.Error("Failed to create AMQP metrics", "error", err)
			os.Exit(1) //nolint:gocritic // exitAfterDefer: fatal initialization errors terminate immediately
		}
		amqpBroker.SetMetrics(amqpMetrics)
		slog.Info("AMQP OTel metrics enabled")
	}

	// Initialize file-based log storage for queues
	{
		queueDir := cfg.Storage.BadgerDir
		if !strings.HasSuffix(queueDir, "/") {
			queueDir += "/"
		}
		queueDir += "queue"

		// Use file-based AOL storage (implements both LogStore and ConsumerGroupStore)
		adapterCfg := logStorage.DefaultAdapterConfig()
		adapterCfg.RecoverOnStartup = cfg.Storage.RecoverOnStartup
		adapterCfg.RecoveryLogger = slog.Warn
		adapterCfg.SyncInterval = cfg.Storage.QueueSyncInterval
		queueLogStore, err = logStorage.NewAdapter(queueDir, adapterCfg)
		if err != nil {
			slog.Error("Failed to initialize queue log storage", "error", err)
			os.Exit(1)
		}
		if wantsDurableSync(cfg) {
			durableStore, ok := any(queueLogStore).(queueStorage.DurableQueueStore)
			if !ok || !durableStore.SupportsDurableSync() {
				slog.Error("a queue asks for fsync acknowledgement but the queue log cannot sync a single append",
					"hint", "set ack_durability: buffered, or use a queue log that supports durable sync")
				os.Exit(1)
			}
		}

		// The store is released by the deferred teardown registered before
		// b.Close, so that it runs after the broker has stopped the queue
		// manager. Registering it here would run it first: defers are LIFO, and
		// this is registered later than b.Close.

		// Convert queue configs from main config to queue types
		queueCfg := queue.DefaultConfig()
		queueCfg.AutoCommitInterval = cfg.QueueManager.AutoCommitInterval
		// An omitted key leaves the dispatcher default in place; a written one
		// is used as written, and validation already refused a written zero.
		queueCfg.CaptureWorkers = valueOr(cfg.QueueManager.CaptureWorkers, queueCfg.CaptureWorkers)
		queueCfg.CaptureQueueDepth = valueOr(cfg.QueueManager.CaptureQueueDepth, queueCfg.CaptureQueueDepth)
		queueCfg.CaptureDrainTimeout = valueOr(cfg.QueueManager.CaptureDrainTimeout, queueCfg.CaptureDrainTimeout)
		queueCfg.AckDurability = queue.AckDurability(cfg.Storage.QueueAckDurability)
		queueCfg.WritePolicy = queue.WritePolicy(cfg.Cluster.Raft.WritePolicy)
		queueCfg.DistributionMode = queue.DistributionMode(cfg.Cluster.Raft.DistributionMode)
		for _, qc := range cfg.Queues {
			replication := queueTypes.ReplicationConfig{}
			if qc.Replication.Enabled {
				replication = queueTypes.ReplicationConfig{
					Enabled: qc.Replication.Enabled,
				}
				if strings.EqualFold(qc.Replication.Mode, "async") {
					replication.Mode = queueTypes.ReplicationAsync
				} else {
					replication.Mode = queueTypes.ReplicationSync
				}

				replication.Group = qc.Replication.Group
				replication.ReplicationFactor = qc.Replication.ReplicationFactor
				if replication.ReplicationFactor == 0 {
					replication.ReplicationFactor = cfg.Cluster.Raft.ReplicationFactor
					if replication.ReplicationFactor == 0 {
						replication.ReplicationFactor = 3
					}
				}

				replication.MinInSyncReplicas = qc.Replication.MinInSyncReplicas
				if replication.MinInSyncReplicas == 0 {
					replication.MinInSyncReplicas = cfg.Cluster.Raft.MinInSyncReplicas
					if replication.MinInSyncReplicas == 0 {
						replication.MinInSyncReplicas = 2
					}
				}

				replication.AckTimeout = qc.Replication.AckTimeout
				if replication.AckTimeout <= 0 {
					replication.AckTimeout = cfg.Cluster.Raft.AckTimeout
					if replication.AckTimeout <= 0 {
						replication.AckTimeout = 5 * time.Second
					}
				}

				replication.HeartbeatTimeout = qc.Replication.HeartbeatTimeout
				if replication.HeartbeatTimeout <= 0 {
					replication.HeartbeatTimeout = cfg.Cluster.Raft.HeartbeatTimeout
				}
				replication.ElectionTimeout = qc.Replication.ElectionTimeout
				if replication.ElectionTimeout <= 0 {
					replication.ElectionTimeout = cfg.Cluster.Raft.ElectionTimeout
				}
				replication.SnapshotInterval = qc.Replication.SnapshotInterval
				if replication.SnapshotInterval <= 0 {
					replication.SnapshotInterval = cfg.Cluster.Raft.SnapshotInterval
				}
				replication.SnapshotThreshold = qc.Replication.SnapshotThreshold
				if replication.SnapshotThreshold == 0 {
					replication.SnapshotThreshold = cfg.Cluster.Raft.SnapshotThreshold
				}
			}

			queueCfg.QueueConfigs = append(queueCfg.QueueConfigs, queueTypes.FromInput(queueTypes.QueueConfigInput{
				Name:           qc.Name,
				Topics:         qc.Topics,
				Reserved:       qc.Reserved,
				Type:           queueTypes.QueueType(qc.Type),
				PrimaryGroup:   qc.PrimaryGroup,
				AckDurability:  qc.AckDurability,
				MaxMessageSize: qc.Limits.MaxMessageSize,
				MaxDepth:       qc.Limits.MaxDepth,
				MessageTTL:     qc.Limits.MessageTTL,
				MaxRetries:     qc.Retry.MaxRetries,
				InitialBackoff: qc.Retry.InitialBackoff,
				MaxBackoff:     qc.Retry.MaxBackoff,
				Multiplier:     qc.Retry.Multiplier,
				DLQEnabled:     qc.DLQ.Enabled,
				DLQTopic:       qc.DLQ.Topic,
				Retention: queueTypes.RetentionPolicy{
					RetentionTime:     qc.Retention.MaxAge,
					RetentionBytes:    qc.Retention.MaxLengthBytes,
					RetentionMessages: qc.Retention.MaxLengthMessages,
				},
				Replication: replication,
			}))
		}
		configuredQueueContracts = append(configuredQueueContracts, queueCfg.QueueConfigs...)
		protectedQueueContracts, err := localPrincipalPublishTargetContracts(
			cfg.Auth.LocalPrincipals,
			configuredQueueContracts,
		)
		if err != nil {
			slog.Error("Invalid local principal publish target", "error", err)
			os.Exit(1)
		}
		queueCfg.ProtectedQueueContracts = protectedQueueContracts

		// Notify AMQP 0.9.1 clients when their consumers are removed by stale cleanup
		queueCfg.OnConsumerRemoved = func(queueName, groupID string, consumerIDs []string) {
			amqp091Broker.CancelConsumers(queueName, groupID, consumerIDs)
		}

		// Delivery dispatcher: routes to AMQP or MQTT broker based on client ID prefix.
		deliveryTarget := &brokerDeliveryTarget{
			mqtt:    b,
			amqp:    amqpBroker,
			amqp091: amqp091Broker,
		}

		// Create log-based queue manager with wildcard support
		qm = queue.NewManager(
			queueLogStore,
			queueLogStore,
			deliveryTarget,
			queueCfg,
			logger,
			cl,
		)

		// Initialize queue Raft replication if enabled (default + optional per-group managers).
		if cfg.Cluster.Enabled && cfg.Cluster.Raft.Enabled {
			raftCoordinator, defaultRaftManager, groupRuntimes, err := qraft.StartQueueCoordinator(
				cfg.Cluster.NodeID,
				cfg.Cluster.Raft,
				queueLogStore,
				queueLogStore,
				clusterTLS,
				logger,
			)
			if err != nil {
				slog.Error("Failed to start Raft manager", "error", err)
				os.Exit(1)
			}

			if raftCoordinator != nil {
				qm.SetRaftCoordinator(raftCoordinator)
			} else if defaultRaftManager != nil {
				qm.SetRaftManager(defaultRaftManager)
			}

			groupIDs := make([]string, 0, len(groupRuntimes))
			for _, runtime := range groupRuntimes {
				groupIDs = append(groupIDs, runtime.GroupID)
			}

			slog.Info("Raft replication enabled",
				slog.String("node_id", cfg.Cluster.NodeID),
				slog.Int("group_count", len(groupRuntimes)),
				slog.Any("groups", groupIDs))
		}

		if err := b.SetQueueManager(qm); err != nil {
			slog.Error("Failed to set queue manager", "error", err)
			os.Exit(1)
		}
		if err := validateLocalPrincipalPublishTargets(
			context.Background(),
			cfg.Auth.LocalPrincipals,
			configuredQueueContracts,
			qm.QueueStore(),
		); err != nil {
			slog.Error("Invalid local principal publish target", "error", err)
			os.Exit(1)
		}

		// Set queue manager on AMQP broker
		amqpBroker.SetQueueManager(qm)
		amqp091Broker.SetQueueManager(qm)

		// Set queue handler on cluster for cross-node message routing
		if etcdCluster != nil {
			etcdCluster.SetQueueHandler(qm)
		}

		slog.Info("Log-based queue initialized", "storage", "file", "dir", queueDir)
	}

	// Local pub/sub dispatcher: routes pub/sub messages to the correct protocol broker
	// based on client ID prefix. Must live outside the queue block so it is always wired
	// even when the queue manager is not configured.
	crossDeliver := corebroker.CrossDeliverFunc(func(ctx context.Context, clientID string, topic string, payload []byte, qos byte, props map[string]string) {
		if amqp1broker.IsAMQPClient(clientID) {
			amqpBroker.LocalDeliverPubSub(ctx, clientID, topic, payload, qos, props)
			return
		}
		if amqpbroker.IsAMQP091Client(clientID) {
			amqp091Broker.LocalDeliverPubSub(ctx, clientID, topic, payload, qos, props)
			return
		}
		s := b.Get(clientID)
		if s == nil {
			return
		}
		msg := message.New(topic, payload)
		msg.BrokerMeta.Delivery.QoS = qos
		if err := message.ApplyTrustedProperties(msg, props); err != nil {
			slog.Warn("cross-deliver dropped malformed properties", "client_id", clientID, "topic", topic, "error", err)
		}
		if _, err := b.DeliverToSession(ctx, s, msg); err != nil {
			slog.Debug("cross-deliver to MQTT session failed", "client_id", clientID, "topic", topic, "error", err)
		}
	})
	b.SetCrossDeliver(crossDeliver)
	amqp091Broker.SetCrossDeliver(crossDeliver)
	amqpBroker.SetCrossDeliver(crossDeliver)

	// Set cluster on AMQP brokers for cross-node pub/sub routing
	amqpBroker.SetCluster(cl)
	amqpBroker.SetRoutePublishTimeout(cfg.Cluster.Transport.RoutePublishTimeout)
	amqp091Broker.SetCluster(cl)
	amqp091Broker.SetRoutePublishTimeout(cfg.Cluster.Transport.RoutePublishTimeout)

	// Set message handler and forward publish handler on cluster if it's an etcd cluster
	if etcdCluster != nil {
		dispatcher := wiring.NewMessageDispatcher(b, amqpBroker, amqp091Broker)
		etcdCluster.SetMessageHandler(dispatcher)
		etcdCluster.SetForwardPublishHandler(dispatcher)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	serverErr := make(chan error, 10)

	tcpSlots := []struct {
		name string
		cfg  config.MQTTTCPListenerConfig
	}{
		{name: "v3", cfg: cfg.Server.MQTT.TCP.V3},
		{name: "v5", cfg: cfg.Server.MQTT.TCP.V5},
		{name: listenerTLS, cfg: cfg.Server.MQTT.TCP.TLS},
		{name: listenerMTLS, cfg: cfg.Server.MQTT.TCP.MTLS},
	}

	for _, slot := range tcpSlots {
		if strings.TrimSpace(slot.cfg.Addr) == "" {
			continue
		}

		tlsCfg, err := mqtttls.LoadTLSConfig[*tls.Config](&slot.cfg.TLS)
		if err != nil {
			slog.Error("Failed to build TCP TLS configuration", "listener", slot.name, "error", err)
			os.Exit(1)
		}

		tcpCfg := tcp.Config{
			Address:              slot.cfg.Addr,
			TLSConfig:            tlsCfg,
			ShutdownTimeout:      cfg.Server.ShutdownTimeout,
			MaxConnections:       slot.cfg.MaxConnections,
			ReadTimeout:          slot.cfg.ReadTimeout,
			WriteTimeout:         slot.cfg.WriteTimeout,
			SendQueueSize:        cfg.Session.MaxSendQueueSize,
			DisconnectOnFull:     cfg.Session.DisconnectOnFull,
			ProtocolVersion:      protocolVersionForMode(slot.cfg.Protocol),
			MaxPacketSize:        maxMQTTPacketSize(cfg.Broker.MaxMessageSize),
			RequireMQTTTwoFactor: slot.name == listenerMTLS,
			Logger:               logger,
		}
		tcpCfg.IPRateLimiter = rateLimitManager
		tcpServer := tcp.New(tcpCfg, b)

		wg.Add(1)
		go func(name, addr, protocol string, server *tcp.Server) {
			defer wg.Done()
			slog.Info("Starting TCP server", "mode", name, "address", addr, "protocol", config.NormalizeProtocolMode(protocol))
			if err := server.Listen(ctx); err != nil {
				serverErr <- err
			}
		}(slot.name, slot.cfg.Addr, slot.cfg.Protocol, tcpServer)
	}

	wsSlots := []struct {
		name string
		cfg  config.MQTTWebSocketListenerConfig
	}{
		{name: "v3", cfg: cfg.Server.MQTT.WebSocket.V3},
		{name: "v5", cfg: cfg.Server.MQTT.WebSocket.V5},
		{name: listenerTLS, cfg: cfg.Server.MQTT.WebSocket.TLS},
		{name: listenerMTLS, cfg: cfg.Server.MQTT.WebSocket.MTLS},
	}

	for _, slot := range wsSlots {
		if strings.TrimSpace(slot.cfg.Addr) == "" {
			continue
		}

		tlsCfg, err := mqtttls.LoadTLSConfig[*tls.Config](&slot.cfg.TLS)
		if err != nil {
			slog.Error("Failed to build WebSocket TLS configuration", "listener", slot.name, "error", err)
			os.Exit(1)
		}

		wsCfg := websocket.Config{
			Address:              slot.cfg.Addr,
			Path:                 slot.cfg.Path,
			ShutdownTimeout:      cfg.Server.ShutdownTimeout,
			TLSConfig:            tlsCfg,
			ProtocolVersion:      protocolVersionForMode(slot.cfg.Protocol),
			AllowedOrigins:       slot.cfg.AllowedOrigins,
			MaxPacketSize:        maxMQTTPacketSize(cfg.Broker.MaxMessageSize),
			ReadTimeout:          slot.cfg.ReadTimeout,
			WriteTimeout:         slot.cfg.WriteTimeout,
			MaxConnections:       slot.cfg.MaxConnections,
			RequireMQTTTwoFactor: slot.name == listenerMTLS,
		}
		wsCfg.IPRateLimiter = rateLimitManager

		wsServer := websocket.New(wsCfg, b, logger)

		wg.Add(1)
		go func(name, addr, path, protocol string, server *websocket.Server) {
			defer wg.Done()
			slog.Info("Starting WebSocket server", "mode", name, "address", addr, "path", path, "protocol", config.NormalizeProtocolMode(protocol))
			if err := server.Listen(ctx); err != nil {
				serverErr <- err
			}
		}(slot.name, slot.cfg.Addr, slot.cfg.Path, slot.cfg.Protocol, wsServer)
	}

	httpSlots := []struct {
		name string
		cfg  config.HTTPListenerConfig
	}{
		{name: listenerPlain, cfg: cfg.Server.HTTP.Plain},
		{name: listenerTLS, cfg: cfg.Server.HTTP.TLS},
		{name: listenerMTLS, cfg: cfg.Server.HTTP.MTLS},
	}

	for _, slot := range httpSlots {
		if strings.TrimSpace(slot.cfg.Addr) == "" {
			continue
		}

		var tlsCfg *tls.Config
		if slot.name != listenerPlain {
			var err error
			tlsCfg, err = mqtttls.LoadTLSConfig[*tls.Config](&slot.cfg.TLS)
			if err != nil {
				slog.Error("Failed to build HTTP TLS configuration", "listener", slot.name, "error", err)
				os.Exit(1)
			}
		}

		httpCfg := http.Config{
			Address:         slot.cfg.Addr,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
			TLSConfig:       tlsCfg,
		}
		httpServer := http.New(httpCfg, b, logger)

		wg.Add(1)
		go func(name, addr string, server *http.Server) {
			defer wg.Done()
			slog.Info("Starting HTTP-MQTT bridge", "mode", name, "address", addr)
			if err := server.Listen(ctx); err != nil {
				serverErr <- err
			}
		}(slot.name, slot.cfg.Addr, httpServer)
	}

	coapSlots := []struct {
		name string
		cfg  config.CoAPListenerConfig
	}{
		{name: listenerPlain, cfg: cfg.Server.CoAP.Plain},
		{name: "dtls", cfg: cfg.Server.CoAP.DTLS},
		{name: "mdtls", cfg: cfg.Server.CoAP.MDTLS},
	}

	for _, slot := range coapSlots {
		if strings.TrimSpace(slot.cfg.Addr) == "" {
			continue
		}

		var dtlsCfg *piondtls.Config
		if slot.name != listenerPlain {
			var err error
			dtlsCfg, err = mqtttls.LoadTLSConfig[*piondtls.Config](&slot.cfg.TLS)
			if err != nil {
				slog.Error("Failed to build CoAP DTLS configuration", "listener", slot.name, "error", err)
				os.Exit(1)
			}
		}

		coapCfg := coap.Config{
			Address:         slot.cfg.Addr,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
			TLSConfig:       dtlsCfg,
		}
		coapServer := coap.New(coapCfg, b, logger)

		wg.Add(1)
		go func(name, addr string, server *coap.Server) {
			defer wg.Done()
			slog.Info("Starting CoAP server", "mode", name, "address", addr)
			if err := server.Listen(ctx); err != nil {
				serverErr <- err
			}
		}(slot.name, slot.cfg.Addr, coapServer)
	}

	// AMQP 1.0 servers
	amqpSlots := []struct {
		name string
		cfg  config.AMQPListenerConfig
	}{
		{name: listenerPlain, cfg: cfg.Server.AMQP.Plain},
		{name: listenerTLS, cfg: cfg.Server.AMQP.TLS},
		{name: listenerMTLS, cfg: cfg.Server.AMQP.MTLS},
	}

	for _, slot := range amqpSlots {
		if strings.TrimSpace(slot.cfg.Addr) == "" {
			continue
		}

		tlsCfg, err := mqtttls.LoadTLSConfig[*tls.Config](&slot.cfg.TLS)
		if err != nil {
			slog.Error("Failed to build AMQP TLS configuration", "listener", slot.name, "error", err)
			os.Exit(1)
		}

		amqpCfg := amqp1server.Config{
			Address:          slot.cfg.Addr,
			TLSConfig:        tlsCfg,
			HandshakeTimeout: slot.cfg.HandshakeTimeout,
			ShutdownTimeout:  cfg.Server.ShutdownTimeout,
			MaxConnections:   slot.cfg.MaxConnections,
			Logger:           logger,
		}
		amqpSrv := amqp1server.New(amqpCfg, amqpBroker)

		wg.Add(1)
		go func(name, addr string, server *amqp1server.Server) {
			defer wg.Done()
			slog.Info("Starting AMQP server", "mode", name, "address", addr)
			if err := server.Listen(ctx); err != nil {
				serverErr <- err
			}
		}(slot.name, slot.cfg.Addr, amqpSrv)
	}

	// AMQP 0.9.1 servers. The public listeners receive only the external
	// callout policy; the local listeners receive only the local-principal
	// policy. There is deliberately no fallback between these policies.
	maxAMQP091MessageSize := uint64(0)
	if cfg.Broker.MaxMessageSize > 0 {
		maxAMQP091MessageSize = uint64(cfg.Broker.MaxMessageSize)
	}
	externalAMQP091Policy := amqpbroker.NewExternalConnectionPolicy(
		amqp091ExternalAuth,
		amqp091ExternalHooks,
		maxAMQP091MessageSize,
	)
	// Both local listeners share one policy. They differ only in network
	// placement: capability comes from the authenticated principal's role, so a
	// principal cannot widen itself by choosing a port.
	localAMQP091Policy := amqpbroker.NewLocalConnectionPolicy(
		localPolicyAdapter,
		localPolicyAdapter,
		localPolicyAdapter,
		maxAMQP091MessageSize,
	)
	amqp091Slots := []struct {
		name   string
		cfg    config.AMQP091ListenerConfig
		policy *amqpbroker.ConnectionPolicy
	}{
		{name: listenerPlain, cfg: cfg.Server.AMQP091.Plain, policy: externalAMQP091Policy},
		{name: listenerTLS, cfg: cfg.Server.AMQP091.TLS, policy: externalAMQP091Policy},
		{name: listenerMTLS, cfg: cfg.Server.AMQP091.MTLS, policy: externalAMQP091Policy},
	}
	// Every local-principal listener gets the same policy under whichever key
	// named it. They differ only in network placement.
	for _, listener := range cfg.Server.AMQP091.LocalListeners() {
		amqp091Slots = append(amqp091Slots, struct {
			name   string
			cfg    config.AMQP091ListenerConfig
			policy *amqpbroker.ConnectionPolicy
		}{name: listener.Name, cfg: listener.Config, policy: localAMQP091Policy})
	}
	if deprecated := cfg.Server.AMQP091.DeprecatedLocalListenerNames(); len(deprecated) > 0 {
		slog.Warn("Deprecated AMQP 0.9.1 listener key",
			"keys", strings.Join(deprecated, ","),
			"replacement", "server.amqp091.local",
			"reason", "local listeners are equivalent; capability comes from the principal role")
	}

	var amqp091Ready []<-chan struct{}
	for _, slot := range amqp091Slots {
		if strings.TrimSpace(slot.cfg.Addr) == "" {
			continue
		}

		tlsCfg, err := mqtttls.LoadTLSConfig[*tls.Config](&slot.cfg.TLS)
		if err != nil {
			slog.Error("Failed to build AMQP 0.9.1 TLS configuration", "listener", slot.name, "error", err)
			os.Exit(1)
		}

		amqp091Cfg := amqpserver.Config{
			Address:                 slot.cfg.Addr,
			TLSConfig:               tlsCfg,
			HandshakeTimeout:        slot.cfg.HandshakeTimeout,
			DisableHandshakeTimeout: slot.cfg.HandshakeTimeout == 0,
			ShutdownTimeout:         cfg.Server.ShutdownTimeout,
			MaxConnections:          slot.cfg.MaxConnections,
			ConnectionPolicy:        slot.policy,
			Logger:                  logger,
		}
		amqp091Srv := amqpserver.New(amqp091Cfg, amqp091Broker)
		amqp091Ready = append(amqp091Ready, amqp091Srv.Ready())

		wg.Add(1)
		go func(name, addr string, server *amqpserver.Server) {
			defer wg.Done()
			slog.Info("Starting AMQP 0.9.1 server", "mode", name, "address", addr)
			if err := server.Listen(ctx); err != nil {
				serverErr <- err
			}
		}(slot.name, slot.cfg.Addr, amqp091Srv)
	}

	for _, ready := range amqp091Ready {
		select {
		case <-ready:
		case err := <-serverErr:
			slog.Error("AMQP 0.9.1 listener failed during startup", "error", err)
			os.Exit(1)
		case <-time.After(10 * time.Second):
			slog.Error("Timed out waiting for AMQP 0.9.1 listener readiness")
			os.Exit(1)
		}
	}

	if cfg.Server.HealthEnabled {
		healthCfg := health.Config{
			Address:         cfg.Server.HealthAddr,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
		}
		healthServer := health.New(healthCfg, b, cl, store, logger)

		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("Starting health check server", "address", cfg.Server.HealthAddr)
			if err := healthServer.Listen(ctx); err != nil {
				serverErr <- err
			}
		}()
	}

	// Initialize config reload manager.
	reloadManager := reload.New(
		*configFile, cfg,
		reload.WithLogSetup(reload.SetupLogger),
		reload.WithRateLimiter(rateLimitManager),
		reload.WithBroker(b),
		reload.WithSessionTuner(b),
		reload.WithWebhookTuner(webhookNotifier),
		reload.WithLocalPrincipalsReloadFailure(func(error) {
			amqp091Broker.RecordLocalPrincipalReload(false)
		}),
		reload.WithLocalPrincipalsReload(func(principals []config.LocalPrincipalConfig) (bool, error) {
			changed, err := reloadLocalPrincipals(
				context.Background(),
				localPrincipalStore,
				principals,
				configuredQueueContracts,
				qm,
			)
			if err != nil {
				amqp091Broker.RecordLocalPrincipalReload(false)
				return false, err
			}
			amqp091Broker.RecordLocalPrincipalReload(true)
			if !changed {
				return false, nil
			}
			disconnected := amqp091Broker.DisconnectInvalidLocalSessions(localPolicyAdapter.IsSessionActive)
			slog.Info("Local principals reloaded",
				"outcome", "success",
				"generation", localPrincipalStore.Generation(),
				"disconnected_sessions", disconnected)
			return true, nil
		}),
	)

	// Start Admin API server (HTTP + Connect/gRPC queue service)
	if cfg.Server.AdminAPIAddr != "" {
		apiCfg := api.Config{
			Address:         cfg.Server.AdminAPIAddr,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
		}

		if qm != nil && queueLogStore != nil {
			apiServer := api.New(apiCfg, b, amqp091Broker, cl, qm, qm.QueueStore(), qm.GroupStore(), logger)
			apiServer.SetReloadManager(reloadManager)

			wg.Add(1)
			go func() {
				defer wg.Done()
				slog.Info("Starting admin API server", "address", cfg.Server.AdminAPIAddr)
				if err := apiServer.Listen(ctx); err != nil {
					serverErr <- err
				}
			}()
		} else {
			slog.Warn("Queue manager or log storage not available, admin API server disabled")
		}
	}

	slog.Info("MQTT broker started successfully")

	// SIGHUP triggers config reload; SIGINT/SIGTERM trigger shutdown.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	for {
		select {
		case sig := <-sigChan:
			if sig == syscall.SIGHUP {
				slog.Info("Received SIGHUP, reloading configuration")
				if _, err := reloadManager.Reload(ctx); err != nil {
					slog.Error("Config reload failed", "error", err)
				}
				continue
			}
			slog.Info("Received shutdown signal", "signal", sig)
		case err := <-serverErr:
			slog.Error("Server error", "error", err)
		}
		break
	}

	reloadManager.Shutdown()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	cancel()

	if err := b.Shutdown(shutdownCtx, cfg.Server.ShutdownTimeout); err != nil {
		slog.Error("Error during shutdown", "error", err)
	}

	if otelShutdown != nil {
		otelShutdownCtx, otelCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer otelCancel()
		if err := otelShutdown(otelShutdownCtx); err != nil {
			slog.Error("Failed to shutdown OpenTelemetry", "error", err)
		} else {
			slog.Info("OpenTelemetry shutdown complete")
		}
	}

	wg.Wait()
	slog.Info("MQTT broker stopped")
}

// wantsDurableSync reports whether any queue will acknowledge publishes only
// after an fsync, either through its own policy or the broker-wide default.
// Promising that on a store that cannot sync one append is a promise the broker
// cannot keep, so startup refuses it rather than quietly acknowledging from the
// page cache.
func wantsDurableSync(cfg *config.Config) bool {
	brokerWide := queue.NormalizeAckDurability(queue.AckDurability(cfg.Storage.QueueAckDurability))
	for _, q := range cfg.Queues {
		policy := q.AckDurability
		if strings.TrimSpace(policy) == "" {
			if brokerWide == queue.AckDurabilityFsync {
				return true
			}
			continue
		}
		if queue.NormalizeAckDurability(queue.AckDurability(policy)) == queue.AckDurabilityFsync {
			return true
		}
	}
	return brokerWide == queue.AckDurabilityFsync
}
