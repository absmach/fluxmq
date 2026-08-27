// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/message"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdtransport "go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/embed"
)

const (
	// MQTT-specific prefixes.
	willPrefix     = "/mqtt/wills/"
	retainedPrefix = "/mqtt/retained/"

	// Protocol-agnostic prefixes.
	subscriptionsPrefix  = "/subscriptions/"
	sessionsPrefix       = "/sessions/"
	queueConsumersPrefix = "/queue-consumers/"
	electionPrefix       = "/leader"

	defaultRouteBatchFlushWorkers = 4
)

var (
	_ Cluster = (*EtcdCluster)(nil)

	ErrEtcdServerStartTimeout     = errors.New("etcd server took too long to start")
	ErrTransportNotConfigured     = errors.New("transport not configured")
	ErrNoMessageHandlerConfigured = errors.New("no message handler configured")
	ErrNoLocalStoreConfigured     = errors.New("no local store configured")
	ErrSessionOwned               = errors.New("session is owned by another node")
	ErrSessionOwnershipLost       = errors.New("local session ownership was lost")
	ErrTakeoverInProgress         = errors.New("session takeover is already in progress")
)

// SessionOwnedError reports the node that won a competing ownership claim.
type SessionOwnedError struct {
	ClientID string
	Owner    string
}

func (e *SessionOwnedError) Error() string {
	return fmt.Sprintf("session %q is owned by node %q", e.ClientID, e.Owner)
}

func (e *SessionOwnedError) Unwrap() error { return ErrSessionOwned }

// EtcdCluster implements the Cluster interface using embedded etcd.
type EtcdCluster struct {
	nodeID string
	config *EtcdConfig

	// Embedded etcd server
	etcd   *embed.Etcd
	client *clientv3.Client

	// For leadership election
	election *concurrency.Election
	session  *concurrency.Session

	// Lease for session ownership (with auto-renewal).
	// leasedKeys tracks every key this node has registered under the
	// session lease (key → value) so they can be re-registered after
	// the lease expires (e.g. etcd stall or leader election); guarded
	// by leaseMu together with sessionLease.
	sessionLease    clientv3.LeaseID
	leaseMu         sync.Mutex
	leaseRecoveryMu sync.Mutex
	leaseCancel     context.CancelFunc
	leasedKeys      map[string]string

	// Throttles the unknown-owner warning in RoutePublish (unix nanos of last log).
	lastUnknownOwnerWarn atomic.Int64

	// gRPC transport for inter-broker communication
	transport *Transport

	// Handler for incoming routed messages and session management
	msgHandler MessageHandler

	logger *slog.Logger

	// Local subscription cache for fast topic matching.
	// subCacheRev is the etcd revision the cache was loaded at; the watch
	// resumes from it so no event is missed between load and watch.
	subCache    map[string]*storage.Subscription // key: clientID|filter
	clientSubs  map[string][]string              // clientID → []cacheKey (reverse index)
	subTrie     *router.TrieRouter
	subCacheRev int64
	subCacheMu  sync.RWMutex

	// Local session owner cache to avoid etcd roundtrips in RoutePublish.
	// ownerCacheRev is the etcd revision the cache was loaded at; the owner
	// watch resumes from it so no event is missed between load and watch.
	ownerCache    map[string]string // clientID -> nodeID
	ownerCacheRev int64
	ownerCacheMu  sync.RWMutex

	// Local queue consumer cache for fast queue delivery/routing lookups.
	// queueConsumersCacheRev: see subCacheRev.
	queueConsumersAll      map[string]*QueueConsumerInfo                       // key: queue|group|consumer
	queueConsumersByQueue  map[string]map[string]*QueueConsumerInfo            // queue -> key -> info
	queueConsumersByGroup  map[string]map[string]map[string]*QueueConsumerInfo // queue -> group -> key -> info
	queueConsumersCacheRev int64
	queueConsumersCacheMu  sync.RWMutex

	routeBatchMaxSize      int
	routeBatchMaxDelay     time.Duration
	routeBatchFlushWorkers int
	forwardBatcher         *nodeBatcher[*clusterv1.ForwardPublishRequest]
	queueBatcher           *nodeBatcher[QueueDelivery]

	// Local retained message cache for fast wildcard matching (deprecated, use hybridRetained).
	// retainedCacheRev: see subCacheRev.
	retainedCache    map[string]*message.Envelope // key: topic
	retainedCacheRev int64
	retainedCacheMu  sync.RWMutex

	// Hybrid storage
	localStore     storage.Store  // BadgerDB for local payload storage
	hybridRetained *RetainedStore // Hybrid retained store
	hybridWill     *WillStore     // Hybrid will store

	wg     sync.WaitGroup
	stopCh chan struct{}

	lifecycleCtx    context.Context
	cancelLifecycle context.CancelFunc
}

// EtcdConfig holds embedded etcd configuration.
type EtcdConfig struct {
	NodeID                      string
	DataDir                     string
	BindAddr                    string
	ClientAddr                  string
	AdvertiseAddr               string
	InitialCluster              string
	TransportAddr               string
	PeerTransports              map[string]string
	Bootstrap                   bool
	AllowInsecure               bool
	HybridRetainedSizeThreshold int // Size threshold in bytes for hybrid retained storage (default 1024)
	RouteBatchMaxSize           int
	RouteBatchMaxDelay          time.Duration
	RouteBatchFlushWorkers      int

	// Transport TLS configuration
	TransportTLS *TransportTLSConfig
}

// TransportTLSConfig holds TLS configuration for inter-broker gRPC transport.
type TransportTLSConfig struct {
	CertFile string // Server certificate file
	KeyFile  string // Server private key file
	CAFile   string // CA certificate for verifying peer certificates
}

// NewEtcdCluster creates a new embedded etcd cluster.
func NewEtcdCluster(cfg *EtcdConfig, localStore storage.Store, logger *slog.Logger) (*EtcdCluster, error) {
	if cfg == nil {
		return nil, fmt.Errorf("etcd cluster configuration is required")
	}
	if !isLoopbackAddress(cfg.ClientAddr) {
		return nil, fmt.Errorf("embedded etcd client address must be loopback-only")
	}
	if cfg.TransportTLS == nil && !cfg.AllowInsecure {
		return nil, fmt.Errorf("cluster TLS is required unless allow_insecure is enabled")
	}
	scheme := "http"
	var clientTLSConfig *tls.Config
	if cfg.TransportTLS != nil {
		scheme = "https"
		_, loadedClientTLS, err := LoadMutualTLSConfigs(cfg.TransportTLS)
		if err != nil {
			return nil, err
		}
		clientTLSConfig = loadedClientTLS
	}

	// Create embedded etcd configuration
	eCfg := embed.NewConfig()
	eCfg.Name = cfg.NodeID
	eCfg.Dir = cfg.DataDir

	// Peer URLs (for Raft communication)
	peerURL, err := url.Parse(scheme + "://" + cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid bind address: %w", err)
	}
	eCfg.ListenPeerUrls = []url.URL{*peerURL}

	// Advertise URL (what other nodes use to contact this node)
	if cfg.AdvertiseAddr != "" {
		advertiseURL, err := url.Parse(scheme + "://" + cfg.AdvertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("invalid advertise address: %w", err)
		}
		eCfg.AdvertisePeerUrls = []url.URL{*advertiseURL}
	} else {
		eCfg.AdvertisePeerUrls = []url.URL{*peerURL}
	}

	// Client URLs (for KV operations)
	clientURL, err := url.Parse(scheme + "://" + cfg.ClientAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid client address: %w", err)
	}
	eCfg.ListenClientUrls = []url.URL{*clientURL}
	eCfg.AdvertiseClientUrls = []url.URL{*clientURL}
	if cfg.TransportTLS != nil {
		tlsInfo := etcdtransport.TLSInfo{
			CertFile:       cfg.TransportTLS.CertFile,
			KeyFile:        cfg.TransportTLS.KeyFile,
			ClientCertFile: cfg.TransportTLS.CertFile,
			ClientKeyFile:  cfg.TransportTLS.KeyFile,
			TrustedCAFile:  cfg.TransportTLS.CAFile,
			ClientCertAuth: true,
		}
		eCfg.PeerTLSInfo = tlsInfo
		eCfg.ClientTLSInfo = tlsInfo
	}

	// Cluster configuration
	eCfg.InitialCluster = cfg.InitialCluster
	if cfg.Bootstrap {
		eCfg.ClusterState = "new"
	} else {
		eCfg.ClusterState = "existing"
	}

	// Disable etcd logging (we'll use our own logger)
	eCfg.Logger = "zap"
	eCfg.LogLevel = "error"

	// Start embedded etcd
	e, err := embed.StartEtcd(eCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to start etcd: %w", err)
	}

	// Wait for etcd to be ready
	select {
	case <-e.Server.ReadyNotify():
		logger.Info("etcd server is ready", slog.String("node_id", cfg.NodeID))
	case <-time.After(60 * time.Second):
		e.Server.Stop()
		return nil, ErrEtcdServerStartTimeout
	}

	// Create etcd client
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		DialTimeout: 5 * time.Second,
		TLS:         clientTLSConfig,
	})
	if err != nil {
		e.Close()
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	// Create session for leadership and leases
	s, err := concurrency.NewSession(client, concurrency.WithTTL(10))
	if err != nil {
		client.Close()
		e.Close()
		return nil, fmt.Errorf("failed to create concurrency session: %w", err)
	}

	// Create election for leadership
	election := concurrency.NewElection(s, electionPrefix)

	c := &EtcdCluster{
		nodeID:                cfg.NodeID,
		config:                cfg,
		etcd:                  e,
		client:                client,
		election:              election,
		session:               s,
		logger:                logger,
		subCache:              make(map[string]*storage.Subscription),
		clientSubs:            make(map[string][]string),
		subTrie:               router.NewRouter(),
		ownerCache:            make(map[string]string),
		queueConsumersAll:     make(map[string]*QueueConsumerInfo),
		queueConsumersByQueue: make(map[string]map[string]*QueueConsumerInfo),
		queueConsumersByGroup: make(map[string]map[string]map[string]*QueueConsumerInfo),
		retainedCache:         make(map[string]*message.Envelope),
		localStore:            localStore,
		leasedKeys:            make(map[string]string),
		stopCh:                make(chan struct{}),
	}
	c.lifecycleCtx, c.cancelLifecycle = context.WithCancel(context.Background())

	const (
		defaultRouteBatchMaxSize  = 256
		defaultRouteBatchMaxDelay = 5 * time.Millisecond
	)
	c.routeBatchMaxSize = cfg.RouteBatchMaxSize
	if c.routeBatchMaxSize <= 0 {
		c.routeBatchMaxSize = defaultRouteBatchMaxSize
	}
	c.routeBatchMaxDelay = cfg.RouteBatchMaxDelay
	if c.routeBatchMaxDelay <= 0 {
		c.routeBatchMaxDelay = defaultRouteBatchMaxDelay
	}
	c.routeBatchFlushWorkers = cfg.RouteBatchFlushWorkers
	if c.routeBatchFlushWorkers <= 0 {
		c.routeBatchFlushWorkers = defaultRouteBatchFlushWorkers
	}

	// Create a lease for session ownership with auto-renewal
	if err := c.refreshSessionLease(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to create session lease: %w", err)
	}

	// Initialize gRPC transport if configured
	if cfg.TransportAddr != "" {
		transport, err := NewTransport(cfg.NodeID, cfg.TransportAddr, c, cfg.TransportTLS, logger)
		if err != nil {
			client.Close()
			s.Close()
			e.Close()
			return nil, fmt.Errorf("failed to create transport: %w", err)
		}
		c.transport = transport
	}

	if c.transport != nil {
		c.queueBatcher = newNodeBatcher[QueueDelivery](
			c.routeBatchMaxSize,
			c.routeBatchMaxDelay,
			c.routeBatchFlushWorkers,
			c.stopCh,
			logger.With(slog.String("batcher", "queue")),
			"queue",
			func(ctx context.Context, nodeID string, items []QueueDelivery) error {
				return c.transport.SendRouteQueueBatch(ctx, nodeID, items)
			},
		)
	}

	// Initialize hybrid retained store
	if localStore != nil {
		threshold := cfg.HybridRetainedSizeThreshold
		if threshold <= 0 {
			threshold = 1024 // Default to 1KB if not configured
		}
		c.hybridRetained = NewRetainedStore(
			cfg.NodeID,
			localStore.Retained(),
			client,
			c.transport,
			threshold,
			logger,
		)

		// Initialize hybrid will store using same threshold
		c.hybridWill = NewWillStore(
			cfg.NodeID,
			localStore.Wills(),
			client,
			c.transport,
			threshold,
			logger,
		)
	}

	return c, nil
}

func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return false
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Start begins cluster participation (campaigns for leadership).
func (c *EtcdCluster) Start() error {
	// Load existing subscriptions into cache
	if err := c.loadSubscriptionCache(); err != nil {
		c.logger.Warn("failed to load subscription cache", slog.String("error", err.Error()))
	}

	// Load existing session owners into cache
	if err := c.loadSessionOwnerCache(); err != nil {
		c.logger.Warn("failed to load session owner cache", slog.String("error", err.Error()))
	}

	// Load existing queue consumers into cache
	if err := c.loadQueueConsumerCache(); err != nil {
		c.logger.Warn("failed to load queue consumer cache", slog.String("error", err.Error()))
	}

	// Start watching for session owner changes
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.watchSessionOwners()
	}()

	// Start periodic session owner cache reconciliation
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.reconcileSessionOwnerCache()
	}()

	// Start watching for subscription changes
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.watchSubscriptions()
	}()

	// Start periodic subscription cache reconciliation
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.reconcileSubscriptionCache()
	}()

	// Start watching for queue consumer changes
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.watchQueueConsumers()
	}()

	// Load retained message cache on startup
	if err := c.loadRetainedCache(); err != nil {
		c.logger.Warn("failed to load retained cache", slog.String("error", err.Error()))
	}

	// Start watching for retained message changes
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.watchRetained()
	}()

	// Start gRPC transport if configured
	if c.transport != nil {
		if err := c.transport.Start(); err != nil {
			return fmt.Errorf("failed to start transport: %w", err)
		}

		// Connect to peer nodes with background retry for failures
		if c.config.PeerTransports != nil {
			for nodeID, addr := range c.config.PeerTransports {
				if nodeID != c.nodeID {
					if err := c.transport.ConnectPeer(nodeID, addr); err != nil {
						c.logger.Warn("failed to connect to peer", slog.String("node_id", nodeID), slog.String("error", err.Error()))
					}
				}
			}

			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.peerRetryLoop()
			}()
		}
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.campaignLeader()
	}()
	return nil
}

// Stop gracefully shuts down the cluster.
func (c *EtcdCluster) Stop() error {
	close(c.stopCh)
	if c.cancelLifecycle != nil {
		c.cancelLifecycle()
	}
	c.wg.Wait()

	c.leaseMu.Lock()
	if c.leaseCancel != nil {
		c.leaseCancel()
		c.leaseCancel = nil
	}
	c.leaseMu.Unlock()

	// Stop hybrid store watchers before closing the etcd client so they
	// don't keep retrying against a closed client.
	if c.hybridRetained != nil {
		c.hybridRetained.Close() //nolint:errcheck // best-effort shutdown
	}
	if c.hybridWill != nil {
		c.hybridWill.Close() //nolint:errcheck // best-effort shutdown
	}

	// Stop gRPC transport
	if c.transport != nil {
		c.transport.Stop() //nolint:errcheck // best-effort shutdown; transport is being discarded
	}

	// Revoke session (releases leadership)
	if c.session != nil {
		c.session.Close()
	}

	// Close client
	if c.client != nil {
		c.client.Close()
	}

	// Stop etcd server
	if c.etcd != nil {
		c.etcd.Close()
	}

	return nil
}

// peerRetryLoop periodically attempts to reconnect to peers that are not connected.
func (c *EtcdCluster) peerRetryLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			for nodeID, addr := range c.config.PeerTransports {
				if nodeID == c.nodeID {
					continue
				}
				if !c.transport.HasPeerConnection(nodeID) {
					if err := c.transport.ConnectPeer(nodeID, addr); err != nil {
						c.logger.Debug("peer retry failed", slog.String("node_id", nodeID), slog.String("error", err.Error()))
					} else {
						c.logger.Info("reconnected to peer", slog.String("node_id", nodeID))
					}
				}
			}
		}
	}
}

// NodeID returns this node's identifier.
func (c *EtcdCluster) NodeID() string {
	return c.nodeID
}

// Nodes returns information about all cluster nodes.
func (c *EtcdCluster) Nodes() []NodeInfo {
	// Query etcd for member list
	members := c.etcd.Server.Cluster().Members()

	nodes := make([]NodeInfo, 0, len(members))
	for _, member := range members {
		peerURL := ""
		if len(member.PeerURLs) > 0 {
			peerURL = member.PeerURLs[0]
		}

		// Check if node is healthy: either it's this node, or we have a gRPC connection to it
		healthy := member.Name == c.nodeID
		if !healthy && c.transport != nil {
			healthy = c.transport.HasPeerConnection(member.Name)
		}

		nodes = append(nodes, NodeInfo{
			ID:      member.Name,
			Address: peerURL,
			Healthy: healthy,
			Leader:  member.Name == c.nodeID && c.IsLeader(context.Background()),
		})
	}

	return nodes
}

// IsLeader checks if this node is the cluster leader.
func (c *EtcdCluster) IsLeader(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	resp, err := c.election.Leader(ctx)
	if err != nil {
		return false
	}

	if len(resp.Kvs) == 0 {
		return false
	}

	return string(resp.Kvs[0].Value) == c.nodeID
}

// WaitForLeader blocks until this node becomes leader.
func (c *EtcdCluster) WaitForLeader(ctx context.Context) error {
	for {
		if c.IsLeader(ctx) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Check again
		}
	}
}

// loadRetainedCache loads all retained messages from etcd into the local cache.
func (c *EtcdCluster) loadRetainedCache() error {
	ctx := context.Background()
	resp, err := c.client.Get(ctx, retainedPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to load retained messages: %w", err)
	}

	// Rebuild from scratch so reloads after watch interruptions also evict
	// entries whose delete events were missed.
	fresh := make(map[string]*message.Envelope, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		msg, err := message.UnmarshalBinary(kv.Value)
		if err != nil {
			c.logger.Warn("failed to unmarshal retained message during cache load", slog.String("error", err.Error()))
			continue
		}

		// Extract topic from key (remove prefix)
		topic := strings.TrimPrefix(string(kv.Key), retainedPrefix)
		fresh[topic] = msg
	}

	c.retainedCacheMu.Lock()
	previous := c.retainedCache
	c.retainedCache = fresh
	c.retainedCacheRev = resp.Header.Revision
	c.retainedCacheMu.Unlock()
	for _, envelope := range previous {
		message.Release(envelope)
	}

	c.logger.Info("loaded retained messages into cache", slog.Int("count", len(fresh)))
	return nil
}

// watchRetained watches etcd for retained message changes and updates the local cache.
func (c *EtcdCluster) watchRetained() {
	for {
		c.retainedCacheMu.RLock()
		rev := c.retainedCacheRev
		c.retainedCacheMu.RUnlock()
		watchCh := c.client.Watch(c.lifecycleCtx, retainedPrefix, prefixWatchOpts(rev)...)

		for {
			select {
			case <-c.stopCh:
				return
			case watchResp, ok := <-watchCh:
				if !ok {
					if c.lifecycleCtx.Err() != nil {
						return
					}
					c.logger.Warn("retained watch channel closed, reloading cache")
					if err := c.loadRetainedCache(); err != nil {
						c.logger.Error("failed to reload retained messages", slog.String("error", err.Error()))
					}
					goto restart
				}
				if watchResp.Err() != nil {
					c.logger.Error("retained watch error", slog.String("error", watchResp.Err().Error()))
					if err := c.loadRetainedCache(); err != nil {
						c.logger.Error("failed to reload retained messages", slog.String("error", err.Error()))
					}
					goto restart
				}

				c.retainedCacheMu.Lock()
				for _, event := range watchResp.Events {
					topic := strings.TrimPrefix(string(event.Kv.Key), retainedPrefix)

					switch event.Type {
					case clientv3.EventTypePut:
						msg, err := message.UnmarshalBinary(event.Kv.Value)
						if err != nil {
							c.logger.Warn("failed to unmarshal retained message", slog.String("error", err.Error()))
							continue
						}
						message.Release(c.retainedCache[topic])
						c.retainedCache[topic] = msg

					case clientv3.EventTypeDelete:
						message.Release(c.retainedCache[topic])
						delete(c.retainedCache, topic)
					}
				}
				c.retainedCacheMu.Unlock()
			}
		}
	restart:
		select {
		case <-c.stopCh:
			return
		case <-time.After(time.Second):
		}
	}
}

// campaignLeader attempts to become the cluster leader.
// Retries on failure until successful or cluster stops.
func (c *EtcdCluster) campaignLeader() {
	// Wait a bit for cluster to form quorum (2 out of 3 nodes)
	// This prevents racing to campaign before the cluster is ready
	time.Sleep(3 * time.Second)

	ctx := c.lifecycleCtx
	retryDelay := 2 * time.Second
	maxRetryDelay := 30 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}
		c.logger.Info("Campaigning for leadership", slog.String("node_id", c.nodeID))

		if err := c.election.Campaign(ctx, c.nodeID); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				return
			}
			c.logger.Warn("Failed to campaign for leader",
				slog.String("node_id", c.nodeID),
				slog.String("error", err.Error()),
				slog.Duration("retry_in", retryDelay))

			// If the error is about lost watcher/session, recreate the session and election
			if strings.Contains(err.Error(), "lost watcher") || strings.Contains(err.Error(), "session") {
				c.logger.Info("Recreating session and election due to session loss")
				if err := c.recreateSessionAndElection(); err != nil {
					c.logger.Error("Failed to recreate session", slog.String("error", err.Error()))
				}
			}

			// Check if cluster is stopping
			select {
			case <-c.stopCh:
				c.logger.Info("Cluster stopping, ending campaign", slog.String("node_id", c.nodeID))
				return
			case <-ctx.Done():
				return
			case <-time.After(retryDelay):
				// Exponential backoff with max cap
				retryDelay *= 2
				if retryDelay > maxRetryDelay {
					retryDelay = maxRetryDelay
				}
				continue
			}
		}

		c.logger.Info("Node became cluster leader", slog.String("node_id", c.nodeID))
		return
	}
}

// recreateSessionAndElection recreates the concurrency session and election
// when the previous session has been lost or expired.
func (c *EtcdCluster) recreateSessionAndElection() error {
	// Close the old session if it exists
	if c.session != nil {
		c.session.Close()
	}

	// Create a new session
	s, err := concurrency.NewSession(c.client, concurrency.WithTTL(10))
	if err != nil {
		return fmt.Errorf("failed to create new session: %w", err)
	}

	// Create a new election with the new session
	election := concurrency.NewElection(s, electionPrefix)

	// Update the cluster's session and election
	c.session = s
	c.election = election

	c.logger.Info("Successfully recreated session and election", slog.String("node_id", c.nodeID))
	return nil
}

func (c *EtcdCluster) refreshSessionLease(ctx context.Context) error {
	c.leaseMu.Lock()
	defer c.leaseMu.Unlock()
	return c.refreshSessionLeaseLocked(ctx)
}

func (c *EtcdCluster) refreshSessionLeaseLocked(ctx context.Context) error {
	leaseResp, err := c.client.Grant(ctx, 30)
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}

	if c.leaseCancel != nil {
		c.leaseCancel()
		c.leaseCancel = nil
	}

	keepAliveCtx, cancel := context.WithCancel(context.Background()) //nolint:contextcheck // intentionally creates new context for lease keep-alive lifecycle independent of caller
	ch, err := c.client.KeepAlive(keepAliveCtx, leaseResp.ID)        //nolint:contextcheck // intentionally creates new context for lease keep-alive lifecycle independent of caller
	if err != nil {
		cancel()
		return fmt.Errorf("failed to keep lease alive: %w", err)
	}

	c.sessionLease = leaseResp.ID
	c.leaseCancel = cancel

	leaseID := leaseResp.ID
	go func() { //nolint:contextcheck // recovery deliberately uses the cluster lifecycle context, not the caller's
		for {
			select {
			case <-c.stopCh:
				return
			case _, ok := <-ch:
				if !ok {
					// The keepalive stream died: either this lease was
					// superseded by a newer one (refresh cancelled our
					// context) or it expired server-side. Only the goroutine
					// watching the current lease may start recovery, so all
					// keys registered under the expired lease are restored.
					c.leaseMu.Lock()
					current := c.sessionLease == leaseID
					c.leaseMu.Unlock()
					if !current {
						return
					}
					select {
					case <-c.stopCh:
						return
					default:
					}
					c.logger.Warn("session lease keepalive lost, starting recovery",
						slog.String("node_id", c.nodeID))
					c.recoverLeaseLoop(leaseID)
					return
				}
			}
		}
	}()

	return nil
}

// recoverLeaseLoop re-grants the session lease and re-registers all leased
// keys, retrying with backoff until it succeeds or the cluster shuts down.
func (c *EtcdCluster) recoverLeaseLoop(expectedLease clientv3.LeaseID) {
	backoff := 500 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for {
		ctx, cancel := context.WithTimeout(c.lifecycleCtx, 10*time.Second)
		err := c.recoverSessionLease(ctx, expectedLease)
		cancel()
		if err == nil {
			return
		}
		expectedLease = c.currentSessionLease()
		c.logger.Error("session lease recovery failed, retrying",
			slog.Duration("backoff", backoff),
			slog.String("error", err.Error()))

		select {
		case <-c.stopCh:
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// recoverSessionLease grants a fresh lease and restores recoverable keys this
// node had attached to the previous lease. Session owners are fenced instead.
func (c *EtcdCluster) recoverSessionLease(ctx context.Context, expectedLease clientv3.LeaseID) error {
	c.leaseRecoveryMu.Lock()
	defer c.leaseRecoveryMu.Unlock()

	c.leaseMu.Lock()
	if c.sessionLease != expectedLease {
		c.leaseMu.Unlock()
		return nil
	}
	lostSessions := c.detachSessionOwnersLocked()
	c.leaseMu.Unlock()

	// Fence old connections before this node can claim sessions under a fresh
	// lease. Queue-consumer registrations retain their existing recovery
	// behavior; session ownership deliberately does not.
	c.notifySessionLeaseLost(ctx, lostSessions)

	c.leaseMu.Lock()
	defer c.leaseMu.Unlock()
	if err := c.refreshSessionLeaseLocked(ctx); err != nil {
		return err
	}
	if err := c.reregisterLeasedKeysLocked(ctx); err != nil {
		return err
	}

	c.logger.Info("session lease recovered",
		slog.String("node_id", c.nodeID),
		slog.Int("reregistered_keys", len(c.leasedKeys)),
		slog.Int("fenced_sessions", len(lostSessions)))
	return nil
}

// detachSessionOwnersLocked removes session-owner claims from the set that is
// automatically restored after a lease loss. Caller must hold leaseMu.
func (c *EtcdCluster) detachSessionOwnersLocked() []string {
	clientIDs := make([]string, 0)
	for key := range c.leasedKeys {
		clientID, ok := parseSessionOwnerKey(key)
		if !ok {
			continue
		}
		delete(c.leasedKeys, key)
		clientIDs = append(clientIDs, clientID)
	}
	return clientIDs
}

func (c *EtcdCluster) notifySessionLeaseLost(ctx context.Context, clientIDs []string) {
	if len(clientIDs) == 0 || c.msgHandler == nil {
		return
	}
	c.msgHandler.HandleSessionLeaseLost(ctx, clientIDs)
}

// reregisterLeasedKeysLocked re-puts all tracked keys under the current
// lease. Caller must hold leaseMu.
func (c *EtcdCluster) reregisterLeasedKeysLocked(ctx context.Context) error {
	var errs []error
	for key, value := range c.leasedKeys {
		if _, isSessionOwner := parseSessionOwnerKey(key); isSessionOwner {
			continue
		}
		if _, err := c.client.Put(ctx, key, value, clientv3.WithLease(c.sessionLease)); err != nil {
			errs = append(errs, fmt.Errorf("re-register %s: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

// prefixWatchOpts returns watch options that resume right after the given
// revision, so events landing between a cache load and the watch registration
// are replayed instead of lost. A compacted revision surfaces as a watch
// error, which the callers handle by reloading the cache and re-watching.
func prefixWatchOpts(rev int64) []clientv3.OpOption {
	opts := []clientv3.OpOption{clientv3.WithPrefix()}
	if rev > 0 {
		opts = append(opts, clientv3.WithRev(rev+1))
	}
	return opts
}

func isLeaseNotFoundErr(err error) bool {
	return err != nil && errors.Is(rpctypes.Error(err), rpctypes.ErrLeaseNotFound)
}

func (c *EtcdCluster) putWithSessionLease(ctx context.Context, key, value string) error {
	c.leaseMu.Lock()
	leaseID := c.sessionLease
	c.leaseMu.Unlock()

	_, err := c.client.Put(ctx, key, value, clientv3.WithLease(leaseID))
	if err == nil {
		c.trackLeasedKey(key, value)
		return nil
	}
	if !isLeaseNotFoundErr(err) {
		return err
	}

	// The lease expired server-side: recover it, restore the queue-consumer
	// registrations attached to it, then retry this Put.
	if err := c.recoverSessionLease(ctx, leaseID); err != nil {
		return err
	}

	c.leaseMu.Lock()
	leaseID = c.sessionLease
	c.leaseMu.Unlock()
	_, err = c.client.Put(ctx, key, value, clientv3.WithLease(leaseID))
	if err == nil {
		c.trackLeasedKey(key, value)
	}
	return err
}

func (c *EtcdCluster) trackLeasedKey(key, value string) {
	c.leaseMu.Lock()
	c.leasedKeys[key] = value
	c.leaseMu.Unlock()
}

func (c *EtcdCluster) untrackLeasedKey(key string) {
	c.leaseMu.Lock()
	delete(c.leasedKeys, key)
	c.leaseMu.Unlock()
}

// AcquireSession registers this node as the owner of a session. A fresh claim
// is a create-only transaction; an existing claim by the same node is renewed
// idempotently. A different owner is never overwritten here.
func (c *EtcdCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
	ownerKey := sessionOwnerKey(clientID)
	takeoverKey := sessionTakeoverKey(clientID)

	for attempt := 0; attempt < 2; attempt++ {
		resp, leaseID, tracked, err := c.tryInitialSessionClaim(ctx, clientID, nodeID, ownerKey, takeoverKey)
		if isLeaseNotFoundErr(err) {
			if recoverErr := c.recoverSessionLease(ctx, leaseID); recoverErr != nil {
				return recoverErr
			}
			continue
		}
		if err != nil {
			return err
		}
		if resp.Succeeded {
			return nil
		}

		owner := txnRangeValue(resp, 0)
		if txnRangeValue(resp, 1) != "" {
			return ErrTakeoverInProgress
		}
		if tracked {
			c.fenceLostSessionClaim(ctx, clientID, ownerKey, nodeID)
			if owner == "" {
				return fmt.Errorf("%w: %s", ErrSessionOwnershipLost, clientID)
			}
			return &SessionOwnedError{ClientID: clientID, Owner: owner}
		}
		if owner != nodeID {
			return &SessionOwnedError{ClientID: clientID, Owner: owner}
		}

		// Reattach this node's idempotent claim to the current lease, but do
		// not cross a takeover that began after the first transaction.
		resp, leaseID, err = c.tryRenewSessionClaim(ctx, clientID, nodeID, ownerKey, takeoverKey)
		if isLeaseNotFoundErr(err) {
			if recoverErr := c.recoverSessionLease(ctx, leaseID); recoverErr != nil {
				return recoverErr
			}
			continue
		}
		if err != nil {
			return err
		}
		if resp.Succeeded {
			return nil
		}
		if txnRangeValue(resp, 1) != "" {
			return ErrTakeoverInProgress
		}
		return &SessionOwnedError{ClientID: clientID, Owner: txnRangeValue(resp, 0)}
	}
	return fmt.Errorf("acquire session %q: lease recovery exhausted", clientID)
}

func (c *EtcdCluster) tryInitialSessionClaim(
	ctx context.Context,
	clientID, nodeID, ownerKey, takeoverKey string,
) (*clientv3.TxnResponse, clientv3.LeaseID, bool, error) {
	c.leaseRecoveryMu.Lock()
	defer c.leaseRecoveryMu.Unlock()

	leaseID := c.currentSessionLease()
	_, tracked := c.getLeasedKey(ownerKey)
	claimCompare := clientv3.Compare(clientv3.Version(ownerKey), "=", 0)
	if tracked {
		// A locally tracked claim may only be renewed while the key still
		// proves this node owns it. Recreating a missing tracked key would
		// resurrect an owner before lease-loss fencing runs.
		claimCompare = clientv3.Compare(clientv3.Value(ownerKey), "=", nodeID)
	}
	resp, err := c.client.Txn(ctx).
		If(claimCompare, clientv3.Compare(clientv3.Version(takeoverKey), "=", 0)).
		Then(clientv3.OpPut(ownerKey, nodeID, clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(ownerKey), clientv3.OpGet(takeoverKey)).
		Commit()
	if err == nil && resp.Succeeded {
		c.recordSessionOwnership(clientID, nodeID, ownerKey)
	}
	return resp, leaseID, tracked, err
}

func (c *EtcdCluster) tryRenewSessionClaim(
	ctx context.Context,
	clientID, nodeID, ownerKey, takeoverKey string,
) (*clientv3.TxnResponse, clientv3.LeaseID, error) {
	c.leaseRecoveryMu.Lock()
	defer c.leaseRecoveryMu.Unlock()

	leaseID := c.currentSessionLease()
	resp, err := c.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(ownerKey), "=", nodeID),
			clientv3.Compare(clientv3.Version(takeoverKey), "=", 0),
		).
		Then(clientv3.OpPut(ownerKey, nodeID, clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(ownerKey), clientv3.OpGet(takeoverKey)).
		Commit()
	if err == nil && resp.Succeeded {
		c.recordSessionOwnership(clientID, nodeID, ownerKey)
	}
	return resp, leaseID, err
}

func (c *EtcdCluster) fenceLostSessionClaim(ctx context.Context, clientID, ownerKey, expectedOwner string) {
	c.untrackLeasedKey(ownerKey)
	c.ownerCacheMu.Lock()
	if c.ownerCache[clientID] == expectedOwner {
		delete(c.ownerCache, clientID)
	}
	c.ownerCacheMu.Unlock()
	c.notifySessionLeaseLost(ctx, []string{clientID})
}

func (c *EtcdCluster) currentSessionLease() clientv3.LeaseID {
	c.leaseMu.Lock()
	defer c.leaseMu.Unlock()
	return c.sessionLease
}

func (c *EtcdCluster) recordSessionOwnership(clientID, nodeID, key string) {
	c.trackLeasedKey(key, nodeID)
	c.ownerCacheMu.Lock()
	c.ownerCache[clientID] = nodeID
	c.ownerCacheMu.Unlock()
}

func txnRangeValue(resp *clientv3.TxnResponse, index int) string {
	if resp == nil || index < 0 || index >= len(resp.Responses) {
		return ""
	}
	rangeResp := resp.Responses[index].GetResponseRange()
	if rangeResp == nil || len(rangeResp.Kvs) == 0 {
		return ""
	}
	return string(rangeResp.Kvs[0].Value)
}

// ReleaseSession releases ownership of a session, only if this node owns it.
func (c *EtcdCluster) ReleaseSession(ctx context.Context, clientID string) error {
	key := sessionOwnerKey(clientID)

	// Untrack before deleting so the watcher does not re-register the key
	// when the delete event arrives.
	c.untrackLeasedKey(key)

	// CAS delete: only delete if we own it
	_, err := c.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", c.nodeID)).
		Then(clientv3.OpDelete(key)).
		Commit()

	c.ownerCacheMu.Lock()
	// A concurrent takeover can replace the cache entry before this CAS
	// completes. Only evict the local claim; never erase the new owner.
	if c.ownerCache[clientID] == c.nodeID {
		delete(c.ownerCache, clientID)
	}
	c.ownerCacheMu.Unlock()

	return err
}

// GetSessionOwner returns the authoritative node ID that owns the session.
// Connection admission cannot use the routing cache because a delayed watch
// event could otherwise initiate takeover from a stale owner.
func (c *EtcdCluster) GetSessionOwner(ctx context.Context, clientID string) (string, bool, error) {
	key := sessionOwnerKey(clientID)

	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return "", false, err
	}

	if len(resp.Kvs) == 0 {
		return "", false, nil
	}

	owner := string(resp.Kvs[0].Value)
	return owner, true, nil
}

// WatchSessionOwner watches for ownership changes of a specific session.
func (c *EtcdCluster) WatchSessionOwner(ctx context.Context, clientID string) <-chan OwnershipChange {
	key := sessionOwnerKey(clientID)
	ch := make(chan OwnershipChange, 1)

	watchCh := c.client.Watch(ctx, key)

	go func() {
		defer close(ch)
		for resp := range watchCh {
			for _, ev := range resp.Events {
				var change OwnershipChange
				change.ClientID = clientID
				change.Time = time.Now()

				if ev.Type == clientv3.EventTypeDelete {
					if ev.PrevKv != nil {
						change.OldNode = string(ev.PrevKv.Value)
					}
					change.NewNode = ""
				} else {
					if ev.PrevKv != nil {
						change.OldNode = string(ev.PrevKv.Value)
					}
					change.NewNode = string(ev.Kv.Value)
				}

				ch <- change
			}
		}
	}()

	return ch
}

// AddSubscription adds a subscription to the cluster store.
// Uses read-modify-write with CAS to consolidate all client subscriptions in a single key.
func (c *EtcdCluster) AddSubscription(ctx context.Context, clientID, filter string, qos byte, opts storage.SubscribeOptions) error {
	key := subscriptionsPrefix + clientID

	newSub := storage.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      qos,
		Options:  opts,
	}

	for {
		resp, err := c.client.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get subscriptions: %w", err)
		}

		var subs []storage.Subscription
		var modRev int64
		if len(resp.Kvs) > 0 {
			modRev = resp.Kvs[0].ModRevision
			if err := json.Unmarshal(resp.Kvs[0].Value, &subs); err != nil {
				return fmt.Errorf("failed to unmarshal subscriptions: %w", err)
			}
		}

		replaced := false
		for i, s := range subs {
			if s.Filter == filter {
				subs[i] = newSub
				replaced = true
				break
			}
		}
		if !replaced {
			subs = append(subs, newSub)
		}

		data, err := json.Marshal(subs)
		if err != nil {
			return fmt.Errorf("failed to marshal subscriptions: %w", err)
		}

		var cmp clientv3.Cmp
		if modRev == 0 {
			cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		} else {
			cmp = clientv3.Compare(clientv3.ModRevision(key), "=", modRev)
		}

		txnResp, err := c.client.Txn(ctx).
			If(cmp).
			Then(clientv3.OpPut(key, string(data))).
			Commit()
		if err != nil {
			return fmt.Errorf("failed to commit subscription: %w", err)
		}
		if txnResp.Succeeded {
			return nil
		}
	}
}

// RemoveSubscription removes a subscription from the cluster store.
// Uses read-modify-write with CAS. Deletes the key if no subscriptions remain.
func (c *EtcdCluster) RemoveSubscription(ctx context.Context, clientID, filter string) error {
	key := subscriptionsPrefix + clientID

	for {
		resp, err := c.client.Get(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get subscriptions: %w", err)
		}
		if len(resp.Kvs) == 0 {
			return nil
		}

		modRev := resp.Kvs[0].ModRevision
		var subs []storage.Subscription
		if err := json.Unmarshal(resp.Kvs[0].Value, &subs); err != nil {
			return fmt.Errorf("failed to unmarshal subscriptions: %w", err)
		}

		idx := -1
		for i, s := range subs {
			if s.Filter == filter {
				idx = i
				break
			}
		}
		if idx == -1 {
			return nil
		}

		subs = append(subs[:idx], subs[idx+1:]...)

		cmp := clientv3.Compare(clientv3.ModRevision(key), "=", modRev)

		var op clientv3.Op
		if len(subs) == 0 {
			op = clientv3.OpDelete(key)
		} else {
			data, err := json.Marshal(subs)
			if err != nil {
				return fmt.Errorf("failed to marshal subscriptions: %w", err)
			}
			op = clientv3.OpPut(key, string(data))
		}

		txnResp, err := c.client.Txn(ctx).
			If(cmp).
			Then(op).
			Commit()
		if err != nil {
			return fmt.Errorf("failed to commit subscription removal: %w", err)
		}
		if txnResp.Succeeded {
			return nil
		}
	}
}

// RemoveAllSubscriptions removes all subscriptions for a client in a single DELETE.
func (c *EtcdCluster) RemoveAllSubscriptions(ctx context.Context, clientID string) error {
	key := subscriptionsPrefix + clientID
	_, err := c.client.Delete(ctx, key)
	return err
}

// GetSubscriptionsForClient returns all subscriptions for a client.
func (c *EtcdCluster) GetSubscriptionsForClient(ctx context.Context, clientID string) ([]*storage.Subscription, error) {
	key := subscriptionsPrefix + clientID

	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var subs []storage.Subscription
	if err := json.Unmarshal(resp.Kvs[0].Value, &subs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal subscriptions: %w", err)
	}

	result := make([]*storage.Subscription, len(subs))
	for i := range subs {
		result[i] = &subs[i]
	}
	return result, nil
}

// GetSubscribersForTopic returns all subscriptions matching a topic.
// Optimized: uses local cache for fast lookup.
func (c *EtcdCluster) GetSubscribersForTopic(ctx context.Context, topic string) ([]*storage.Subscription, error) {
	c.subCacheMu.RLock()
	subTrie := c.subTrie
	c.subCacheMu.RUnlock()
	if subTrie == nil {
		return nil, nil
	}

	return subTrie.Match(topic)
}

// Retained returns the cluster-wide retained message store.
func (c *EtcdCluster) Retained() storage.RetainedStore {
	// Return hybrid retained store if available, otherwise fall back to old implementation
	if c.hybridRetained != nil {
		return c.hybridRetained
	}
	return &etcdRetainedStore{logger: c.logger, client: c.client, cluster: c}
}

// Wills returns the cluster-wide will message store.
func (c *EtcdCluster) Wills() storage.WillStore {
	// Return hybrid will store if available, otherwise fall back to old implementation
	if c.hybridWill != nil {
		return c.hybridWill
	}
	return &etcdWillStore{logger: c.logger, client: c.client}
}

// etcdRetainedStore implements storage.RetainedStore using etcd.
type etcdRetainedStore struct {
	logger  *slog.Logger
	client  *clientv3.Client
	cluster *EtcdCluster
}

func (s *etcdRetainedStore) Set(ctx context.Context, topic string, msg *message.Envelope) error {
	key := retainedPrefix + topic

	// Empty payload means delete
	if len(msg.PayloadBytes()) == 0 {
		return s.Delete(ctx, topic)
	}

	data, err := message.MarshalBinary(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal retained message: %w", err)
	}

	_, err = s.client.Put(ctx, key, string(data))
	return err
}

func (s *etcdRetainedStore) Get(ctx context.Context, topic string) (*message.Envelope, error) {
	key := retainedPrefix + topic

	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, storage.ErrNotFound
	}

	msg, err := message.UnmarshalBinary(resp.Kvs[0].Value)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal retained message: %w", err)
	}

	return msg, nil
}

func (s *etcdRetainedStore) Delete(ctx context.Context, topic string) error {
	key := retainedPrefix + topic
	_, err := s.client.Delete(ctx, key)
	return err
}

func (s *etcdRetainedStore) Match(ctx context.Context, filter string) ([]*message.Envelope, error) {
	// Use local cache for fast wildcard matching instead of etcd scan
	s.cluster.retainedCacheMu.RLock()
	defer s.cluster.retainedCacheMu.RUnlock()

	var matched []*message.Envelope
	for topic, msg := range s.cluster.retainedCache {
		if topicMatchesFilter(topic, filter) {
			// Create a copy to avoid returning cached pointers
			matched = append(matched, msg.Clone())
		}
	}

	return matched, nil
}

// etcdWillStore implements storage.WillStore using etcd.
type etcdWillStore struct {
	logger *slog.Logger
	client *clientv3.Client
}

type etcdWillEntry struct {
	Will           *storage.WillMessage `json:"will"`
	DisconnectedAt time.Time            `json:"disconnected_at"`
}

func (s *etcdWillStore) Set(ctx context.Context, clientID string, will *storage.WillMessage) error {
	key := willPrefix + clientID

	entry := etcdWillEntry{
		Will:           will,
		DisconnectedAt: time.Now(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal will message: %w", err)
	}

	_, err = s.client.Put(ctx, key, string(data))
	return err
}

func (s *etcdWillStore) Get(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	key := willPrefix + clientID

	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, storage.ErrNotFound
	}

	var entry etcdWillEntry
	if err := json.Unmarshal(resp.Kvs[0].Value, &entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal will message: %w", err)
	}

	return entry.Will, nil
}

func (s *etcdWillStore) Delete(ctx context.Context, clientID string) error {
	key := willPrefix + clientID
	_, err := s.client.Delete(ctx, key)
	return err
}

func (s *etcdWillStore) GetPending(ctx context.Context, before time.Time) ([]*storage.WillMessage, error) {
	resp, err := s.client.Get(ctx, willPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var pending []*storage.WillMessage
	for _, kv := range resp.Kvs {
		var entry etcdWillEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			s.logger.Warn("failed to unmarshal will entry", slog.String("error", err.Error()))
			continue
		}

		if !entry.DisconnectedAt.IsZero() {
			triggerTime := entry.DisconnectedAt.Add(time.Duration(entry.Will.Delay) * time.Second)
			if triggerTime.Before(before) || triggerTime.Equal(before) {
				pending = append(pending, entry.Will)
			}
		}
	}

	return pending, nil
}

// SetMessageHandler sets the handler for incoming routed messages and session management.
func (c *EtcdCluster) SetMessageHandler(handler MessageHandler) {
	c.msgHandler = handler
}

// SetForwardPublishHandler sets the handler for topic-based forward publish RPCs
// and initializes the forward batcher for batching outbound ForwardPublish messages.
func (c *EtcdCluster) SetForwardPublishHandler(handler ForwardPublishHandler) {
	if c.transport != nil {
		c.transport.SetForwardPublishHandler(handler)

		c.forwardBatcher = newNodeBatcher(
			c.routeBatchMaxSize,
			c.routeBatchMaxDelay,
			c.routeBatchFlushWorkers,
			c.stopCh,
			c.logger.With(slog.String("batcher", "forward-publish")),
			"forward-publish",
			func(ctx context.Context, nodeID string, items []*clusterv1.ForwardPublishRequest) error {
				return c.transport.SendForwardPublishBatch(ctx, nodeID, items)
			},
		)
	}
}

// RoutePublish routes a publish to interested nodes with matching subscriptions.
// It sends one ForwardPublishRequest per remote node (topic-based fan-out).
// The receiving node performs its own local subscription match and delivery.
// It borrows msg for the duration of the call.
func (c *EtcdCluster) RoutePublish(ctx context.Context, msg *message.Envelope) error {
	if c.transport == nil {
		return nil
	}
	if msg == nil {
		return errEmptyEnvelope
	}
	topic := msg.Topic

	// Match cluster trie to find any remote subscribers
	subs, err := c.GetSubscribersForTopic(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to get subscribers: %w", err)
	}

	// Collect unique remote node IDs
	remoteNodes := make(map[string]struct{}, len(subs))
	c.ownerCacheMu.RLock()
	var cacheMisses map[string]struct{}
	for _, sub := range subs {
		nodeID, ok := c.ownerCache[sub.ClientID]
		if !ok {
			if cacheMisses == nil {
				cacheMisses = make(map[string]struct{})
			}
			cacheMisses[sub.ClientID] = struct{}{}
			continue
		}
		if nodeID != c.nodeID {
			remoteNodes[nodeID] = struct{}{}
		}
	}
	c.ownerCacheMu.RUnlock()

	// Fallback to etcd for cache misses
	unknownOwners := 0
	if len(cacheMisses) > 0 {
		for clientID := range cacheMisses {
			if ctx.Err() != nil {
				break
			}
			nodeID, _, err := c.GetSessionOwner(ctx, clientID)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					break
				}
				unknownOwners++
				continue
			}
			if nodeID == "" {
				unknownOwners++
				continue
			}
			if nodeID == c.nodeID {
				continue
			}
			remoteNodes[nodeID] = struct{}{}
		}
	}
	if unknownOwners > 0 {
		c.warnUnknownOwners(topic, unknownOwners)
	}

	if len(remoteNodes) == 0 {
		return nil
	}

	// Encoding produces a private copy of the envelope, so the async QoS0 batch
	// path needs no separate snapshot: RoutePublish may return, and the caller
	// release its buffer, before the worker flushes.
	encoded, err := encodeEnvelope(msg)
	if err != nil {
		return err
	}
	qos := msg.BrokerMeta.Delivery.QoS

	// Send one ForwardPublish per remote node
	req := &clusterv1.ForwardPublishRequest{Envelope: encoded}

	var errs []error
	for nodeID := range remoteNodes {
		var err error
		if c.forwardBatcher != nil {
			if qos == 0 {
				err = c.forwardBatcher.EnqueueAsync(ctx, nodeID, []*clusterv1.ForwardPublishRequest{req})
			} else {
				err = c.forwardBatcher.Enqueue(ctx, nodeID, []*clusterv1.ForwardPublishRequest{req})
			}
		} else {
			err = c.transport.SendForwardPublishBatch(ctx, nodeID, []*clusterv1.ForwardPublishRequest{req})
		}
		if err != nil {
			c.logger.Warn("failed to forward publish",
				slog.String("node_id", nodeID),
				slog.String("topic", topic),
				slog.String("error", err.Error()))
			errs = append(errs, fmt.Errorf("forward publish to node %s failed: %w", nodeID, err))
		}
	}

	return errors.Join(errs...)
}

// warnUnknownOwners logs (at most once per 10s) that subscribers matched a
// topic but their owning node is unknown, so no forward target exists. This
// is expected for stale subscription entries of long-gone clients, but a
// sustained burst means live cross-node subscribers are losing messages.
func (c *EtcdCluster) warnUnknownOwners(topic string, count int) {
	const throttle = int64(10 * time.Second)
	now := time.Now().UnixNano()
	last := c.lastUnknownOwnerWarn.Load()
	if now-last < throttle || !c.lastUnknownOwnerWarn.CompareAndSwap(last, now) {
		return
	}
	c.logger.Warn("skipped subscribers with unknown session owner during cross-node routing",
		slog.String("topic", topic),
		slog.Int("skipped", count))
}

// TakeoverSession initiates session takeover from one node to another.
func (c *EtcdCluster) TakeoverSession(ctx context.Context, clientID, fromNode, toNode string, identity *SessionIdentityGuard) (*clusterv1.SessionState, error) {
	if fromNode == toNode {
		return nil, nil
	}
	if c.transport == nil {
		return nil, ErrTransportNotConfigured
	}
	if toNode != c.nodeID {
		return nil, fmt.Errorf("takeover target %q is not local node %q", toNode, c.nodeID)
	}

	lockKey := sessionTakeoverKey(clientID)
	token := fmt.Sprintf("%s:%d", toNode, time.Now().UnixNano())
	if err := c.acquireTakeoverLock(ctx, clientID, fromNode, lockKey, token); err != nil {
		return nil, err
	}
	lockHeld := true
	defer func() {
		if lockHeld {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer cancel()
			c.releaseTakeoverLock(cleanupCtx, lockKey, token)
		}
	}()

	state, err := c.transport.SendTakeover(ctx, fromNode, clientID, fromNode, toNode, identity)
	if err != nil {
		return nil, fmt.Errorf("failed to request takeover from %s: %w", fromNode, err)
	}
	if err := c.finalizeTakeover(ctx, clientID, fromNode, toNode, lockKey, token); err != nil {
		return nil, fmt.Errorf("failed to finalize session takeover: %w", err)
	}
	lockHeld = false

	c.logger.Info("session taken over",
		slog.String("client_id", clientID),
		slog.String("from_node", fromNode),
		slog.String("to_node", toNode))
	return state, nil
}

func (c *EtcdCluster) acquireTakeoverLock(ctx context.Context, clientID, fromNode, lockKey, token string) error {
	c.leaseRecoveryMu.Lock()
	defer c.leaseRecoveryMu.Unlock()

	ownerKey := sessionOwnerKey(clientID)
	resp, err := c.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(ownerKey), "=", fromNode),
			clientv3.Compare(clientv3.Version(lockKey), "=", 0),
		).
		Then(clientv3.OpPut(lockKey, token, clientv3.WithLease(c.currentSessionLease()))).
		Else(clientv3.OpGet(ownerKey), clientv3.OpGet(lockKey)).
		Commit()
	if err != nil {
		return err
	}
	if resp.Succeeded {
		return nil
	}
	if txnRangeValue(resp, 1) != "" {
		return ErrTakeoverInProgress
	}
	return &SessionOwnedError{ClientID: clientID, Owner: txnRangeValue(resp, 0)}
}

func (c *EtcdCluster) finalizeTakeover(ctx context.Context, clientID, fromNode, toNode, lockKey, token string) error {
	c.leaseRecoveryMu.Lock()
	defer c.leaseRecoveryMu.Unlock()

	ownerKey := sessionOwnerKey(clientID)
	leaseID := c.currentSessionLease()
	resp, err := c.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(ownerKey), "=", fromNode),
			clientv3.Compare(clientv3.Value(lockKey), "=", token),
		).
		Then(
			clientv3.OpPut(ownerKey, toNode, clientv3.WithLease(leaseID)),
			clientv3.OpDelete(lockKey),
		).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		// The source owner's lease may expire after it handed over state. The
		// takeover lock is on this node's lease, so an absent owner can still
		// be claimed safely by the holder of that exact lock token.
		resp, err = c.client.Txn(ctx).
			If(
				clientv3.Compare(clientv3.Version(ownerKey), "=", 0),
				clientv3.Compare(clientv3.Value(lockKey), "=", token),
			).
			Then(
				clientv3.OpPut(ownerKey, toNode, clientv3.WithLease(leaseID)),
				clientv3.OpDelete(lockKey),
			).
			Else(clientv3.OpGet(ownerKey), clientv3.OpGet(lockKey)).
			Commit()
		if err != nil {
			return err
		}
		if !resp.Succeeded {
			if txnRangeValue(resp, 1) != token {
				return ErrTakeoverInProgress
			}
			return &SessionOwnedError{ClientID: clientID, Owner: txnRangeValue(resp, 0)}
		}
	}

	c.recordSessionOwnership(clientID, toNode, ownerKey)
	return nil
}

func (c *EtcdCluster) releaseTakeoverLock(ctx context.Context, lockKey, token string) {
	_, err := c.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(lockKey), "=", token)).
		Then(clientv3.OpDelete(lockKey)).
		Commit()
	if err != nil {
		c.logger.Warn("failed to release session takeover lock",
			slog.String("key", lockKey),
			slog.String("error", err.Error()))
	}
}

// EnqueueRemote sends an enqueue request to a remote node. It borrows msg for
// the duration of the call.
func (c *EtcdCluster) EnqueueRemote(ctx context.Context, nodeID, queueName string, msg *message.Envelope) error {
	if c.transport == nil {
		return ErrTransportNotConfigured
	}
	return c.transport.SendEnqueueRemote(ctx, nodeID, queueName, msg, nil, false, false)
}

// RouteQueueMessage sends a queue message to a remote consumer.
func (c *EtcdCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID string, msg *message.Envelope) error {
	if c.transport == nil {
		return ErrTransportNotConfigured
	}
	return c.transport.SendRouteQueueMessage(ctx, nodeID, clientID, msg)
}

// RouteQueueBatch sends multiple queue messages to a remote node.
func (c *EtcdCluster) RouteQueueBatch(ctx context.Context, nodeID string, deliveries []QueueDelivery) error {
	if c.transport == nil {
		return ErrTransportNotConfigured
	}
	if c.queueBatcher != nil {
		return c.queueBatcher.Enqueue(ctx, nodeID, deliveries)
	}
	return c.transport.SendRouteQueueBatch(ctx, nodeID, deliveries)
}

// SetQueueHandler sets the queue handler for queue distribution operations.
// This should be called after the queue manager is created to enable queue RPC handling.
func (c *EtcdCluster) SetQueueHandler(handler QueueHandler) {
	if c.transport != nil {
		c.transport.SetQueueHandler(handler)
	}
}

// These methods allow EtcdCluster to implement the MessageHandler interface
// by delegating to the broker's handler.

// DeliverToClient implements MessageHandler.DeliverToClient.
// Delegates to the broker to deliver a message to a local client.
func (c *EtcdCluster) DeliverToClient(ctx context.Context, clientID string, msg *message.Envelope) error {
	if c.msgHandler == nil {
		return ErrNoMessageHandlerConfigured
	}
	return c.msgHandler.DeliverToClient(ctx, clientID, msg)
}

// GetSessionStateAndClose implements MessageHandler.GetSessionStateAndClose.
// Delegates to the broker to capture session state and close the session.
func (c *EtcdCluster) GetSessionStateAndClose(ctx context.Context, clientID string, identity *SessionIdentityGuard) (*clusterv1.SessionState, error) {
	if c.msgHandler == nil {
		return nil, ErrNoMessageHandlerConfigured
	}
	return c.msgHandler.GetSessionStateAndClose(ctx, clientID, identity)
}

// GetRetainedMessage implements MessageHandler.GetRetainedMessage.
// Fetches a retained message from the local BadgerDB store.
func (c *EtcdCluster) GetRetainedMessage(ctx context.Context, topic string) (*message.Envelope, error) {
	if c.localStore == nil {
		return nil, ErrNoLocalStoreConfigured
	}
	return c.localStore.Retained().Get(ctx, topic)
}

// GetWillMessage implements MessageHandler.GetWillMessage.
// Fetches a will message from the local BadgerDB store.
func (c *EtcdCluster) GetWillMessage(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	if c.localStore == nil {
		return nil, ErrNoLocalStoreConfigured
	}
	return c.localStore.Wills().Get(ctx, clientID)
}

// HandlePublish implements TransportHandler.HandlePublish.
// Called when another broker routes a PUBLISH message to this node. It takes
// ownership of msg on every return path.
func (c *EtcdCluster) HandlePublish(ctx context.Context, clientID string, msg *message.Envelope) error {
	if c.msgHandler == nil {
		message.Release(msg)
		return ErrNoMessageHandlerConfigured
	}

	return c.msgHandler.DeliverToClient(ctx, clientID, msg)
}

// --- Queue Consumer Registry ---

// RegisterQueueConsumer registers a queue consumer visible to all nodes.
func (c *EtcdCluster) RegisterQueueConsumer(ctx context.Context, info *QueueConsumerInfo) error {
	// Key format: /mqtt/queue-consumers/{queueName}/{groupID}/{consumerID}
	key := fmt.Sprintf("%s%s/%s/%s", queueConsumersPrefix, info.QueueName, info.GroupID, info.ConsumerID)

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal consumer info: %w", err)
	}

	if err := c.putWithSessionLease(ctx, key, string(data)); err != nil {
		return fmt.Errorf("failed to store consumer in etcd: %w", err)
	}
	c.upsertQueueConsumerCache(info)

	c.logger.Debug("registered queue consumer in cluster",
		slog.String("queue", info.QueueName),
		slog.String("group", info.GroupID),
		slog.String("consumer", info.ConsumerID),
		slog.String("node", info.ProxyNodeID))

	return nil
}

// UnregisterQueueConsumer removes a queue consumer registration.
func (c *EtcdCluster) UnregisterQueueConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	key := fmt.Sprintf("%s%s/%s/%s", queueConsumersPrefix, queueName, groupID, consumerID)

	c.untrackLeasedKey(key)

	_, err := c.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete consumer from etcd: %w", err)
	}
	c.removeQueueConsumerCache(queueName, groupID, consumerID)

	c.logger.Debug("unregistered queue consumer from cluster",
		slog.String("queue", queueName),
		slog.String("group", groupID),
		slog.String("consumer", consumerID))

	return nil
}

func cloneQueueConsumerInfo(info *QueueConsumerInfo) *QueueConsumerInfo {
	if info == nil {
		return nil
	}
	copy := *info
	return &copy
}

func queueConsumerCacheKey(queueName, groupID, consumerID string) string {
	return queueName + "\x1f" + groupID + "\x1f" + consumerID
}

func parseQueueConsumerKey(key string) (queueName, groupID, consumerID string, ok bool) {
	trimmed := strings.TrimPrefix(key, queueConsumersPrefix)
	if trimmed == key || trimmed == "" {
		return "", "", "", false
	}

	firstSep := strings.Index(trimmed, "/")
	lastSep := strings.LastIndex(trimmed, "/")
	if firstSep <= 0 || lastSep <= firstSep || lastSep >= len(trimmed)-1 {
		return "", "", "", false
	}

	return trimmed[:firstSep], trimmed[firstSep+1 : lastSep], trimmed[lastSep+1:], true
}

func (c *EtcdCluster) upsertQueueConsumerCache(info *QueueConsumerInfo) {
	if info == nil {
		return
	}

	consumerCopy := *info
	cacheKey := queueConsumerCacheKey(consumerCopy.QueueName, consumerCopy.GroupID, consumerCopy.ConsumerID)

	c.queueConsumersCacheMu.Lock()
	defer c.queueConsumersCacheMu.Unlock()

	c.queueConsumersAll[cacheKey] = &consumerCopy

	byQueue := c.queueConsumersByQueue[consumerCopy.QueueName]
	if byQueue == nil {
		byQueue = make(map[string]*QueueConsumerInfo)
		c.queueConsumersByQueue[consumerCopy.QueueName] = byQueue
	}
	byQueue[cacheKey] = &consumerCopy

	byGroup := c.queueConsumersByGroup[consumerCopy.QueueName]
	if byGroup == nil {
		byGroup = make(map[string]map[string]*QueueConsumerInfo)
		c.queueConsumersByGroup[consumerCopy.QueueName] = byGroup
	}
	groupConsumers := byGroup[consumerCopy.GroupID]
	if groupConsumers == nil {
		groupConsumers = make(map[string]*QueueConsumerInfo)
		byGroup[consumerCopy.GroupID] = groupConsumers
	}
	groupConsumers[cacheKey] = &consumerCopy
}

func (c *EtcdCluster) removeQueueConsumerCache(queueName, groupID, consumerID string) {
	cacheKey := queueConsumerCacheKey(queueName, groupID, consumerID)

	c.queueConsumersCacheMu.Lock()
	defer c.queueConsumersCacheMu.Unlock()

	delete(c.queueConsumersAll, cacheKey)

	if byQueue, ok := c.queueConsumersByQueue[queueName]; ok {
		delete(byQueue, cacheKey)
		if len(byQueue) == 0 {
			delete(c.queueConsumersByQueue, queueName)
		}
	}

	if byGroup, ok := c.queueConsumersByGroup[queueName]; ok {
		if groupConsumers, ok := byGroup[groupID]; ok {
			delete(groupConsumers, cacheKey)
			if len(groupConsumers) == 0 {
				delete(byGroup, groupID)
			}
		}
		if len(byGroup) == 0 {
			delete(c.queueConsumersByGroup, queueName)
		}
	}
}

func (c *EtcdCluster) loadQueueConsumerCache() error {
	ctx := context.Background()
	resp, err := c.client.Get(ctx, queueConsumersPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to load queue consumers: %w", err)
	}

	freshAll := make(map[string]*QueueConsumerInfo)
	freshByQueue := make(map[string]map[string]*QueueConsumerInfo)
	freshByGroup := make(map[string]map[string]map[string]*QueueConsumerInfo)

	for _, kv := range resp.Kvs {
		var info QueueConsumerInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			c.logger.Warn("failed to unmarshal queue consumer info during cache load",
				slog.String("key", string(kv.Key)),
				slog.String("error", err.Error()))
			continue
		}

		cacheKey := queueConsumerCacheKey(info.QueueName, info.GroupID, info.ConsumerID)
		infoPtr := new(QueueConsumerInfo)
		*infoPtr = info
		freshAll[cacheKey] = infoPtr

		byQueue := freshByQueue[info.QueueName]
		if byQueue == nil {
			byQueue = make(map[string]*QueueConsumerInfo)
			freshByQueue[info.QueueName] = byQueue
		}
		byQueue[cacheKey] = infoPtr

		byGroup := freshByGroup[info.QueueName]
		if byGroup == nil {
			byGroup = make(map[string]map[string]*QueueConsumerInfo)
			freshByGroup[info.QueueName] = byGroup
		}
		groupConsumers := byGroup[info.GroupID]
		if groupConsumers == nil {
			groupConsumers = make(map[string]*QueueConsumerInfo)
			byGroup[info.GroupID] = groupConsumers
		}
		groupConsumers[cacheKey] = infoPtr
	}

	c.queueConsumersCacheMu.Lock()
	c.queueConsumersAll = freshAll
	c.queueConsumersByQueue = freshByQueue
	c.queueConsumersByGroup = freshByGroup
	c.queueConsumersCacheRev = resp.Header.Revision
	c.queueConsumersCacheMu.Unlock()

	c.logger.Info("loaded queue consumers into cache", slog.Int("cache_len", len(freshAll)))
	return nil
}

func (c *EtcdCluster) watchQueueConsumers() {
	for {
		c.queueConsumersCacheMu.RLock()
		rev := c.queueConsumersCacheRev
		c.queueConsumersCacheMu.RUnlock()
		watchCh := c.client.Watch(c.lifecycleCtx, queueConsumersPrefix, prefixWatchOpts(rev)...)

		for {
			select {
			case <-c.stopCh:
				return
			case watchResp, ok := <-watchCh:
				if !ok {
					if c.lifecycleCtx.Err() != nil {
						return
					}
					c.logger.Warn("queue consumer watch channel closed, reloading cache")
					if err := c.loadQueueConsumerCache(); err != nil {
						c.logger.Error("failed to reload queue consumer cache", slog.String("error", err.Error()))
					}
					goto restart
				}
				if watchResp.Err() != nil {
					c.logger.Error("queue consumer watch error", slog.String("error", watchResp.Err().Error()))
					if err := c.loadQueueConsumerCache(); err != nil {
						c.logger.Error("failed to reload queue consumer cache", slog.String("error", err.Error()))
					}
					goto restart
				}

				for _, event := range watchResp.Events {
					switch event.Type {
					case clientv3.EventTypePut:
						var info QueueConsumerInfo
						if err := json.Unmarshal(event.Kv.Value, &info); err != nil {
							c.logger.Warn("failed to unmarshal queue consumer info in watch",
								slog.String("key", string(event.Kv.Key)),
								slog.String("error", err.Error()))
							continue
						}
						c.upsertQueueConsumerCache(&info)
					case clientv3.EventTypeDelete:
						queueName, groupID, consumerID, ok := parseQueueConsumerKey(string(event.Kv.Key))
						if !ok {
							c.logger.Warn("failed to parse queue consumer key in watch",
								slog.String("key", string(event.Kv.Key)))
							continue
						}
						c.removeQueueConsumerCache(queueName, groupID, consumerID)
					}
				}
			}
		}
	restart:
		select {
		case <-c.stopCh:
			return
		case <-time.After(time.Second):
		}
	}
}

// ListQueueConsumers returns all consumers for a queue across all nodes.
func (c *EtcdCluster) ListQueueConsumers(ctx context.Context, queueName string) ([]*QueueConsumerInfo, error) {
	c.queueConsumersCacheMu.RLock()
	defer c.queueConsumersCacheMu.RUnlock()

	byQueue, ok := c.queueConsumersByQueue[queueName]
	if !ok || len(byQueue) == 0 {
		return nil, nil
	}

	consumers := make([]*QueueConsumerInfo, 0, len(byQueue))
	for _, info := range byQueue {
		consumers = append(consumers, cloneQueueConsumerInfo(info))
	}

	return consumers, nil
}

// ListQueueConsumersByGroup returns all consumers for a specific group.
func (c *EtcdCluster) ListQueueConsumersByGroup(ctx context.Context, queueName, groupID string) ([]*QueueConsumerInfo, error) {
	c.queueConsumersCacheMu.RLock()
	defer c.queueConsumersCacheMu.RUnlock()

	byQueue, ok := c.queueConsumersByGroup[queueName]
	if !ok {
		return nil, nil
	}
	byGroup, ok := byQueue[groupID]
	if !ok || len(byGroup) == 0 {
		return nil, nil
	}

	consumers := make([]*QueueConsumerInfo, 0, len(byGroup))
	for _, info := range byGroup {
		consumers = append(consumers, cloneQueueConsumerInfo(info))
	}

	return consumers, nil
}

// ListAllQueueConsumers returns all queue consumers across all queues.
func (c *EtcdCluster) ListAllQueueConsumers(ctx context.Context) ([]*QueueConsumerInfo, error) {
	c.queueConsumersCacheMu.RLock()
	defer c.queueConsumersCacheMu.RUnlock()

	if len(c.queueConsumersAll) == 0 {
		return nil, nil
	}

	consumers := make([]*QueueConsumerInfo, 0, len(c.queueConsumersAll))
	for _, info := range c.queueConsumersAll {
		consumers = append(consumers, cloneQueueConsumerInfo(info))
	}

	return consumers, nil
}

// ForwardQueuePublish forwards a queue publish to a remote node. It borrows msg
// for the duration of the call. A forwarded publish routes by the envelope's
// topic, so it names no queue.
func (c *EtcdCluster) ForwardQueuePublish(
	ctx context.Context, nodeID string, msg *message.Envelope, targetQueues []string, forwardToLeader bool,
) error {
	if c.transport == nil {
		return ErrTransportNotConfigured
	}

	return c.transport.SendEnqueueRemote(ctx, nodeID, "", msg, targetQueues, true, forwardToLeader)
}

// ForwardGroupOp forwards a consumer group operation to a remote node.
func (c *EtcdCluster) ForwardGroupOp(ctx context.Context, nodeID, queueName string, op *clusterv1.GroupOperation) error {
	if c.transport == nil {
		return ErrTransportNotConfigured
	}

	return c.transport.SendForwardGroupOp(ctx, nodeID, queueName, op)
}

// HandleTakeover implements TransportHandler.HandleTakeover.
// Called when another broker requests to take over a session from this node.
func (c *EtcdCluster) HandleTakeover(ctx context.Context, clientID, fromNode, toNode string, state *clusterv1.SessionState) (*clusterv1.SessionState, error) {
	// Verify this is the node being asked to give up the session
	if fromNode != c.nodeID {
		return nil, fmt.Errorf("takeover request for wrong node: expected %s, got %s", c.nodeID, fromNode)
	}

	// Check if we have a message handler
	if c.msgHandler == nil {
		return nil, ErrNoMessageHandlerConfigured
	}

	// Get session state and close the session
	sessionState, err := c.msgHandler.GetSessionStateAndClose(ctx, clientID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get session state: %w", err)
	}

	c.logger.Info("session handed over",
		slog.String("client_id", clientID),
		slog.String("from_node", fromNode),
		slog.String("to_node", toNode))
	return sessionState, nil
}

// HandleSessionLeaseLost delegates fencing to the broker message handler.
func (c *EtcdCluster) HandleSessionLeaseLost(ctx context.Context, clientIDs []string) {
	c.notifySessionLeaseLost(ctx, clientIDs)
}

func sessionOwnerKey(clientID string) string {
	return sessionsPrefix + clientID + "/owner"
}

func sessionTakeoverKey(clientID string) string {
	return sessionsPrefix + clientID + "/takeover"
}

func parseSessionOwnerKey(key string) (clientID string, ok bool) {
	if !strings.HasPrefix(key, sessionsPrefix) || !strings.HasSuffix(key, "/owner") {
		return "", false
	}
	clientID = strings.TrimPrefix(key, sessionsPrefix)
	clientID = strings.TrimSuffix(clientID, "/owner")
	if clientID == "" {
		return "", false
	}
	return clientID, true
}

// loadSessionOwnerCache loads all session owners from etcd into the local cache.
func (c *EtcdCluster) loadSessionOwnerCache() error {
	ctx := context.Background()
	resp, err := c.client.Get(ctx, sessionsPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to load session owners: %w", err)
	}

	fresh := make(map[string]string)
	for _, kv := range resp.Kvs {
		clientID, ok := parseSessionOwnerKey(string(kv.Key))
		if !ok {
			continue
		}
		fresh[clientID] = string(kv.Value)
	}

	c.ownerCacheMu.Lock()
	prevSize := len(c.ownerCache)
	c.ownerCache = fresh
	c.ownerCacheRev = resp.Header.Revision
	c.ownerCacheMu.Unlock()

	if staleRemoved := prevSize - len(fresh); staleRemoved > 0 {
		c.logger.Info("session owner cache reconciled",
			slog.Int("prev_size", prevSize),
			slog.Int("new_size", len(fresh)),
			slog.Int("stale_removed", staleRemoved))
	} else {
		c.logger.Info("loaded session owners into cache", slog.Int("cache_len", len(fresh)))
	}
	return nil
}

// reconcileSessionOwnerCache periodically reloads session owners from etcd.
func (c *EtcdCluster) reconcileSessionOwnerCache() {
	const reconcileInterval = 5 * time.Minute

	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			if err := c.loadSessionOwnerCache(); err != nil {
				c.logger.Error("session owner cache reconciliation failed",
					slog.String("error", err.Error()))
			}
			c.selfHealLeasedKeys()
		}
	}
}

// selfHealLeasedKeys re-registers tracked leased keys that are missing from
// etcd (deleted by lease expiry while watch events were missed). All leased
// keys live under sessionsPrefix or queueConsumersPrefix.
func (c *EtcdCluster) selfHealLeasedKeys() {
	c.leaseMu.Lock()
	tracked := maps.Clone(c.leasedKeys)
	c.leaseMu.Unlock()
	if len(tracked) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(c.lifecycleCtx, 10*time.Second)
	defer cancel()

	existing := make(map[string]struct{}, len(tracked))
	for _, prefix := range []string{sessionsPrefix, queueConsumersPrefix} {
		resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
		if err != nil {
			c.logger.Error("leased key self-heal scan failed",
				slog.String("prefix", prefix),
				slog.String("error", err.Error()))
			return
		}
		for _, kv := range resp.Kvs {
			existing[string(kv.Key)] = struct{}{}
		}
	}

	restored := 0
	for key, value := range tracked {
		if _, isSessionOwner := parseSessionOwnerKey(key); isSessionOwner {
			continue
		}
		if _, ok := existing[key]; ok {
			continue
		}
		if err := c.putWithSessionLease(ctx, key, value); err != nil {
			c.logger.Error("failed to restore leased key",
				slog.String("key", key),
				slog.String("error", err.Error()))
			continue
		}
		restored++
	}
	if restored > 0 {
		c.logger.Warn("restored leased keys missing from etcd",
			slog.Int("count", restored))
	}
}

// getLeasedKey returns the tracked value for a leased key, if present.
func (c *EtcdCluster) getLeasedKey(key string) (string, bool) {
	c.leaseMu.Lock()
	defer c.leaseMu.Unlock()
	value, ok := c.leasedKeys[key]
	return value, ok
}

// watchSessionOwners watches etcd for session owner changes and updates the local cache.
func (c *EtcdCluster) watchSessionOwners() {
	for {
		c.ownerCacheMu.RLock()
		rev := c.ownerCacheRev
		c.ownerCacheMu.RUnlock()
		watchCh := c.client.Watch(c.lifecycleCtx, sessionsPrefix, prefixWatchOpts(rev)...)

		for {
			select {
			case <-c.stopCh:
				return
			case watchResp, ok := <-watchCh:
				if !ok {
					if c.lifecycleCtx.Err() != nil {
						return
					}
					c.logger.Warn("session owner watch channel closed, reloading cache")
					if err := c.loadSessionOwnerCache(); err != nil {
						c.logger.Error("failed to reload session owners", slog.String("error", err.Error()))
					}
					goto restart
				}
				if watchResp.Err() != nil {
					c.logger.Error("session owner watch error", slog.String("error", watchResp.Err().Error()))
					if err := c.loadSessionOwnerCache(); err != nil {
						c.logger.Error("failed to reload session owners", slog.String("error", err.Error()))
					}
					goto restart
				}

				lostSessions := make([]string, 0)
				for _, event := range watchResp.Events {
					if event.Kv == nil {
						continue
					}
					key := string(event.Kv.Key)
					clientID, ok := parseSessionOwnerKey(key)
					if !ok {
						continue
					}
					if event.Type == clientv3.EventTypeDelete {
						if _, tracked := c.getLeasedKey(key); tracked {
							// An owner key disappearing means this node can
							// no longer prove ownership. Never resurrect it:
							// stop tracking and fence the local connection.
							c.untrackLeasedKey(key)
							lostSessions = append(lostSessions, clientID)
						}
						c.ownerCacheMu.Lock()
						delete(c.ownerCache, clientID)
						c.ownerCacheMu.Unlock()
						continue
					}
					value := string(event.Kv.Value)
					if value != c.nodeID {
						// Ownership moved to another node; stop tracking
						// so this node never resurrects the key.
						c.untrackLeasedKey(key)
					}
					c.ownerCacheMu.Lock()
					c.ownerCache[clientID] = value
					c.ownerCacheMu.Unlock()
				}
				c.notifySessionLeaseLost(c.lifecycleCtx, lostSessions)
			}
		}
	restart:
		select {
		case <-c.stopCh:
			return
		case <-time.After(time.Second):
		}
	}
}

func (c *EtcdCluster) loadSubscriptionCache() error {
	ctx := context.Background()
	resp, err := c.client.Get(ctx, subscriptionsPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to load subscriptions: %w", err)
	}

	fresh := make(map[string]*storage.Subscription)
	freshClientSubs := make(map[string][]string)
	freshTrie := router.NewRouter()

	for _, kv := range resp.Kvs {
		clientID := strings.TrimPrefix(string(kv.Key), subscriptionsPrefix)

		var subs []storage.Subscription
		if err := json.Unmarshal(kv.Value, &subs); err != nil {
			c.logger.Warn("failed to unmarshal subscriptions during cache load",
				slog.String("client_id", clientID), slog.String("error", err.Error()))
			continue
		}

		for i := range subs {
			subPtr := new(storage.Subscription)
			*subPtr = subs[i]
			cacheKey := subPtr.ClientID + "|" + subPtr.Filter
			fresh[cacheKey] = subPtr
			freshClientSubs[clientID] = append(freshClientSubs[clientID], cacheKey)
			if err := freshTrie.Subscribe(subPtr.ClientID, subPtr.Filter, subPtr.QoS, subPtr.Options); err != nil {
				c.logger.Warn("failed to index subscription in trie",
					slog.String("client_id", subPtr.ClientID),
					slog.String("filter", subPtr.Filter),
					slog.String("error", err.Error()))
			}
		}
	}

	c.subCacheMu.Lock()
	prevSize := len(c.subCache)
	c.subCache = fresh
	c.clientSubs = freshClientSubs
	c.subTrie = freshTrie
	c.subCacheRev = resp.Header.Revision
	c.subCacheMu.Unlock()

	if staleRemoved := prevSize - len(fresh); staleRemoved > 0 {
		c.logger.Info("subscription cache reconciled",
			slog.Int("prev_size", prevSize),
			slog.Int("new_size", len(fresh)),
			slog.Int("stale_removed", staleRemoved))
	} else {
		c.logger.Info("loaded subscriptions into cache", slog.Int("cache_len", len(fresh)))
	}
	return nil
}

// reconcileSubscriptionCache periodically reloads the subscription cache from
// etcd to evict stale entries that may have been missed by the watch (e.g. due
// to etcd compaction, network partition, or missed delete events).
func (c *EtcdCluster) reconcileSubscriptionCache() {
	const reconcileInterval = 5 * time.Minute

	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			if err := c.loadSubscriptionCache(); err != nil {
				c.logger.Error("subscription cache reconciliation failed",
					slog.String("error", err.Error()))
			}
		}
	}
}

// watchSubscriptions watches etcd for subscription changes and updates the local cache.
func (c *EtcdCluster) watchSubscriptions() {
	for {
		c.subCacheMu.RLock()
		rev := c.subCacheRev
		c.subCacheMu.RUnlock()
		watchCh := c.client.Watch(c.lifecycleCtx, subscriptionsPrefix, prefixWatchOpts(rev)...)

		for {
			select {
			case <-c.stopCh:
				return
			case watchResp, ok := <-watchCh:
				if !ok {
					if c.lifecycleCtx.Err() != nil {
						return
					}
					c.logger.Warn("subscription watch channel closed, reloading cache")
					if err := c.loadSubscriptionCache(); err != nil {
						c.logger.Error("failed to reload subscriptions", slog.String("error", err.Error()))
					}
					goto restart
				}
				if watchResp.Err() != nil {
					c.logger.Error("subscription watch error", slog.String("error", watchResp.Err().Error()))
					if err := c.loadSubscriptionCache(); err != nil {
						c.logger.Error("failed to reload subscriptions", slog.String("error", err.Error()))
					}
					goto restart
				}

				c.subCacheMu.Lock()
				for _, event := range watchResp.Events {
					clientID := strings.TrimPrefix(string(event.Kv.Key), subscriptionsPrefix)

					// Purge all existing cache entries for this client
					for _, ck := range c.clientSubs[clientID] {
						if prevSub, ok := c.subCache[ck]; ok {
							_ = c.subTrie.Unsubscribe(prevSub.ClientID, prevSub.Filter)
						}
						delete(c.subCache, ck)
					}
					delete(c.clientSubs, clientID)

					if event.Type == clientv3.EventTypePut {
						var subs []storage.Subscription
						if err := json.Unmarshal(event.Kv.Value, &subs); err != nil {
							c.logger.Error("failed to unmarshal subscriptions in watch",
								slog.String("client_id", clientID), slog.String("error", err.Error()))
							continue
						}

						keys := make([]string, 0, len(subs))
						for i := range subs {
							subPtr := new(storage.Subscription)
							*subPtr = subs[i]
							ck := subPtr.ClientID + "|" + subPtr.Filter
							c.subCache[ck] = subPtr
							keys = append(keys, ck)
							if err := c.subTrie.Subscribe(subPtr.ClientID, subPtr.Filter, subPtr.QoS, subPtr.Options); err != nil {
								c.logger.Warn("failed to index subscription in trie",
									slog.String("client_id", subPtr.ClientID),
									slog.String("filter", subPtr.Filter),
									slog.String("error", err.Error()))
							}
						}
						c.clientSubs[clientID] = keys
					}
				}
				c.subCacheMu.Unlock()
			}
		}
	restart:
		select {
		case <-c.stopCh:
			return
		case <-time.After(time.Second):
		}
	}
}
