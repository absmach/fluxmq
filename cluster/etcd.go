// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/absmach/fluxmq/broker/router"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/embed"
)

const (
	// MQTT-specific prefixes
	willPrefix     = "/mqtt/wills/"
	retainedPrefix = "/mqtt/retained/"

	// Protocol-agnostic prefixes
	subscriptionsPrefix  = "/subscriptions/"
	sessionsPrefix       = "/sessions/"
	queueConsumersPrefix = "/queue-consumers/"
	electionPrefix       = "/leader"

	urlPrefix = "http://"
)

var (
	_ Cluster = (*EtcdCluster)(nil)

	ErrEtcdServerStartTimeout     = errors.New("etcd server took too long to start")
	ErrTransportNotConfigured     = errors.New("transport not configured")
	ErrNoMessageHandlerConfigured = errors.New("no message handler configured")
	ErrNoLocalStoreConfigured     = errors.New("no local store configured")
)

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

	// Lease for session ownership (with auto-renewal)
	sessionLease clientv3.LeaseID
	leaseMu      sync.Mutex
	leaseCancel  context.CancelFunc

	// gRPC transport for inter-broker communication
	transport *Transport

	// Handler for incoming routed messages and session management
	msgHandler MessageHandler

	logger *slog.Logger

	// Local subscription cache for fast topic matching
	subCache   map[string]*storage.Subscription // key: clientID|filter
	clientSubs map[string][]string              // clientID → []cacheKey (reverse index)
	subTrie    *router.TrieRouter
	subCacheMu sync.RWMutex

	// Local session owner cache to avoid etcd roundtrips in RoutePublish
	ownerCache   map[string]string // clientID -> nodeID
	ownerCacheMu sync.RWMutex

	// Local queue consumer cache for fast queue delivery/routing lookups.
	queueConsumersAll     map[string]*QueueConsumerInfo                       // key: queue|group|consumer
	queueConsumersByQueue map[string]map[string]*QueueConsumerInfo            // queue -> key -> info
	queueConsumersByGroup map[string]map[string]map[string]*QueueConsumerInfo // queue -> group -> key -> info
	queueConsumersCacheMu sync.RWMutex

	routeBatchMaxSize  int
	routeBatchMaxDelay time.Duration
	forwardBatcher     *nodeBatcher[*clusterv1.ForwardPublishRequest]
	queueBatcher       *nodeBatcher[QueueDelivery]

	// Local retained message cache for fast wildcard matching (deprecated, use hybridRetained)
	retainedCache   map[string]*storage.Message // key: topic
	retainedCacheMu sync.RWMutex

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
	HybridRetainedSizeThreshold int // Size threshold in bytes for hybrid retained storage (default 1024)
	RouteBatchMaxSize           int
	RouteBatchMaxDelay          time.Duration

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
	// Create embedded etcd configuration
	eCfg := embed.NewConfig()
	eCfg.Name = cfg.NodeID
	eCfg.Dir = cfg.DataDir

	// Peer URLs (for Raft communication)
	peerURL, err := url.Parse(urlPrefix + cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid bind address: %w", err)
	}
	eCfg.ListenPeerUrls = []url.URL{*peerURL}

	// Advertise URL (what other nodes use to contact this node)
	if cfg.AdvertiseAddr != "" {
		advertiseURL, err := url.Parse(urlPrefix + cfg.AdvertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("invalid advertise address: %w", err)
		}
		eCfg.AdvertisePeerUrls = []url.URL{*advertiseURL}
	} else {
		eCfg.AdvertisePeerUrls = []url.URL{*peerURL}
	}

	// Client URLs (for KV operations)
	clientURL, err := url.Parse(urlPrefix + cfg.ClientAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid client address: %w", err)
	}
	eCfg.ListenClientUrls = []url.URL{*clientURL}
	eCfg.AdvertiseClientUrls = []url.URL{*clientURL}

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
		Endpoints:   []string{cfg.ClientAddr},
		DialTimeout: 5 * time.Second,
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
		retainedCache:         make(map[string]*storage.Message),
		localStore:            localStore,
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

	// Stop gRPC transport
	if c.transport != nil {
		c.transport.Stop()
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
			Leader:  member.Name == c.nodeID && c.IsLeader(),
		})
	}

	return nodes
}

// IsLeader checks if this node is the cluster leader.
func (c *EtcdCluster) IsLeader() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
		if c.IsLeader() {
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

	c.retainedCacheMu.Lock()
	defer c.retainedCacheMu.Unlock()

	for _, kv := range resp.Kvs {
		var msg storage.Message
		if err := json.Unmarshal(kv.Value, &msg); err != nil {
			c.logger.Warn("failed to unmarshal retained message during cache load", slog.String("error", err.Error()))
			continue
		}

		// Extract topic from key (remove prefix)
		topic := strings.TrimPrefix(string(kv.Key), retainedPrefix)
		c.retainedCache[topic] = &msg
	}

	c.logger.Info("loaded retained messages into cache", slog.Int("count", len(c.retainedCache)))
	return nil
}

// watchRetained watches etcd for retained message changes and updates the local cache.
func (c *EtcdCluster) watchRetained() {
	for {
		watchCh := c.client.Watch(c.lifecycleCtx, retainedPrefix, clientv3.WithPrefix())

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
						var msg storage.Message
						if err := json.Unmarshal(event.Kv.Value, &msg); err != nil {
							c.logger.Warn("failed to unmarshal retained message", slog.String("error", err.Error()))
							continue
						}
						c.retainedCache[topic] = &msg

					case clientv3.EventTypeDelete:
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

	keepAliveCtx, cancel := context.WithCancel(context.Background())
	ch, err := c.client.KeepAlive(keepAliveCtx, leaseResp.ID)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to keep lease alive: %w", err)
	}

	c.sessionLease = leaseResp.ID
	c.leaseCancel = cancel

	go func() {
		for {
			select {
			case <-c.stopCh:
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
			}
		}
	}()

	return nil
}

func isLeaseNotFoundErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "requested lease not found")
}

func (c *EtcdCluster) putWithSessionLease(ctx context.Context, key, value string) error {
	c.leaseMu.Lock()
	leaseID := c.sessionLease
	c.leaseMu.Unlock()

	_, err := c.client.Put(ctx, key, value, clientv3.WithLease(leaseID))
	if err == nil {
		return nil
	}
	if !isLeaseNotFoundErr(err) {
		return err
	}

	if err := c.refreshSessionLease(ctx); err != nil {
		return err
	}

	c.leaseMu.Lock()
	leaseID = c.sessionLease
	c.leaseMu.Unlock()
	_, err = c.client.Put(ctx, key, value, clientv3.WithLease(leaseID))
	return err
}

// AcquireSession registers this node as the owner of a session.
// Uses a leased Put so ownership auto-expires if this node dies.
// This is called after takeover has completed (if needed), so it's safe
// to unconditionally overwrite — the caller already handled ownership transfer.
func (c *EtcdCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
	key := sessionsPrefix + clientID + "/owner"

	if err := c.putWithSessionLease(ctx, key, nodeID); err != nil {
		return err
	}

	c.ownerCacheMu.Lock()
	c.ownerCache[clientID] = nodeID
	c.ownerCacheMu.Unlock()

	return nil
}

// ReleaseSession releases ownership of a session, only if this node owns it.
func (c *EtcdCluster) ReleaseSession(ctx context.Context, clientID string) error {
	key := sessionsPrefix + clientID + "/owner"

	// CAS delete: only delete if we own it
	_, err := c.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(key), "=", c.nodeID)).
		Then(clientv3.OpDelete(key)).
		Commit()

	c.ownerCacheMu.Lock()
	delete(c.ownerCache, clientID)
	c.ownerCacheMu.Unlock()

	return err
}

// GetSessionOwner returns the node ID that owns the session.
// Uses local cache first, falls back to etcd.
func (c *EtcdCluster) GetSessionOwner(ctx context.Context, clientID string) (string, bool, error) {
	// Check local cache first
	c.ownerCacheMu.RLock()
	nodeID, ok := c.ownerCache[clientID]
	c.ownerCacheMu.RUnlock()
	if ok {
		return nodeID, true, nil
	}

	key := sessionsPrefix + clientID + "/owner"

	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return "", false, err
	}

	if len(resp.Kvs) == 0 {
		return "", false, nil
	}

	owner := string(resp.Kvs[0].Value)

	// Cache the result
	c.ownerCacheMu.Lock()
	c.ownerCache[clientID] = owner
	c.ownerCacheMu.Unlock()

	return owner, true, nil
}

// WatchSessionOwner watches for ownership changes of a specific session.
func (c *EtcdCluster) WatchSessionOwner(ctx context.Context, clientID string) <-chan OwnershipChange {
	key := sessionsPrefix + clientID + "/owner"
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

func (s *etcdRetainedStore) Set(ctx context.Context, topic string, msg *storage.Message) error {
	key := retainedPrefix + topic

	// Empty payload means delete
	if len(msg.Payload) == 0 {
		return s.Delete(ctx, topic)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal retained message: %w", err)
	}

	_, err = s.client.Put(ctx, key, string(data))
	return err
}

func (s *etcdRetainedStore) Get(ctx context.Context, topic string) (*storage.Message, error) {
	key := retainedPrefix + topic

	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, storage.ErrNotFound
	}

	var msg storage.Message
	if err := json.Unmarshal(resp.Kvs[0].Value, &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal retained message: %w", err)
	}

	return &msg, nil
}

func (s *etcdRetainedStore) Delete(ctx context.Context, topic string) error {
	key := retainedPrefix + topic
	_, err := s.client.Delete(ctx, key)
	return err
}

func (s *etcdRetainedStore) Match(ctx context.Context, filter string) ([]*storage.Message, error) {
	// Use local cache for fast wildcard matching instead of etcd scan
	s.cluster.retainedCacheMu.RLock()
	defer s.cluster.retainedCacheMu.RUnlock()

	var matched []*storage.Message
	for topic, msg := range s.cluster.retainedCache {
		if topicMatchesFilter(topic, filter) {
			// Create a copy to avoid returning cached pointers
			msgCopy := *msg
			matched = append(matched, &msgCopy)
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
func (c *EtcdCluster) RoutePublish(ctx context.Context, topic string, payload []byte, qos byte, retain bool, properties map[string]string) error {
	if c.transport == nil {
		return nil
	}

	// Match cluster trie to find any remote subscribers
	subs, err := c.GetSubscribersForTopic(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to get subscribers: %w", err)
	}

	// Collect unique remote node IDs
	remoteNodes := make(map[string]struct{})
	c.ownerCacheMu.RLock()
	cacheMisses := make(map[string]struct{})
	for _, sub := range subs {
		nodeID, ok := c.ownerCache[sub.ClientID]
		if !ok {
			cacheMisses[sub.ClientID] = struct{}{}
			continue
		}
		if nodeID != c.nodeID {
			remoteNodes[nodeID] = struct{}{}
		}
	}
	c.ownerCacheMu.RUnlock()

	// Fallback to etcd for cache misses
	for clientID := range cacheMisses {
		if ctx.Err() != nil {
			break
		}
		nodeID, _, err := c.GetSessionOwner(ctx, clientID)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				break
			}
			continue
		}
		if nodeID == "" || nodeID == c.nodeID {
			continue
		}
		remoteNodes[nodeID] = struct{}{}
	}

	if len(remoteNodes) == 0 {
		return nil
	}

	// Send one ForwardPublish per remote node
	msg := &clusterv1.ForwardPublishRequest{
		Topic:      topic,
		Payload:    payload,
		Qos:        uint32(qos),
		Retain:     retain,
		Properties: properties,
	}

	var errs []error
	for nodeID := range remoteNodes {
		var err error
		if c.forwardBatcher != nil {
			if qos == 0 {
				err = c.forwardBatcher.EnqueueAsync(ctx, nodeID, []*clusterv1.ForwardPublishRequest{msg})
			} else {
				err = c.forwardBatcher.Enqueue(ctx, nodeID, []*clusterv1.ForwardPublishRequest{msg})
			}
		} else {
			err = c.transport.SendForwardPublishBatch(ctx, nodeID, []*clusterv1.ForwardPublishRequest{msg})
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

// TakeoverSession initiates session takeover from one node to another.
func (c *EtcdCluster) TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) (*clusterv1.SessionState, error) {
	if fromNode == toNode {
		// Same node, no takeover needed
		return nil, nil
	}

	if c.transport == nil {
		// No transport, can't do remote takeover
		// Just update ownership
		if err := c.AcquireSession(ctx, clientID, toNode); err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Call gRPC to fromNode to get session state
	state, err := c.transport.SendTakeover(ctx, fromNode, clientID, fromNode, toNode)
	if err != nil {
		return nil, fmt.Errorf("failed to request takeover from %s: %w", fromNode, err)
	}

	// Update ownership in etcd to new node
	if err := c.AcquireSession(ctx, clientID, toNode); err != nil {
		return nil, fmt.Errorf("failed to acquire session ownership: %w", err)
	}

	c.logger.Info("session taken over",
		slog.String("client_id", clientID),
		slog.String("from_node", fromNode),
		slog.String("to_node", toNode))
	return state, nil
}

// EnqueueRemote sends an enqueue request to a remote node.
func (c *EtcdCluster) EnqueueRemote(ctx context.Context, nodeID, queueName string, payload []byte, properties map[string]string) (string, error) {
	if c.transport == nil {
		return "", ErrTransportNotConfigured
	}
	return c.transport.SendEnqueueRemote(ctx, nodeID, queueName, payload, properties, false, false)
}

// RouteQueueMessage sends a queue message to a remote consumer.
func (c *EtcdCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName string, msg *QueueMessage) error {
	if c.transport == nil {
		return ErrTransportNotConfigured
	}
	return c.transport.SendRouteQueueMessage(ctx, nodeID, clientID, queueName, msg)
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
func (c *EtcdCluster) DeliverToClient(ctx context.Context, clientID string, msg *Message) error {
	if c.msgHandler == nil {
		return ErrNoMessageHandlerConfigured
	}
	return c.msgHandler.DeliverToClient(ctx, clientID, msg)
}

// GetSessionStateAndClose implements MessageHandler.GetSessionStateAndClose.
// Delegates to the broker to capture session state and close the session.
func (c *EtcdCluster) GetSessionStateAndClose(ctx context.Context, clientID string) (*clusterv1.SessionState, error) {
	if c.msgHandler == nil {
		return nil, ErrNoMessageHandlerConfigured
	}
	return c.msgHandler.GetSessionStateAndClose(ctx, clientID)
}

// GetRetainedMessage implements MessageHandler.GetRetainedMessage.
// Fetches a retained message from the local BadgerDB store.
func (c *EtcdCluster) GetRetainedMessage(ctx context.Context, topic string) (*storage.Message, error) {
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
// Called when another broker routes a PUBLISH message to this node.
func (c *EtcdCluster) HandlePublish(ctx context.Context, clientID, topic string, payload []byte, qos byte, retain, dup bool, properties map[string]string) error {
	if c.msgHandler == nil {
		return ErrNoMessageHandlerConfigured
	}

	msg := &Message{
		Topic:      topic,
		Payload:    payload,
		QoS:        qos,
		Retain:     retain,
		Dup:        dup,
		Properties: properties,
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
	c.queueConsumersCacheMu.Unlock()

	c.logger.Info("loaded queue consumers into cache", slog.Int("cache_len", len(freshAll)))
	return nil
}

func (c *EtcdCluster) watchQueueConsumers() {
	for {
		watchCh := c.client.Watch(c.lifecycleCtx, queueConsumersPrefix, clientv3.WithPrefix())

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

// ForwardQueuePublish forwards a queue publish to a remote node.
func (c *EtcdCluster) ForwardQueuePublish(ctx context.Context, nodeID, topic string, payload []byte, properties map[string]string, forwardToLeader bool) error {
	if c.transport == nil {
		return ErrTransportNotConfigured
	}

	// Use SendEnqueueRemote with topic in queueName field
	_, err := c.transport.SendEnqueueRemote(ctx, nodeID, topic, payload, properties, true, forwardToLeader)
	return err
}

// ForwardGroupOp forwards a consumer group operation to a remote node.
func (c *EtcdCluster) ForwardGroupOp(ctx context.Context, nodeID, queueName string, opData []byte) error {
	if c.transport == nil {
		return ErrTransportNotConfigured
	}

	return c.transport.SendForwardGroupOp(ctx, nodeID, queueName, opData)
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
	sessionState, err := c.msgHandler.GetSessionStateAndClose(ctx, clientID)
	if err != nil {
		return nil, fmt.Errorf("failed to get session state: %w", err)
	}

	c.logger.Info("session handed over",
		slog.String("client_id", clientID),
		slog.String("from_node", fromNode),
		slog.String("to_node", toNode))
	return sessionState, nil
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
		}
	}
}

// watchSessionOwners watches etcd for session owner changes and updates the local cache.
func (c *EtcdCluster) watchSessionOwners() {
	for {
		watchCh := c.client.Watch(c.lifecycleCtx, sessionsPrefix, clientv3.WithPrefix())

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

				c.ownerCacheMu.Lock()
				for _, event := range watchResp.Events {
					if event.Kv == nil {
						continue
					}
					clientID, ok := parseSessionOwnerKey(string(event.Kv.Key))
					if !ok {
						continue
					}
					if event.Type == clientv3.EventTypeDelete {
						delete(c.ownerCache, clientID)
						continue
					}
					c.ownerCache[clientID] = string(event.Kv.Value)
				}
				c.ownerCacheMu.Unlock()
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
		watchCh := c.client.Watch(c.lifecycleCtx, subscriptionsPrefix, clientv3.WithPrefix())

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
