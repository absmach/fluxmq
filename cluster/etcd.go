// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/absmach/mqtt/cluster/grpc"
	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/embed"
)

const (
	willPrefix          = "/mqtt/wills/"
	retainedPrefix      = "/mqtt/retained/"
	subscriptionsPrefix = "/mqtt/subscriptions/"
	sessionsPrefix      = "/mqtt/sessions/"

	electionPrefix = "/mqtt/leader"
	urlPrefix      = "http://"
)

var _ Cluster = (*EtcdCluster)(nil)

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

	// gRPC transport for inter-broker communication
	transport *Transport

	// Handler for incoming routed messages and session management
	msgHandler MessageHandler

	logger *slog.Logger

	// Local subscription cache for fast topic matching
	subCache   map[string]*storage.Subscription // key: clientID|filter
	subCacheMu sync.RWMutex

	// Local retained message cache for fast wildcard matching
	retainedCache   map[string]*storage.Message // key: topic
	retainedCacheMu sync.RWMutex

	stopCh chan struct{}
}

// EtcdConfig holds embedded etcd configuration.
type EtcdConfig struct {
	NodeID         string
	DataDir        string
	BindAddr       string
	ClientAddr     string
	AdvertiseAddr  string
	InitialCluster string
	TransportAddr  string
	PeerTransports map[string]string
	Bootstrap      bool
}

// NewEtcdCluster creates a new embedded etcd cluster.
func NewEtcdCluster(cfg *EtcdConfig, logger *slog.Logger) (*EtcdCluster, error) {
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
		return nil, fmt.Errorf("etcd server took too long to start")
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
		nodeID:        cfg.NodeID,
		config:        cfg,
		etcd:          e,
		client:        client,
		election:      election,
		session:       s,
		logger:        logger,
		subCache:      make(map[string]*storage.Subscription),
		retainedCache: make(map[string]*storage.Message),
		stopCh:        make(chan struct{}),
	}

	// Create a lease for session ownership with auto-renewal
	leaseResp, err := client.Grant(context.Background(), 30) // 30 second TTL
	if err != nil {
		return nil, fmt.Errorf("failed to create lease: %w", err)
	}
	c.sessionLease = leaseResp.ID

	// Keep lease alive
	ch, err := client.KeepAlive(context.Background(), c.sessionLease)
	if err != nil {
		return nil, fmt.Errorf("failed to keep lease alive: %w", err)
	}

	// Consume keepalive responses in background
	go func() {
		for range ch {
			// Lease kept alive
		}
	}()

	// Initialize gRPC transport if configured
	if cfg.TransportAddr != "" {
		transport, err := NewTransport(cfg.NodeID, cfg.TransportAddr, c, logger)
		if err != nil {
			client.Close()
			s.Close()
			e.Close()
			return nil, fmt.Errorf("failed to create transport: %w", err)
		}
		c.transport = transport
	}

	return c, nil
}

// Start begins cluster participation (campaigns for leadership).
func (c *EtcdCluster) Start() error {
	// Load existing subscriptions into cache
	if err := c.loadSubscriptionCache(); err != nil {
		c.logger.Warn("failed to load subscription cache", slog.String("error", err.Error()))
	}

	// Start watching for subscription changes
	go c.watchSubscriptions()

	// Load retained message cache on startup
	if err := c.loadRetainedCache(); err != nil {
		c.logger.Warn("failed to load retained cache", slog.String("error", err.Error()))
	}

	// Start watching for retained message changes
	go c.watchRetained()

	// Start gRPC transport if configured
	if c.transport != nil {
		if err := c.transport.Start(); err != nil {
			return fmt.Errorf("failed to start transport: %w", err)
		}

		// Connect to peer nodes
		if c.config.PeerTransports != nil {
			for nodeID, addr := range c.config.PeerTransports {
				if nodeID != c.nodeID {
					if err := c.transport.ConnectPeer(nodeID, addr); err != nil {
						c.logger.Warn("failed to connect to peer", slog.String("node_id", nodeID), slog.String("error", err.Error()))
					}
				}
			}
		}
	}

	go c.campaignLeader()
	return nil
}

// Stop gracefully shuts down the cluster.
func (c *EtcdCluster) Stop() error {
	close(c.stopCh)

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
	watchCh := c.client.Watch(context.Background(), retainedPrefix, clientv3.WithPrefix())

	for {
		select {
		case <-c.stopCh:
			return
		case watchResp := <-watchCh:
			if watchResp.Err() != nil {
				c.logger.Error("retained watch error", slog.String("error", watchResp.Err().Error()))
				continue
			}

			c.retainedCacheMu.Lock()
			for _, event := range watchResp.Events {
				topic := strings.TrimPrefix(string(event.Kv.Key), retainedPrefix)

				switch event.Type {
				case clientv3.EventTypePut:
					// Retained message added or updated
					var msg storage.Message
					if err := json.Unmarshal(event.Kv.Value, &msg); err != nil {
						c.logger.Warn("failed to unmarshal retained message", slog.String("error", err.Error()))
						continue
					}
					c.retainedCache[topic] = &msg

				case clientv3.EventTypeDelete:
					// Retained message removed
					delete(c.retainedCache, topic)
				}
			}
			c.retainedCacheMu.Unlock()
		}
	}
}

// campaignLeader attempts to become the cluster leader.
// Retries on failure until successful or cluster stops.
func (c *EtcdCluster) campaignLeader() {
	// Wait a bit for cluster to form quorum (2 out of 3 nodes)
	// This prevents racing to campaign before the cluster is ready
	time.Sleep(3 * time.Second)

	ctx := context.Background()
	retryDelay := 2 * time.Second
	maxRetryDelay := 30 * time.Second

	for {
		c.logger.Info("Campaigning for leadership", slog.String("node_id", c.nodeID))

		if err := c.election.Campaign(ctx, c.nodeID); err != nil {
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

// AcquireSession registers this node as the owner of a session.
func (c *EtcdCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
	key := sessionsPrefix + clientID + "/owner"

	// Try to acquire with our lease (auto-expires if node dies)
	_, err := c.client.Put(ctx, key, nodeID, clientv3.WithLease(c.sessionLease))
	return err
}

// ReleaseSession releases ownership of a session.
func (c *EtcdCluster) ReleaseSession(ctx context.Context, clientID string) error {
	key := sessionsPrefix + clientID + "/owner"
	_, err := c.client.Delete(ctx, key)
	return err
}

// GetSessionOwner returns the node ID that owns the session.
func (c *EtcdCluster) GetSessionOwner(ctx context.Context, clientID string) (string, bool, error) {
	key := sessionsPrefix + clientID + "/owner"

	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return "", false, err
	}

	if len(resp.Kvs) == 0 {
		return "", false, nil
	}

	return string(resp.Kvs[0].Value), true, nil
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
func (c *EtcdCluster) AddSubscription(ctx context.Context, clientID, filter string, qos byte, opts storage.SubscribeOptions) error {
	key := fmt.Sprintf("%s%s/%s", subscriptionsPrefix, clientID, filter)

	sub := &storage.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      qos,
		Options:  opts,
	}

	data, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("failed to marshal subscription: %w", err)
	}

	_, err = c.client.Put(ctx, key, string(data))
	return err
}

// RemoveSubscription removes a subscription from the cluster store.
func (c *EtcdCluster) RemoveSubscription(ctx context.Context, clientID, filter string) error {
	key := fmt.Sprintf("%s%s/%s", subscriptionsPrefix, clientID, filter)
	_, err := c.client.Delete(ctx, key)
	return err
}

// GetSubscriptionsForClient returns all subscriptions for a client.
func (c *EtcdCluster) GetSubscriptionsForClient(ctx context.Context, clientID string) ([]*storage.Subscription, error) {
	prefix := subscriptionsPrefix + clientID + "/"

	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var subs []*storage.Subscription
	for _, kv := range resp.Kvs {
		var sub storage.Subscription
		if err := json.Unmarshal(kv.Value, &sub); err != nil {
			c.logger.Warn("failed to unmarshal subscription", slog.String("error", err.Error()))
			continue
		}
		subs = append(subs, &sub)
	}

	return subs, nil
}

// GetSubscribersForTopic returns all subscriptions matching a topic.
// Optimized: uses local cache for fast lookup.
func (c *EtcdCluster) GetSubscribersForTopic(ctx context.Context, topic string) ([]*storage.Subscription, error) {
	c.subCacheMu.RLock()
	defer c.subCacheMu.RUnlock()

	var matched []*storage.Subscription
	for _, sub := range c.subCache {
		// Check if topic matches the subscription filter
		if topicMatchesFilter(topic, sub.Filter) {
			matched = append(matched, sub)
		}
	}

	return matched, nil
}

// Retained returns the cluster-wide retained message store.
func (c *EtcdCluster) Retained() storage.RetainedStore {
	return &etcdRetainedStore{logger: c.logger, client: c.client, cluster: c}
}

// Wills returns the cluster-wide will message store.
func (c *EtcdCluster) Wills() storage.WillStore {
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

// RoutePublish routes a publish to interested nodes with matching subscriptions.
func (c *EtcdCluster) RoutePublish(ctx context.Context, topic string, payload []byte, qos byte, retain bool, properties map[string]string) error {
	if c.transport == nil {
		// No transport configured, messages only delivered locally
		return nil
	}

	// Get all subscriptions matching this topic
	subs, err := c.GetSubscribersForTopic(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to get subscribers: %w", err)
	}

	nodeClients := make(map[string][]string) // nodeID -> []clientIDs
	for _, sub := range subs {
		nodeID, exists, err := c.GetSessionOwner(ctx, sub.ClientID)
		if err != nil {
			c.logger.Warn("failed to get session owner for", slog.String("client_id", sub.ClientID), slog.String("error", err.Error()))
			continue
		}
		if !exists {
			// Client not connected, skip
			continue
		}

		if nodeID == c.nodeID {
			continue
		}

		nodeClients[nodeID] = append(nodeClients[nodeID], sub.ClientID)
	}

	for nodeID, clientIDs := range nodeClients {
		for _, clientID := range clientIDs {
			err := c.transport.SendPublish(ctx, nodeID, clientID, topic, payload, qos, retain, false, properties)
			if err != nil {
				c.logger.Warn("failed to route publish",
					slog.String("client_id", clientID),
					slog.String("node_id", nodeID),
					slog.String("error", err.Error()))
			}
		}
	}

	return nil
}

// TakeoverSession initiates session takeover from one node to another.
func (c *EtcdCluster) TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) (*grpc.SessionState, error) {
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

// These methods allow EtcdCluster to implement the MessageHandler interface
// by delegating to the broker's handler.

// DeliverToClient implements MessageHandler.DeliverToClient.
// Delegates to the broker to deliver a message to a local client.
func (c *EtcdCluster) DeliverToClient(ctx context.Context, clientID string, msg *core.Message) error {
	if c.msgHandler == nil {
		return fmt.Errorf("no message handler configured")
	}
	return c.msgHandler.DeliverToClient(ctx, clientID, msg)
}

// GetSessionStateAndClose implements MessageHandler.GetSessionStateAndClose.
// Delegates to the broker to capture session state and close the session.
func (c *EtcdCluster) GetSessionStateAndClose(ctx context.Context, clientID string) (*grpc.SessionState, error) {
	if c.msgHandler == nil {
		return nil, fmt.Errorf("no message handler configured")
	}
	return c.msgHandler.GetSessionStateAndClose(ctx, clientID)
}

// HandlePublish implements TransportHandler.HandlePublish.
// Called when another broker routes a PUBLISH message to this node.
func (c *EtcdCluster) HandlePublish(ctx context.Context, clientID, topic string, payload []byte, qos byte, retain, dup bool, properties map[string]string) error {
	if c.msgHandler == nil {
		return fmt.Errorf("no message handler configured")
	}

	msg := &core.Message{
		Topic:      topic,
		Payload:    payload,
		QoS:        qos,
		Retain:     retain,
		Dup:        dup,
		Properties: properties,
	}

	return c.msgHandler.DeliverToClient(ctx, clientID, msg)
}

// HandleTakeover implements TransportHandler.HandleTakeover.
// Called when another broker requests to take over a session from this node.
func (c *EtcdCluster) HandleTakeover(ctx context.Context, clientID, fromNode, toNode string, state *grpc.SessionState) (*grpc.SessionState, error) {
	// Verify this is the node being asked to give up the session
	if fromNode != c.nodeID {
		return nil, fmt.Errorf("takeover request for wrong node: expected %s, got %s", c.nodeID, fromNode)
	}

	// Check if we have a message handler
	if c.msgHandler == nil {
		return nil, fmt.Errorf("no message handler configured")
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

// loadSubscriptionCache loads all subscriptions from etcd into the local cache.
func (c *EtcdCluster) loadSubscriptionCache() error {
	ctx := context.Background()
	resp, err := c.client.Get(ctx, subscriptionsPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to load subscriptions: %w", err)
	}

	c.subCacheMu.Lock()
	defer c.subCacheMu.Unlock()

	for _, kv := range resp.Kvs {
		var sub storage.Subscription
		if err := json.Unmarshal(kv.Value, &sub); err != nil {
			c.logger.Warn("failed to unmarshal subscription during cache load", slog.String("error", err.Error()))
			continue
		}

		cacheKey := fmt.Sprintf("%s|%s", sub.ClientID, sub.Filter)
		c.subCache[cacheKey] = &sub
	}

	c.logger.Info("loaded subscriptions into cache", slog.Int("cache_len", len(c.subCache)))
	return nil
}

// watchSubscriptions watches etcd for subscription changes and updates the local cache.
func (c *EtcdCluster) watchSubscriptions() {
	watchCh := c.client.Watch(context.Background(), subscriptionsPrefix, clientv3.WithPrefix())

	for {
		select {
		case <-c.stopCh:
			return
		case watchResp := <-watchCh:
			if watchResp.Err() != nil {
				c.logger.Error("subscription watch error", slog.String("error", watchResp.Err().Error()))
				continue
			}

			c.subCacheMu.Lock()
			for _, event := range watchResp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					// Subscription added or updated
					var sub storage.Subscription
					if err := json.Unmarshal(event.Kv.Value, &sub); err != nil {
						c.logger.Error("failed to unmarshal subscription in watch", slog.String("error", err.Error()))
						continue
					}

					cacheKey := fmt.Sprintf("%s|%s", sub.ClientID, sub.Filter)
					c.subCache[cacheKey] = &sub

				case clientv3.EventTypeDelete:
					// Subscription removed
					// Parse key to extract clientID and filter
					key := string(event.Kv.Key)
					parts := strings.Split(strings.TrimPrefix(key, subscriptionsPrefix), "/")
					if len(parts) >= 2 {
						cacheKey := fmt.Sprintf("%s|%s", parts[0], parts[1])
						delete(c.subCache, cacheKey)
					}
				}
			}
			c.subCacheMu.Unlock()
		}
	}
}
