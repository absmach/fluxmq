// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	clustergrpc "github.com/absmach/mqtt/cluster/grpc"
	"github.com/absmach/mqtt/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/embed"
)

var _ Cluster = (*EtcdCluster)(nil)

// MessageHandler handles messages routed from other brokers.
type MessageHandler interface {
	// DeliverToClient delivers a message to a local MQTT client.
	DeliverToClient(ctx context.Context, clientID, topic string, payload []byte, qos byte, retain bool, dup bool, properties map[string]string) error
}

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

	// Handler for incoming routed messages
	msgHandler MessageHandler

	// Manager for session state operations
	sessionManager SessionManager

	// Local subscription cache for fast topic matching
	subCache   map[string]*storage.Subscription // key: clientID|filter
	subCacheMu sync.RWMutex

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
func NewEtcdCluster(cfg *EtcdConfig) (*EtcdCluster, error) {
	// Create embedded etcd configuration
	eCfg := embed.NewConfig()
	eCfg.Name = cfg.NodeID
	eCfg.Dir = cfg.DataDir

	// Peer URLs (for Raft communication)
	peerURL, err := url.Parse("http://" + cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid bind address: %w", err)
	}
	eCfg.ListenPeerUrls = []url.URL{*peerURL}

	// Advertise URL (what other nodes use to contact this node)
	if cfg.AdvertiseAddr != "" {
		advertiseURL, err := url.Parse("http://" + cfg.AdvertiseAddr)
		if err != nil {
			return nil, fmt.Errorf("invalid advertise address: %w", err)
		}
		eCfg.AdvertisePeerUrls = []url.URL{*advertiseURL}
	} else {
		eCfg.AdvertisePeerUrls = []url.URL{*peerURL}
	}

	// Client URLs (for KV operations)
	clientURL, err := url.Parse("http://" + cfg.ClientAddr)
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
		log.Printf("etcd server is ready on node %s", cfg.NodeID)
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
	sess, err := concurrency.NewSession(client, concurrency.WithTTL(10))
	if err != nil {
		client.Close()
		e.Close()
		return nil, fmt.Errorf("failed to create concurrency session: %w", err)
	}

	// Create election for leadership
	election := concurrency.NewElection(sess, "/mqtt/leader")

	c := &EtcdCluster{
		nodeID:   cfg.NodeID,
		config:   cfg,
		etcd:     e,
		client:   client,
		election: election,
		session:  sess,
		subCache: make(map[string]*storage.Subscription),
		stopCh:   make(chan struct{}),
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
		transport, err := NewTransport(cfg.NodeID, cfg.TransportAddr, c)
		if err != nil {
			client.Close()
			sess.Close()
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
		log.Printf("Warning: failed to load subscription cache: %v", err)
	}

	// Start watching for subscription changes
	go c.watchSubscriptions()

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
						log.Printf("Warning: failed to connect to peer %s: %v", nodeID, err)
					}
				}
			}
		}
	}

	// Campaign for leadership in background
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
		nodes = append(nodes, NodeInfo{
			ID:      member.Name,
			Address: peerURL,
			Healthy: true, // TODO: Add actual health check
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

// campaignLeader attempts to become the cluster leader.
func (c *EtcdCluster) campaignLeader() {
	ctx := context.Background()

	if err := c.election.Campaign(ctx, c.nodeID); err != nil {
		log.Printf("Failed to campaign for leader: %v", err)
		return
	}

	log.Printf("Node %s became cluster leader", c.nodeID)
}

// --- Session Ownership Operations ---

// AcquireSession registers this node as the owner of a session.
func (c *EtcdCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
	key := "/mqtt/sessions/" + clientID + "/owner"

	// Try to acquire with our lease (auto-expires if node dies)
	_, err := c.client.Put(ctx, key, nodeID, clientv3.WithLease(c.sessionLease))
	return err
}

// ReleaseSession releases ownership of a session.
func (c *EtcdCluster) ReleaseSession(ctx context.Context, clientID string) error {
	key := "/mqtt/sessions/" + clientID + "/owner"
	_, err := c.client.Delete(ctx, key)
	return err
}

// GetSessionOwner returns the node ID that owns the session.
func (c *EtcdCluster) GetSessionOwner(ctx context.Context, clientID string) (string, bool, error) {
	key := "/mqtt/sessions/" + clientID + "/owner"

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
	key := "/mqtt/sessions/" + clientID + "/owner"
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

// --- Subscription Operations ---

// AddSubscription adds a subscription to the cluster store.
func (c *EtcdCluster) AddSubscription(ctx context.Context, clientID, filter string, qos byte, opts storage.SubscribeOptions) error {
	key := fmt.Sprintf("/mqtt/subscriptions/%s/%s", clientID, filter)

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
	key := fmt.Sprintf("/mqtt/subscriptions/%s/%s", clientID, filter)
	_, err := c.client.Delete(ctx, key)
	return err
}

// GetSubscriptionsForClient returns all subscriptions for a client.
func (c *EtcdCluster) GetSubscriptionsForClient(ctx context.Context, clientID string) ([]*storage.Subscription, error) {
	prefix := "/mqtt/subscriptions/" + clientID + "/"

	resp, err := c.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var subs []*storage.Subscription
	for _, kv := range resp.Kvs {
		var sub storage.Subscription
		if err := json.Unmarshal(kv.Value, &sub); err != nil {
			log.Printf("Failed to unmarshal subscription: %v", err)
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

// --- Retained Message Operations ---

// SetRetained stores a retained message in etcd.
func (c *EtcdCluster) SetRetained(ctx context.Context, topic string, msg *storage.Message) error {
	key := "/mqtt/retained/" + topic

	// Empty payload means delete
	if len(msg.Payload) == 0 {
		return c.DeleteRetained(ctx, topic)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal retained message: %w", err)
	}

	_, err = c.client.Put(ctx, key, string(data))
	return err
}

// GetRetained retrieves a retained message by exact topic.
func (c *EtcdCluster) GetRetained(ctx context.Context, topic string) (*storage.Message, error) {
	key := "/mqtt/retained/" + topic

	resp, err := c.client.Get(ctx, key)
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

// DeleteRetained removes a retained message.
func (c *EtcdCluster) DeleteRetained(ctx context.Context, topic string) error {
	key := "/mqtt/retained/" + topic
	_, err := c.client.Delete(ctx, key)
	return err
}

// GetRetainedMatching returns all retained messages matching a filter.
func (c *EtcdCluster) GetRetainedMatching(ctx context.Context, filter string) ([]*storage.Message, error) {
	// Get all retained messages
	resp, err := c.client.Get(ctx, "/mqtt/retained/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var matched []*storage.Message
	for _, kv := range resp.Kvs {
		// Extract topic from key (remove "/mqtt/retained/" prefix)
		topic := string(kv.Key)[len("/mqtt/retained/"):]

		// Check if topic matches the filter
		if topicMatchesFilter(topic, filter) {
			var msg storage.Message
			if err := json.Unmarshal(kv.Value, &msg); err != nil {
				log.Printf("Failed to unmarshal retained message: %v", err)
				continue
			}
			matched = append(matched, &msg)
		}
	}

	return matched, nil
}

// --- Will Message Operations ---

// SetWill stores a will message in etcd.
func (c *EtcdCluster) SetWill(ctx context.Context, clientID string, will *storage.WillMessage) error {
	key := "/mqtt/wills/" + clientID

	willEntry := struct {
		Will           *storage.WillMessage `json:"will"`
		DisconnectedAt time.Time            `json:"disconnected_at"`
	}{
		Will:           will,
		DisconnectedAt: time.Now(),
	}

	data, err := json.Marshal(willEntry)
	if err != nil {
		return fmt.Errorf("failed to marshal will message: %w", err)
	}

	_, err = c.client.Put(ctx, key, string(data))
	return err
}

// GetWill retrieves the will message for a client.
func (c *EtcdCluster) GetWill(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	key := "/mqtt/wills/" + clientID

	resp, err := c.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, storage.ErrNotFound
	}

	var willEntry struct {
		Will           *storage.WillMessage `json:"will"`
		DisconnectedAt time.Time            `json:"disconnected_at"`
	}

	if err := json.Unmarshal(resp.Kvs[0].Value, &willEntry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal will message: %w", err)
	}

	return willEntry.Will, nil
}

// DeleteWill removes the will message for a client.
func (c *EtcdCluster) DeleteWill(ctx context.Context, clientID string) error {
	key := "/mqtt/wills/" + clientID
	_, err := c.client.Delete(ctx, key)
	return err
}

// GetPendingWills returns will messages that should be triggered.
func (c *EtcdCluster) GetPendingWills(ctx context.Context) ([]*storage.WillMessage, error) {
	// Get all will messages
	resp, err := c.client.Get(ctx, "/mqtt/wills/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var pending []*storage.WillMessage
	now := time.Now()

	for _, kv := range resp.Kvs {
		var willEntry struct {
			Will           *storage.WillMessage `json:"will"`
			DisconnectedAt time.Time            `json:"disconnected_at"`
		}

		if err := json.Unmarshal(kv.Value, &willEntry); err != nil {
			log.Printf("Failed to unmarshal will entry: %v", err)
			continue
		}

		// Check if will delay has elapsed
		if !willEntry.DisconnectedAt.IsZero() {
			triggerTime := willEntry.DisconnectedAt.Add(time.Duration(willEntry.Will.Delay) * time.Second)
			if now.After(triggerTime) || now.Equal(triggerTime) {
				pending = append(pending, willEntry.Will)
			}
		}
	}

	return pending, nil
}

// --- Inter-Broker Communication ---

// SetMessageHandler sets the handler for incoming routed messages.
func (c *EtcdCluster) SetMessageHandler(handler MessageHandler) {
	c.msgHandler = handler
}

// SetSessionManager sets the handler for session state operations.
func (c *EtcdCluster) SetSessionManager(manager SessionManager) {
	c.sessionManager = manager
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
			log.Printf("Failed to get session owner for %s: %v", sub.ClientID, err)
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
				log.Printf("Failed to route publish to %s on node %s: %v", clientID, nodeID, err)
			}
		}
	}

	return nil
}

// TakeoverSession initiates session takeover from one node to another.
func (c *EtcdCluster) TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) (*clustergrpc.SessionState, error) {
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

	log.Printf("Session %s taken over from %s to %s", clientID, fromNode, toNode)
	return state, nil
}

// --- TransportHandler Implementation ---

// HandlePublish implements TransportHandler.HandlePublish.
// Called when another broker routes a PUBLISH message to this node.
func (c *EtcdCluster) HandlePublish(ctx context.Context, clientID, topic string, payload []byte, qos byte, retain bool, dup bool, properties map[string]string) error {
	if c.msgHandler == nil {
		return fmt.Errorf("no message handler configured")
	}

	return c.msgHandler.DeliverToClient(ctx, clientID, topic, payload, qos, retain, dup, properties)
}

// HandleTakeover implements TransportHandler.HandleTakeover.
// Called when another broker requests to take over a session from this node.
func (c *EtcdCluster) HandleTakeover(ctx context.Context, clientID, fromNode, toNode string, state *clustergrpc.SessionState) (*clustergrpc.SessionState, error) {
	// Verify this is the node being asked to give up the session
	if fromNode != c.nodeID {
		return nil, fmt.Errorf("takeover request for wrong node: expected %s, got %s", c.nodeID, fromNode)
	}

	// Check if we have a session manager
	if c.sessionManager == nil {
		return nil, fmt.Errorf("no session manager configured")
	}

	// Get session state and close the session
	sessionState, err := c.sessionManager.GetSessionStateAndClose(ctx, clientID)
	if err != nil {
		return nil, fmt.Errorf("failed to get session state: %w", err)
	}

	log.Printf("Session %s handed over from %s to %s", clientID, fromNode, toNode)
	return sessionState, nil
}

// loadSubscriptionCache loads all subscriptions from etcd into the local cache.
func (c *EtcdCluster) loadSubscriptionCache() error {
	ctx := context.Background()
	resp, err := c.client.Get(ctx, "/mqtt/subscriptions/", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to load subscriptions: %w", err)
	}

	c.subCacheMu.Lock()
	defer c.subCacheMu.Unlock()

	for _, kv := range resp.Kvs {
		var sub storage.Subscription
		if err := json.Unmarshal(kv.Value, &sub); err != nil {
			log.Printf("Failed to unmarshal subscription during cache load: %v", err)
			continue
		}

		cacheKey := fmt.Sprintf("%s|%s", sub.ClientID, sub.Filter)
		c.subCache[cacheKey] = &sub
	}

	log.Printf("Loaded %d subscriptions into cache", len(c.subCache))
	return nil
}

// watchSubscriptions watches etcd for subscription changes and updates the local cache.
func (c *EtcdCluster) watchSubscriptions() {
	watchCh := c.client.Watch(context.Background(), "/mqtt/subscriptions/", clientv3.WithPrefix())

	for {
		select {
		case <-c.stopCh:
			return
		case watchResp := <-watchCh:
			if watchResp.Err() != nil {
				log.Printf("Subscription watch error: %v", watchResp.Err())
				continue
			}

			c.subCacheMu.Lock()
			for _, event := range watchResp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					// Subscription added or updated
					var sub storage.Subscription
					if err := json.Unmarshal(event.Kv.Value, &sub); err != nil {
						log.Printf("Failed to unmarshal subscription in watch: %v", err)
						continue
					}

					cacheKey := fmt.Sprintf("%s|%s", sub.ClientID, sub.Filter)
					c.subCache[cacheKey] = &sub

				case clientv3.EventTypeDelete:
					// Subscription removed
					// Parse key to extract clientID and filter
					key := string(event.Kv.Key)
					parts := strings.Split(strings.TrimPrefix(key, "/mqtt/subscriptions/"), "/")
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
