// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/absmach/fluxmq/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	willDataPrefix  = "/mqtt/will-data/"  // For small wills (<threshold) with full payload
	willIndexPrefix = "/mqtt/will-index/" // For large wills (≥threshold) metadata only
)

// WillMetadata contains metadata about a will message stored in etcd.
type WillMetadata struct {
	NodeID         string    `json:"node_id"`
	ClientID       string    `json:"client_id"`
	Size           int       `json:"size"`
	Replicated     bool      `json:"replicated"` // true = small will replicated, false = large will fetch-on-demand
	DisconnectedAt time.Time `json:"disconnected_at"`
	Delay          uint32    `json:"delay"` // Delay in seconds before will should be published
}

// WillDataEntry contains both metadata and payload for small replicated wills.
type WillDataEntry struct {
	Metadata WillMetadata         `json:"metadata"`
	Will     *storage.WillMessage `json:"will"` // Full will message
}

// WillStore implements storage.WillStore using hybrid storage strategy.
type WillStore struct {
	nodeID        string
	localStore    storage.WillStore // BadgerDB for local payload storage
	etcdClient    *clientv3.Client
	transport     *Transport
	sizeThreshold int

	// Metadata cache (synced from etcd). dataRev/indexRev are the etcd
	// revisions the cache was loaded at; the watches resume from them so no
	// event is missed between load and watch registration.
	metadataCache   map[string]*WillMetadata // key: clientID
	dataRev         int64
	indexRev        int64
	metadataCacheMu sync.RWMutex

	logger *slog.Logger
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewWillStore creates a new hybrid will message store.
func NewWillStore(
	nodeID string,
	localStore storage.WillStore,
	etcdClient *clientv3.Client,
	transport *Transport,
	sizeThreshold int,
	logger *slog.Logger,
) *WillStore {
	if sizeThreshold <= 0 {
		sizeThreshold = defaultSizeThreshold
	}

	h := &WillStore{
		nodeID:        nodeID,
		localStore:    localStore,
		etcdClient:    etcdClient,
		transport:     transport,
		sizeThreshold: sizeThreshold,
		metadataCache: make(map[string]*WillMetadata),
		logger:        logger,
		stopCh:        make(chan struct{}),
	}

	// Load existing metadata before watching so restarts see wills set
	// while this node was down.
	if err := h.loadMetadataCache(); err != nil {
		logger.Warn("failed to load will metadata cache", slog.String("error", err.Error()))
	}

	// Start background watchers for etcd updates
	h.wg.Add(1)
	go h.watchWillData()

	return h
}

// loadMetadataCache rebuilds the metadata cache from both etcd prefixes and
// records the revisions the watches must resume from.
func (h *WillStore) loadMetadataCache() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dataResp, err := h.etcdClient.Get(ctx, willDataPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to load will data entries: %w", err)
	}
	indexResp, err := h.etcdClient.Get(ctx, willIndexPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to load will index entries: %w", err)
	}

	fresh := make(map[string]*WillMetadata, len(dataResp.Kvs)+len(indexResp.Kvs))
	for _, kv := range dataResp.Kvs {
		clientID := string(kv.Key)[len(willDataPrefix):]
		var entry WillDataEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			h.logger.Warn("failed to unmarshal will data entry during load",
				slog.String("client_id", clientID),
				slog.String("error", err.Error()))
			continue
		}
		fresh[clientID] = &entry.Metadata
		h.storeReplicatedWill(ctx, clientID, &entry)
	}
	for _, kv := range indexResp.Kvs {
		clientID := string(kv.Key)[len(willIndexPrefix):]
		var metadata WillMetadata
		if err := json.Unmarshal(kv.Value, &metadata); err != nil {
			h.logger.Warn("failed to unmarshal will metadata during load",
				slog.String("client_id", clientID),
				slog.String("error", err.Error()))
			continue
		}
		fresh[clientID] = &metadata
	}

	h.metadataCacheMu.Lock()
	h.metadataCache = fresh
	h.dataRev = dataResp.Header.Revision
	h.indexRev = indexResp.Header.Revision
	h.metadataCacheMu.Unlock()

	h.logger.Info("loaded will metadata into cache", slog.Int("count", len(fresh)))
	return nil
}

// Set stores a will message using the hybrid strategy.
func (h *WillStore) Set(ctx context.Context, clientID string, will *storage.WillMessage) error {
	// Always write to local BadgerDB first
	if err := h.localStore.Set(ctx, clientID, will); err != nil {
		return fmt.Errorf("failed to write to local store: %w", err)
	}

	// Calculate payload size
	payloadSize := len(will.Payload)

	// Create metadata
	metadata := &WillMetadata{
		NodeID:         h.nodeID,
		ClientID:       clientID,
		Size:           payloadSize,
		Replicated:     payloadSize < h.sizeThreshold,
		DisconnectedAt: time.Now(),
		Delay:          will.Delay,
	}

	// Update local metadata cache
	h.metadataCacheMu.Lock()
	h.metadataCache[clientID] = metadata
	h.metadataCacheMu.Unlock()

	// Publish to etcd based on size
	if metadata.Replicated {
		// Small will: replicate full payload to all nodes
		return h.publishReplicatedWill(ctx, clientID, will, metadata)
	}

	// Large will: publish only metadata
	return h.publishMetadata(ctx, clientID, metadata)
}

// Get retrieves a will message, fetching from remote nodes if necessary.
func (h *WillStore) Get(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	// Try local store first (fast path)
	will, err := h.localStore.Get(ctx, clientID)
	if err == nil && will != nil {
		return will, nil
	}

	// Check metadata cache for remote location
	h.metadataCacheMu.RLock()
	metadata, exists := h.metadataCache[clientID]
	h.metadataCacheMu.RUnlock()

	if !exists {
		// Not in cache, doesn't exist
		return nil, storage.ErrNotFound
	}

	if metadata.NodeID == h.nodeID {
		// We own it but it's not in local store - deleted or error
		return nil, storage.ErrNotFound
	}

	// Remote will - fetch if large (small should already be replicated)
	if !metadata.Replicated {
		return h.fetchRemoteWill(ctx, clientID, metadata.NodeID)
	}

	// Small will should have been replicated, if not found it's deleted
	return nil, storage.ErrNotFound
}

// Delete removes a will message.
func (h *WillStore) Delete(ctx context.Context, clientID string) error {
	// Delete from local store
	if err := h.localStore.Delete(ctx, clientID); err != nil && err != storage.ErrNotFound {
		return fmt.Errorf("failed to delete from local store: %w", err)
	}

	// Delete from etcd (both data and index prefixes)
	if err := h.deleteFromEtcd(ctx, clientID); err != nil {
		return fmt.Errorf("failed to delete from etcd: %w", err)
	}

	// Remove from local metadata cache
	h.metadataCacheMu.Lock()
	delete(h.metadataCache, clientID)
	h.metadataCacheMu.Unlock()

	return nil
}

// GetPending returns all pending will messages (where delay has elapsed).
func (h *WillStore) GetPending(ctx context.Context, before time.Time) ([]*storage.WillMessage, error) {
	var pending []*storage.WillMessage
	var fetchErrors []error

	// Get pending will metadata from cache
	h.metadataCacheMu.RLock()
	var pendingClientIDs []string
	for clientID, metadata := range h.metadataCache {
		// Check if will delay has elapsed
		triggerTime := metadata.DisconnectedAt.Add(time.Duration(metadata.Delay) * time.Second)
		if triggerTime.Before(before) || triggerTime.Equal(before) {
			pendingClientIDs = append(pendingClientIDs, clientID)
		}
	}
	h.metadataCacheMu.RUnlock()

	// Fetch each pending will
	for _, clientID := range pendingClientIDs {
		will, err := h.Get(ctx, clientID)
		if err != nil {
			if err != storage.ErrNotFound {
				fetchErrors = append(fetchErrors, fmt.Errorf("client %s: %w", clientID, err))
			}
			continue
		}
		pending = append(pending, will)
	}

	// Log warnings for partial failures but still return successful fetches
	if len(fetchErrors) > 0 {
		h.logger.Warn("some will messages failed to fetch",
			slog.Int("failed", len(fetchErrors)),
			slog.Int("succeeded", len(pending)))
	}

	return pending, nil
}

// Close stops the hybrid will store and cleans up resources.
func (h *WillStore) Close() error {
	close(h.stopCh)
	h.wg.Wait()
	return nil
}

// publishReplicatedWill publishes a small will with full payload to etcd.
func (h *WillStore) publishReplicatedWill(ctx context.Context, clientID string, will *storage.WillMessage, metadata *WillMetadata) error {
	entry := &WillDataEntry{
		Metadata: *metadata,
		Will:     will,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal will data: %w", err)
	}

	key := willDataPrefix + clientID
	_, err = h.etcdClient.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to publish to etcd: %w", err)
	}

	h.logger.Debug("published replicated will message",
		slog.String("client_id", clientID),
		slog.Int("size", metadata.Size))

	return nil
}

// publishMetadata publishes only metadata for large wills to etcd.
func (h *WillStore) publishMetadata(ctx context.Context, clientID string, metadata *WillMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	key := willIndexPrefix + clientID
	_, err = h.etcdClient.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to publish metadata to etcd: %w", err)
	}

	h.logger.Debug("published will metadata",
		slog.String("client_id", clientID),
		slog.Int("size", metadata.Size))

	return nil
}

// deleteFromEtcd removes will message entries from both prefixes in etcd.
func (h *WillStore) deleteFromEtcd(ctx context.Context, clientID string) error {
	// Try both prefixes (we don't know which one it's in)
	dataKey := willDataPrefix + clientID
	indexKey := willIndexPrefix + clientID

	_, err1 := h.etcdClient.Delete(ctx, dataKey)
	_, err2 := h.etcdClient.Delete(ctx, indexKey)

	// Return error only if both failed
	if err1 != nil && err2 != nil {
		return fmt.Errorf("failed to delete from etcd: data=%w, index=%w", err1, err2)
	}

	return nil
}

// fetchRemoteWill fetches a large will message from a remote node via gRPC.
func (h *WillStore) fetchRemoteWill(ctx context.Context, clientID, nodeID string) (*storage.WillMessage, error) {
	if h.transport == nil {
		return nil, fmt.Errorf("no transport configured, cannot fetch from node %s", nodeID)
	}

	h.logger.Debug("fetching will message from remote node",
		slog.String("client_id", clientID),
		slog.String("node_id", nodeID))

	// Fetch via gRPC
	grpcWill, err := h.transport.SendFetchWill(ctx, nodeID, clientID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch will from node %s: %w", nodeID, err)
	}

	// Not found on remote node (not an error, just return nil)
	if grpcWill == nil {
		return nil, nil
	}

	// Convert grpc.WillMessage to storage.WillMessage
	will := &storage.WillMessage{
		Topic:   grpcWill.Topic,
		Payload: grpcWill.Payload,
		QoS:     byte(grpcWill.Qos),
		Retain:  grpcWill.Retain,
		Delay:   grpcWill.Delay,
	}

	// Cache fetched will locally for future access
	if err := h.localStore.Set(ctx, clientID, will); err != nil {
		h.logger.Warn("failed to cache fetched will message locally",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
	}

	return will, nil
}

// watchWillData watches etcd for will message updates and updates local cache/store.
// watchWillData watches etcd for will updates and updates local cache/store.
// Watches resume from the cache-load revisions; any channel close or watch
// error triggers a cache reload and a re-watch so no event is lost across
// the interruption.
func (h *WillStore) watchWillData() {
	defer h.wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		h.metadataCacheMu.RLock()
		dataRev, indexRev := h.dataRev, h.indexRev
		h.metadataCacheMu.RUnlock()

		// Per-iteration context: when one stream breaks, the surviving
		// watch must be cancelled too, or abandoned watches accumulate on
		// the etcd server across restarts.
		iterCtx, iterCancel := context.WithCancel(ctx)
		dataChan := h.etcdClient.Watch(iterCtx, willDataPrefix, prefixWatchOpts(dataRev)...)
		indexChan := h.etcdClient.Watch(iterCtx, willIndexPrefix, prefixWatchOpts(indexRev)...)

		alive := h.consumeWatchEvents(dataChan, indexChan)
		iterCancel()
		if !alive {
			return
		}

		if err := h.loadMetadataCache(); err != nil {
			h.logger.Error("failed to reload will metadata cache",
				slog.String("error", err.Error()))
		}
		select {
		case <-h.stopCh:
			return
		case <-time.After(time.Second):
		}
	}
}

// consumeWatchEvents processes both watch streams until one breaks. Returns
// false when the store is shutting down, true when the watches must be
// re-established.
func (h *WillStore) consumeWatchEvents(dataChan, indexChan clientv3.WatchChan) bool {
	for {
		select {
		case <-h.stopCh:
			return false

		case resp, ok := <-dataChan:
			if !ok || resp.Err() != nil {
				h.logWatchBreak("will-data", ok, resp)
				return true
			}
			h.handleDataWatchEvents(resp.Events)

		case resp, ok := <-indexChan:
			if !ok || resp.Err() != nil {
				h.logWatchBreak("will-index", ok, resp)
				return true
			}
			h.handleIndexWatchEvents(resp.Events)
		}
	}
}

func (h *WillStore) logWatchBreak(name string, ok bool, resp clientv3.WatchResponse) {
	if !ok {
		h.logger.Warn("watch channel closed, reloading cache and re-watching",
			slog.String("watch", name))
		return
	}
	h.logger.Error("watch error, reloading cache and re-watching",
		slog.String("watch", name),
		slog.String("error", resp.Err().Error()))
}

// handleDataWatchEvents processes watch events for replicated small wills.
func (h *WillStore) handleDataWatchEvents(events []*clientv3.Event) {
	for _, ev := range events {
		clientID := string(ev.Kv.Key)[len(willDataPrefix):]

		if ev.Type == clientv3.EventTypeDelete {
			// Delete from local store and cache
			ctx := context.Background()
			_ = h.localStore.Delete(ctx, clientID)

			h.metadataCacheMu.Lock()
			delete(h.metadataCache, clientID)
			h.metadataCacheMu.Unlock()
			continue
		}

		// Parse replicated data entry
		var entry WillDataEntry
		if err := json.Unmarshal(ev.Kv.Value, &entry); err != nil {
			h.logger.Warn("failed to unmarshal will data entry",
				slog.String("client_id", clientID),
				slog.String("error", err.Error()))
			continue
		}

		// Update metadata cache
		h.metadataCacheMu.Lock()
		h.metadataCache[clientID] = &entry.Metadata
		h.metadataCacheMu.Unlock()

		// Own writes are already in the local store.
		if entry.Metadata.NodeID != h.nodeID {
			h.storeReplicatedWill(context.Background(), clientID, &entry)
		}
	}
}

// storeReplicatedWill persists a small replicated will into the local store.
// Callers decide whether own-node entries apply: the watch path skips them
// (Set already wrote the will locally), while the startup load includes them
// so a node restarting with a fresh local store recovers its own replicated
// wills from etcd.
func (h *WillStore) storeReplicatedWill(ctx context.Context, clientID string, entry *WillDataEntry) {
	if !entry.Metadata.Replicated {
		return
	}

	if err := h.localStore.Set(ctx, clientID, entry.Will); err != nil {
		h.logger.Warn("failed to store replicated will locally",
			slog.String("client_id", clientID),
			slog.String("error", err.Error()))
	}
}

// handleIndexWatchEvents processes watch events for large will metadata.
func (h *WillStore) handleIndexWatchEvents(events []*clientv3.Event) {
	for _, ev := range events {
		clientID := string(ev.Kv.Key)[len(willIndexPrefix):]

		if ev.Type == clientv3.EventTypeDelete {
			h.metadataCacheMu.Lock()
			delete(h.metadataCache, clientID)
			h.metadataCacheMu.Unlock()
			continue
		}

		// Parse metadata
		var metadata WillMetadata
		if err := json.Unmarshal(ev.Kv.Value, &metadata); err != nil {
			h.logger.Warn("failed to unmarshal will metadata",
				slog.String("client_id", clientID),
				slog.String("error", err.Error()))
			continue
		}

		// Update metadata cache
		h.metadataCacheMu.Lock()
		h.metadataCache[clientID] = &metadata
		h.metadataCacheMu.Unlock()
	}
}
