// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/topics"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	retainedDataPrefix   = "/mqtt/retained-data/"  // For small messages (<1KB) with full payload
	retainedIndexPrefix  = "/mqtt/retained-index/" // For large messages (≥1KB) metadata only
	defaultSizeThreshold = 1024                    // 1KB threshold for replication vs fetch-on-demand
)

// RetainedMetadata contains metadata about a retained message stored in etcd.
type RetainedMetadata struct {
	NodeID     string    `json:"node_id"`
	Topic      string    `json:"topic"`
	QoS        byte      `json:"qos"`
	Size       int       `json:"size"`
	Replicated bool      `json:"replicated"` // true = small message replicated, false = large message fetch-on-demand
	Timestamp  time.Time `json:"timestamp"`
}

// RetainedDataEntry contains both metadata and payload for small replicated messages.
type RetainedDataEntry struct {
	Metadata   RetainedMetadata  `json:"metadata"`
	Payload    string            `json:"payload"` // base64 encoded
	Properties map[string]string `json:"properties"`
}

// RetainedStore implements storage.RetainedStore using hybrid storage strategy:
// - Small messages (<1KB): Replicated to all nodes via etcd
// - Large messages (≥1KB): Stored on owner node, fetched on-demand via gRPC.
type RetainedStore struct {
	nodeID        string
	localStore    storage.RetainedStore // BadgerDB for local payload storage
	etcdClient    *clientv3.Client
	transport     *Transport
	sizeThreshold int

	// Metadata cache (synced from etcd). dataRev/indexRev are the etcd
	// revisions the cache was loaded at; the watches resume from them so no
	// event is missed between load and watch registration.
	metadataCache   map[string]*RetainedMetadata // key: topic
	dataRev         int64
	indexRev        int64
	metadataCacheMu sync.RWMutex

	logger *slog.Logger
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewRetainedStore creates a new hybrid retained message store.
func NewRetainedStore(
	nodeID string,
	localStore storage.RetainedStore,
	etcdClient *clientv3.Client,
	transport *Transport,
	sizeThreshold int,
	logger *slog.Logger,
) *RetainedStore {
	if sizeThreshold <= 0 {
		sizeThreshold = defaultSizeThreshold
	}

	h := &RetainedStore{
		nodeID:        nodeID,
		localStore:    localStore,
		etcdClient:    etcdClient,
		transport:     transport,
		sizeThreshold: sizeThreshold,
		metadataCache: make(map[string]*RetainedMetadata),
		logger:        logger,
		stopCh:        make(chan struct{}),
	}

	// Load existing metadata before watching so restarts see retained
	// messages set while this node was down.
	if err := h.loadMetadataCache(); err != nil {
		logger.Warn("failed to load retained metadata cache", slog.String("error", err.Error()))
	}

	// Start background watchers for etcd updates
	h.wg.Add(1)
	go h.watchRetainedData()

	return h
}

// loadMetadataCache rebuilds the metadata cache from both etcd prefixes and
// records the revisions the watches must resume from.
func (h *RetainedStore) loadMetadataCache() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dataResp, err := h.etcdClient.Get(ctx, retainedDataPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to load retained data entries: %w", err)
	}
	indexResp, err := h.etcdClient.Get(ctx, retainedIndexPrefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to load retained index entries: %w", err)
	}

	fresh := make(map[string]*RetainedMetadata, len(dataResp.Kvs)+len(indexResp.Kvs))
	for _, kv := range dataResp.Kvs {
		topic := string(kv.Key)[len(retainedDataPrefix):]
		var entry RetainedDataEntry
		if err := json.Unmarshal(kv.Value, &entry); err != nil {
			h.logger.Warn("failed to unmarshal retained data entry during load",
				slog.String("topic", topic),
				slog.String("error", err.Error()))
			continue
		}
		fresh[topic] = &entry.Metadata
		h.storeReplicatedEntry(ctx, topic, &entry)
	}
	for _, kv := range indexResp.Kvs {
		topic := string(kv.Key)[len(retainedIndexPrefix):]
		var metadata RetainedMetadata
		if err := json.Unmarshal(kv.Value, &metadata); err != nil {
			h.logger.Warn("failed to unmarshal retained metadata during load",
				slog.String("topic", topic),
				slog.String("error", err.Error()))
			continue
		}
		fresh[topic] = &metadata
	}

	h.metadataCacheMu.Lock()
	h.metadataCache = fresh
	h.dataRev = dataResp.Header.Revision
	h.indexRev = indexResp.Header.Revision
	h.metadataCacheMu.Unlock()

	h.logger.Info("loaded retained metadata into cache", slog.Int("count", len(fresh)))
	return nil
}

// Set stores a retained message using the hybrid strategy.
func (h *RetainedStore) Set(ctx context.Context, topic string, msg *storage.Message) error {
	// Empty payload = delete
	if len(msg.Payload) == 0 {
		return h.Delete(ctx, topic)
	}

	// Always write to local BadgerDB first
	if err := h.localStore.Set(ctx, topic, msg); err != nil {
		return fmt.Errorf("failed to write to local store: %w", err)
	}

	// Calculate payload size
	payloadSize := len(msg.Payload)

	// Create metadata
	metadata := &RetainedMetadata{
		NodeID:     h.nodeID,
		Topic:      topic,
		QoS:        msg.QoS,
		Size:       payloadSize,
		Replicated: payloadSize < h.sizeThreshold,
		Timestamp:  time.Now(),
	}

	// Publish to etcd based on size
	var etcdErr error
	if metadata.Replicated {
		etcdErr = h.publishReplicatedMessage(ctx, topic, msg, metadata)
	} else {
		etcdErr = h.publishMetadata(ctx, topic, metadata)
	}
	if etcdErr != nil {
		return etcdErr
	}

	// Update local metadata cache only after successful etcd write
	h.metadataCacheMu.Lock()
	h.metadataCache[topic] = metadata
	h.metadataCacheMu.Unlock()

	return nil
}

// Get retrieves a retained message, fetching from remote nodes if necessary.
func (h *RetainedStore) Get(ctx context.Context, topic string) (*storage.Message, error) {
	// Try local store first (fast path)
	msg, err := h.localStore.Get(ctx, topic)
	if err == nil && msg != nil {
		return msg, nil
	}

	// Check metadata cache for remote location
	h.metadataCacheMu.RLock()
	metadata, exists := h.metadataCache[topic]
	h.metadataCacheMu.RUnlock()

	if !exists {
		// Not in cache, doesn't exist
		return nil, storage.ErrNotFound
	}

	if metadata.NodeID == h.nodeID {
		// We own it but it's not in local store - deleted or error
		return nil, storage.ErrNotFound
	}

	// Remote message - fetch if large (small should already be replicated)
	if !metadata.Replicated {
		return h.fetchRemoteRetained(ctx, topic, metadata.NodeID)
	}

	// Small message should have been replicated, if not found it's deleted
	return nil, storage.ErrNotFound
}

// Delete removes a retained message.
func (h *RetainedStore) Delete(ctx context.Context, topic string) error {
	// Delete from local store
	if err := h.localStore.Delete(ctx, topic); err != nil && err != storage.ErrNotFound {
		return fmt.Errorf("failed to delete from local store: %w", err)
	}

	// Delete from etcd (both data and index prefixes)
	if err := h.deleteFromEtcd(ctx, topic); err != nil {
		return fmt.Errorf("failed to delete from etcd: %w", err)
	}

	// Remove from local metadata cache
	h.metadataCacheMu.Lock()
	delete(h.metadataCache, topic)
	h.metadataCacheMu.Unlock()

	return nil
}

// Match returns all retained messages matching the given topic filter.
func (h *RetainedStore) Match(ctx context.Context, filter string) ([]*storage.Message, error) {
	var messages []*storage.Message
	var fetchErrors []error

	// Get matching topics from metadata cache
	h.metadataCacheMu.RLock()
	var matchingTopics []string
	for topic := range h.metadataCache {
		if topics.TopicMatch(filter, topic) {
			matchingTopics = append(matchingTopics, topic)
		}
	}
	h.metadataCacheMu.RUnlock()

	// Fetch each matching message
	for _, topic := range matchingTopics {
		msg, err := h.Get(ctx, topic)
		if err != nil {
			if err != storage.ErrNotFound {
				fetchErrors = append(fetchErrors, fmt.Errorf("topic %s: %w", topic, err))
			}
			continue
		}
		messages = append(messages, msg)
	}

	// If all fetches failed, return error
	if len(fetchErrors) > 0 && len(messages) == 0 {
		return nil, fmt.Errorf("failed to fetch retained messages: %w", errors.Join(fetchErrors...))
	}

	// Log warnings for partial failures but still return successful fetches
	if len(fetchErrors) > 0 {
		h.logger.Warn("some retained messages failed to fetch",
			slog.Int("failed", len(fetchErrors)),
			slog.Int("succeeded", len(messages)))
	}

	return messages, nil
}

// Close stops the hybrid store and cleans up resources.
func (h *RetainedStore) Close() error {
	close(h.stopCh)
	h.wg.Wait()
	return nil
}

// publishReplicatedMessage publishes a small message with full payload to etcd.
func (h *RetainedStore) publishReplicatedMessage(ctx context.Context, topic string, msg *storage.Message, metadata *RetainedMetadata) error {
	entry := &RetainedDataEntry{
		Metadata:   *metadata,
		Payload:    base64.StdEncoding.EncodeToString(msg.Payload),
		Properties: msg.Properties,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal retained data: %w", err)
	}

	key := retainedDataPrefix + topic
	_, err = h.etcdClient.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to publish to etcd: %w", err)
	}

	h.logger.Debug("published replicated retained message",
		slog.String("topic", topic),
		slog.Int("size", metadata.Size))

	return nil
}

// publishMetadata publishes only metadata for large messages to etcd.
func (h *RetainedStore) publishMetadata(ctx context.Context, topic string, metadata *RetainedMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	key := retainedIndexPrefix + topic
	_, err = h.etcdClient.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to publish metadata to etcd: %w", err)
	}

	h.logger.Debug("published retained metadata",
		slog.String("topic", topic),
		slog.Int("size", metadata.Size))

	return nil
}

// deleteFromEtcd removes retained message entries from both prefixes in etcd.
func (h *RetainedStore) deleteFromEtcd(ctx context.Context, topic string) error {
	// Try both prefixes (we don't know which one it's in)
	dataKey := retainedDataPrefix + topic
	indexKey := retainedIndexPrefix + topic

	_, err1 := h.etcdClient.Delete(ctx, dataKey)
	_, err2 := h.etcdClient.Delete(ctx, indexKey)

	// Return error only if both failed
	if err1 != nil && err2 != nil {
		return fmt.Errorf("failed to delete from etcd: data=%w, index=%w", err1, err2)
	}

	return nil
}

// fetchRemoteRetained fetches a large retained message from a remote node via gRPC.
func (h *RetainedStore) fetchRemoteRetained(ctx context.Context, topic, nodeID string) (*storage.Message, error) {
	if h.transport == nil {
		return nil, fmt.Errorf("no transport configured, cannot fetch from node %s", nodeID)
	}

	h.logger.Debug("fetching retained message from remote node",
		slog.String("topic", topic),
		slog.String("node_id", nodeID))

	grpcMsg, err := h.transport.SendFetchRetained(ctx, nodeID, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from node %s: %w", nodeID, err)
	}

	if grpcMsg == nil {
		return nil, storage.ErrNotFound
	}

	// Convert grpc.RetainedMessage to storage.Message
	msg := &storage.Message{
		Topic:       grpcMsg.Topic,
		Payload:     grpcMsg.Payload,
		QoS:         byte(grpcMsg.Qos),
		Retain:      grpcMsg.Retain,
		Properties:  grpcMsg.Properties,
		PublishTime: time.Unix(grpcMsg.Timestamp, 0),
	}

	// Cache it locally for future reads
	if err := h.localStore.Set(ctx, topic, msg); err != nil {
		h.logger.Warn("failed to cache fetched retained message locally",
			slog.String("topic", topic),
			slog.String("error", err.Error()))
	}

	return msg, nil
}

// watchRetainedData watches etcd for retained message updates and updates
// local cache/store. Watches resume from the cache-load revisions; any
// channel close or watch error triggers a cache reload and a re-watch so no
// event is lost across the interruption.
func (h *RetainedStore) watchRetainedData() {
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
		dataChan := h.etcdClient.Watch(iterCtx, retainedDataPrefix, prefixWatchOpts(dataRev)...)
		indexChan := h.etcdClient.Watch(iterCtx, retainedIndexPrefix, prefixWatchOpts(indexRev)...)

		alive := h.consumeWatchEvents(dataChan, indexChan)
		iterCancel()
		if !alive {
			return
		}

		if err := h.loadMetadataCache(); err != nil {
			h.logger.Error("failed to reload retained metadata cache",
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
func (h *RetainedStore) consumeWatchEvents(dataChan, indexChan clientv3.WatchChan) bool {
	for {
		select {
		case <-h.stopCh:
			return false

		case resp, ok := <-dataChan:
			if !ok || resp.Err() != nil {
				h.logWatchBreak("retained-data", ok, resp)
				return true
			}
			h.handleDataWatchEvents(resp.Events)

		case resp, ok := <-indexChan:
			if !ok || resp.Err() != nil {
				h.logWatchBreak("retained-index", ok, resp)
				return true
			}
			h.handleIndexWatchEvents(resp.Events)
		}
	}
}

func (h *RetainedStore) logWatchBreak(name string, ok bool, resp clientv3.WatchResponse) {
	if !ok {
		h.logger.Warn("watch channel closed, reloading cache and re-watching",
			slog.String("watch", name))
		return
	}
	h.logger.Error("watch error, reloading cache and re-watching",
		slog.String("watch", name),
		slog.String("error", resp.Err().Error()))
}

// handleDataWatchEvents processes watch events for replicated small messages.
func (h *RetainedStore) handleDataWatchEvents(events []*clientv3.Event) {
	for _, ev := range events {
		topic := string(ev.Kv.Key)[len(retainedDataPrefix):]

		if ev.Type == clientv3.EventTypeDelete {
			// Delete from local store and cache
			ctx := context.Background()
			_ = h.localStore.Delete(ctx, topic)

			h.metadataCacheMu.Lock()
			delete(h.metadataCache, topic)
			h.metadataCacheMu.Unlock()
			continue
		}

		// Parse replicated data entry
		var entry RetainedDataEntry
		if err := json.Unmarshal(ev.Kv.Value, &entry); err != nil {
			h.logger.Warn("failed to unmarshal retained data entry",
				slog.String("topic", topic),
				slog.String("error", err.Error()))
			continue
		}

		// Update metadata cache
		h.metadataCacheMu.Lock()
		h.metadataCache[topic] = &entry.Metadata
		h.metadataCacheMu.Unlock()

		// Own writes are already in the local store.
		if entry.Metadata.NodeID != h.nodeID {
			h.storeReplicatedEntry(context.Background(), topic, &entry)
		}
	}
}

// storeReplicatedEntry persists the payload of a small replicated message
// into the local store. Callers decide whether own-node entries apply: the
// watch path skips them (Set already wrote the payload locally), while the
// startup load includes them so a node restarting with a fresh local store
// recovers its own replicated messages from etcd.
func (h *RetainedStore) storeReplicatedEntry(ctx context.Context, topic string, entry *RetainedDataEntry) {
	if !entry.Metadata.Replicated {
		return
	}

	payload, err := base64.StdEncoding.DecodeString(entry.Payload)
	if err != nil {
		h.logger.Warn("failed to decode payload",
			slog.String("topic", topic),
			slog.String("error", err.Error()))
		return
	}

	msg := &storage.Message{
		Topic:       topic,
		Payload:     payload,
		QoS:         entry.Metadata.QoS,
		Retain:      true,
		Properties:  entry.Properties,
		PublishTime: entry.Metadata.Timestamp,
	}

	if err := h.localStore.Set(ctx, topic, msg); err != nil {
		h.logger.Warn("failed to store replicated message locally",
			slog.String("topic", topic),
			slog.String("error", err.Error()))
	}
}

// handleIndexWatchEvents processes watch events for large message metadata.
func (h *RetainedStore) handleIndexWatchEvents(events []*clientv3.Event) {
	for _, ev := range events {
		topic := string(ev.Kv.Key)[len(retainedIndexPrefix):]

		if ev.Type == clientv3.EventTypeDelete {
			h.metadataCacheMu.Lock()
			delete(h.metadataCache, topic)
			h.metadataCacheMu.Unlock()
			continue
		}

		// Parse metadata
		var metadata RetainedMetadata
		if err := json.Unmarshal(ev.Kv.Value, &metadata); err != nil {
			h.logger.Warn("failed to unmarshal retained metadata",
				slog.String("topic", topic),
				slog.String("error", err.Error()))
			continue
		}

		// Update metadata cache
		h.metadataCacheMu.Lock()
		h.metadataCache[topic] = &metadata
		h.metadataCacheMu.Unlock()
	}
}
