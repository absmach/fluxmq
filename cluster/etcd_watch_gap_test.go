// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

// These tests write directly through the raw etcd client right after Start,
// bypassing the cluster API that updates local caches synchronously. The
// watchers must observe the writes even when they land before the watch
// stream is registered — the watch resumes from the cache-load revision, so
// nothing can fall between the load and the watch.

func TestSubscriptionWatchSeesEarlyPut(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientID := "gap-sub-client"
	subs := []storage.Subscription{{ClientID: clientID, Filter: "gap/sub/#", QoS: 1}}
	data, err := json.Marshal(subs)
	require.NoError(t, err)

	_, err = c.client.Put(ctx, subscriptionsPrefix+clientID, string(data))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		matched, err := c.GetSubscribersForTopic(context.Background(), "gap/sub/x")
		if err != nil {
			return false
		}
		for _, sub := range matched {
			if sub.ClientID == clientID {
				return true
			}
		}
		return false
	}, recoveryWait, pollInterval, "subscription written before watch registration never reached the cache")
}

func TestQueueConsumerWatchSeesEarlyPut(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info := QueueConsumerInfo{
		QueueName:    testQueueName,
		GroupID:      testGroupID,
		ConsumerID:   "gap-consumer",
		ClientID:     "gap-consumer-client",
		Pattern:      "#",
		Mode:         testConsumerMode,
		ProxyNodeID:  testOtherNode,
		RegisteredAt: time.Now(),
	}
	data, err := json.Marshal(info)
	require.NoError(t, err)

	key := fmt.Sprintf("%s%s/%s/%s", queueConsumersPrefix, info.QueueName, info.GroupID, info.ConsumerID)
	_, err = c.client.Put(ctx, key, string(data))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		consumers, err := c.ListQueueConsumers(context.Background(), testQueueName)
		if err != nil {
			return false
		}
		for _, consumer := range consumers {
			if consumer.ConsumerID == info.ConsumerID {
				return true
			}
		}
		return false
	}, recoveryWait, pollInterval, "queue consumer written before watch registration never reached the cache")
}

func retainedDataEntryJSON(t *testing.T, nodeID, topic, payload string) string {
	t.Helper()

	entry := RetainedDataEntry{
		Metadata: RetainedMetadata{
			NodeID:     nodeID,
			Topic:      topic,
			QoS:        1,
			Size:       len(payload),
			Replicated: true,
			Timestamp:  time.Now(),
		},
		Payload: base64.StdEncoding.EncodeToString([]byte(payload)),
	}
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	return string(data)
}

func willDataEntryJSON(t *testing.T, nodeID, clientID string) string {
	t.Helper()

	entry := WillDataEntry{
		Metadata: WillMetadata{
			NodeID:         nodeID,
			ClientID:       clientID,
			Replicated:     true,
			DisconnectedAt: time.Now().Add(-time.Minute),
			Delay:          0,
		},
		Will: &storage.WillMessage{
			ClientID: clientID,
			Topic:    "will/" + clientID,
			Payload:  []byte("gone"),
			QoS:      1,
		},
	}
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	return string(data)
}

func TestRetainedStoreWatchSeesEarlyPut(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	require.NotNil(t, c.hybridRetained)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "gap/retained-store"
	_, err := c.client.Put(ctx, retainedDataPrefix+topic, retainedDataEntryJSON(t, testOtherNode, topic, "hello"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		msg, err := c.hybridRetained.Get(context.Background(), topic)
		return err == nil && msg != nil && string(msg.Payload) == "hello"
	}, recoveryWait, pollInterval, "retained entry written before watch registration never became readable")
}

func TestRetainedStoreLoadSeesPreexistingEntries(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	require.NotNil(t, c.hybridRetained)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "gap/retained-preexisting"
	_, err := c.client.Put(ctx, retainedDataPrefix+topic, retainedDataEntryJSON(t, testOtherNode, topic, "old"))
	require.NoError(t, err)

	// Reload as a restarting node would; the entry must become readable
	// even if the watch never saw the put.
	require.NoError(t, c.hybridRetained.loadMetadataCache())

	msg, err := c.hybridRetained.Get(ctx, topic)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.Equal(t, "old", string(msg.Payload))
}

func TestRetainedStoreLoadRestoresOwnNodeEntries(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	require.NotNil(t, c.hybridRetained)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Entry owned by this node, but absent from the local store — the
	// cold-start case where the node restarted with a fresh disk while its
	// replicated entries survived in etcd.
	topic := "gap/retained-own-node"
	_, err := c.client.Put(ctx, retainedDataPrefix+topic, retainedDataEntryJSON(t, c.nodeID, topic, "mine"))
	require.NoError(t, err)

	require.NoError(t, c.hybridRetained.loadMetadataCache())

	msg, err := c.hybridRetained.Get(ctx, topic)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.Equal(t, "mine", string(msg.Payload))
}

func TestWillStoreLoadRestoresOwnNodeEntries(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	require.NotNil(t, c.hybridWill)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientID := "gap-will-own-node"
	_, err := c.client.Put(ctx, willDataPrefix+clientID, willDataEntryJSON(t, c.nodeID, clientID))
	require.NoError(t, err)

	require.NoError(t, c.hybridWill.loadMetadataCache())

	pending, err := c.hybridWill.GetPending(ctx, time.Now())
	require.NoError(t, err)
	found := false
	for _, will := range pending {
		if will.ClientID == clientID {
			found = true
			break
		}
	}
	require.True(t, found, "own-node will not restored after cache load")
}

func TestWillStoreWatchSeesEarlyPut(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	require.NotNil(t, c.hybridWill)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientID := "gap-will-client"
	_, err := c.client.Put(ctx, willDataPrefix+clientID, willDataEntryJSON(t, testOtherNode, clientID))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		pending, err := c.hybridWill.GetPending(context.Background(), time.Now())
		if err != nil {
			return false
		}
		for _, will := range pending {
			if will.ClientID == clientID {
				return true
			}
		}
		return false
	}, recoveryWait, pollInterval, "will written before watch registration never became pending")
}

func TestWillStoreLoadSeesPreexistingEntries(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	require.NotNil(t, c.hybridWill)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientID := "gap-will-preexisting"
	_, err := c.client.Put(ctx, willDataPrefix+clientID, willDataEntryJSON(t, testOtherNode, clientID))
	require.NoError(t, err)

	require.NoError(t, c.hybridWill.loadMetadataCache())

	pending, err := c.hybridWill.GetPending(ctx, time.Now())
	require.NoError(t, err)
	found := false
	for _, will := range pending {
		if will.ClientID == clientID {
			found = true
			break
		}
	}
	require.True(t, found, "preexisting will not visible after cache load")
}

func TestRetainedWatchSeesEarlyPut(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "gap/retained"
	msg := storage.Message{Topic: topic, Payload: []byte("hello"), QoS: 1}
	data, err := json.Marshal(msg)
	require.NoError(t, err)

	_, err = c.client.Put(ctx, retainedPrefix+topic, string(data))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		c.retainedCacheMu.RLock()
		defer c.retainedCacheMu.RUnlock()
		return c.retainedCache[topic] != nil
	}, recoveryWait, pollInterval, "retained message written before watch registration never reached the cache")
}

func TestRetainedStoreSetHandlesBufferBackedPayload(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	require.NotNil(t, c.hybridRetained)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish-path messages carry the payload in PayloadBuf with the
	// legacy Payload field cleared.
	topic := "gap/retained-buffer"
	msg := &storage.Message{Topic: topic, QoS: 1, Retain: true}
	msg.SetPayloadFromBytes([]byte("buffered"))

	require.NoError(t, c.hybridRetained.Set(ctx, topic, msg))
	msg.ReleasePayload()

	// The replicated etcd entry must contain the payload.
	resp, err := c.client.Get(ctx, retainedDataPrefix+topic)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	var entry RetainedDataEntry
	require.NoError(t, json.Unmarshal(resp.Kvs[0].Value, &entry))
	decoded, err := base64.StdEncoding.DecodeString(entry.Payload)
	require.NoError(t, err)
	require.Equal(t, "buffered", string(decoded))

	// And the message must be readable after the pooled buffer is gone.
	got, err := c.hybridRetained.Get(ctx, topic)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "buffered", string(got.GetPayload()))
}

func TestRetainedCacheReloadEvictsStaleEntries(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	// Simulate a topic whose delete event was missed while the watch was
	// down: present in the cache, absent from etcd.
	stale := "gap/retained-stale"
	c.retainedCacheMu.Lock()
	c.retainedCache[stale] = &storage.Message{Topic: stale}
	c.retainedCacheMu.Unlock()

	require.NoError(t, c.loadRetainedCache())

	c.retainedCacheMu.RLock()
	defer c.retainedCacheMu.RUnlock()
	require.NotContains(t, c.retainedCache, stale, "reload must evict entries deleted while the watch was down")
}
