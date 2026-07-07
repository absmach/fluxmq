// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
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
		ProxyNodeID:  "other-node",
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
