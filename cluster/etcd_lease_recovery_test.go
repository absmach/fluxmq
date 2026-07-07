// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	recoveryWait = 15 * time.Second
	pollInterval = 100 * time.Millisecond

	testQueueName    = "events"
	testGroupID      = "workers@#"
	testConsumerMode = "stream"
	testOtherNode    = "other-node"
)

func sessionOwnerKey(clientID string) string {
	return sessionsPrefix + clientID + "/owner"
}

func etcdKeyExists(t *testing.T, c *EtcdCluster, key string) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.client.Get(ctx, key)
	require.NoError(t, err)
	return len(resp.Kvs) == 1
}

func currentLease(c *EtcdCluster) int64 {
	c.leaseMu.Lock()
	defer c.leaseMu.Unlock()
	return int64(c.sessionLease)
}

func TestLeaseRevocationReregistersAllKeys(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientIDs := []string{"lease-recovery-client-1", "lease-recovery-client-2"}
	for _, clientID := range clientIDs {
		require.NoError(t, c.AcquireSession(ctx, clientID, c.nodeID))
	}

	info := &QueueConsumerInfo{
		QueueName:    testQueueName,
		GroupID:      testGroupID,
		ConsumerID:   "consumer-1",
		ClientID:     clientIDs[0],
		Pattern:      "#",
		Mode:         testConsumerMode,
		ProxyNodeID:  c.nodeID,
		RegisteredAt: time.Now(),
	}
	require.NoError(t, c.RegisterQueueConsumer(ctx, info))
	consumerKey := fmt.Sprintf("%s%s/%s/%s", queueConsumersPrefix, info.QueueName, info.GroupID, info.ConsumerID)

	oldLease := currentLease(c)
	_, err := c.client.Revoke(ctx, clientv3.LeaseID(oldLease))
	require.NoError(t, err)

	// The keepalive goroutine must detect the dead lease, grant a new one,
	// and re-register every tracked key without any client reconnecting.
	require.Eventually(t, func() bool {
		if currentLease(c) == oldLease {
			return false
		}
		for _, clientID := range clientIDs {
			if !etcdKeyExists(t, c, sessionOwnerKey(clientID)) {
				return false
			}
		}
		return etcdKeyExists(t, c, consumerKey)
	}, recoveryWait, pollInterval, "leased keys were not re-registered after lease revocation")

	// Owner cache must still resolve the sessions to this node.
	for _, clientID := range clientIDs {
		owner, ok, err := c.GetSessionOwner(ctx, clientID)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, c.nodeID, owner)
	}
}

func TestPutWithStaleLeaseRestoresAllTrackedKeys(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	existing := "stale-lease-client-existing"
	require.NoError(t, c.AcquireSession(ctx, existing, c.nodeID))

	_, err := c.client.Revoke(ctx, clientv3.LeaseID(currentLease(c)))
	require.NoError(t, err)

	// A Put under the stale lease must recover the lease and restore the
	// previously registered key, not just the one being written.
	newcomer := "stale-lease-client-new"
	require.NoError(t, c.AcquireSession(ctx, newcomer, c.nodeID))

	require.Eventually(t, func() bool {
		return etcdKeyExists(t, c, sessionOwnerKey(existing)) &&
			etcdKeyExists(t, c, sessionOwnerKey(newcomer))
	}, recoveryWait, pollInterval, "existing key was not restored by reactive lease recovery")
}

func TestWatchDeleteReacquiresTrackedOwnerKey(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientID := "watch-reacquire-client"
	key := sessionOwnerKey(clientID)
	require.NoError(t, c.AcquireSession(ctx, clientID, c.nodeID))

	// Delete behind the broker's back (simulates lease-expiry deletion).
	_, err := c.client.Delete(ctx, key)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return etcdKeyExists(t, c, key)
	}, recoveryWait, pollInterval, "tracked owner key was not re-acquired after external delete")

	owner, ok, err := c.GetSessionOwner(ctx, clientID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, c.nodeID, owner)
}

func TestWatchDeleteEvictsUntrackedOwnerKey(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// A key owned by another node: present in etcd and cache, not tracked.
	clientID := "watch-evict-client"
	key := sessionOwnerKey(clientID)
	_, err := c.client.Put(ctx, key, testOtherNode)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		c.ownerCacheMu.RLock()
		defer c.ownerCacheMu.RUnlock()
		return c.ownerCache[clientID] == testOtherNode
	}, recoveryWait, pollInterval)

	_, err = c.client.Delete(ctx, key)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		c.ownerCacheMu.RLock()
		defer c.ownerCacheMu.RUnlock()
		_, ok := c.ownerCache[clientID]
		return !ok
	}, recoveryWait, pollInterval, "untracked owner key was not evicted from cache")

	assert.False(t, etcdKeyExists(t, c, key), "untracked key must not be resurrected")
}

func TestReleaseSessionUntracksKey(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientID := "release-untrack-client"
	key := sessionOwnerKey(clientID)
	require.NoError(t, c.AcquireSession(ctx, clientID, c.nodeID))
	require.NoError(t, c.ReleaseSession(ctx, clientID))

	_, tracked := c.getLeasedKey(key)
	assert.False(t, tracked, "released key must be untracked")

	// Neither self-heal nor the watcher may resurrect a released key.
	c.selfHealLeasedKeys()
	assert.False(t, etcdKeyExists(t, c, key))
}

func TestSelfHealRestoresMissingQueueConsumerKey(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info := &QueueConsumerInfo{
		QueueName:    testQueueName,
		GroupID:      testGroupID,
		ConsumerID:   "consumer-heal",
		ClientID:     "self-heal-client",
		Pattern:      "#",
		Mode:         testConsumerMode,
		ProxyNodeID:  c.nodeID,
		RegisteredAt: time.Now(),
	}
	require.NoError(t, c.RegisterQueueConsumer(ctx, info))
	key := fmt.Sprintf("%s%s/%s/%s", queueConsumersPrefix, info.QueueName, info.GroupID, info.ConsumerID)

	// The queue-consumer watcher does not restore keys, so this isolates
	// the periodic self-heal path.
	_, err := c.client.Delete(ctx, key)
	require.NoError(t, err)

	c.selfHealLeasedKeys()

	assert.True(t, etcdKeyExists(t, c, key), "self-heal must restore tracked key missing from etcd")
}
