// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
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

type leaseLossRecorder struct {
	mu      sync.Mutex
	clients map[string]struct{}
}

func newLeaseLossRecorder() *leaseLossRecorder {
	return &leaseLossRecorder{clients: make(map[string]struct{})}
}

func (r *leaseLossRecorder) DeliverToClient(context.Context, string, *Message) error { return nil }

func (r *leaseLossRecorder) GetSessionStateAndClose(context.Context, string) (*clusterv1.SessionState, error) {
	return nil, nil
}

func (r *leaseLossRecorder) HandleSessionLeaseLost(_ context.Context, clientIDs []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, clientID := range clientIDs {
		r.clients[clientID] = struct{}{}
	}
}

func (r *leaseLossRecorder) GetRetainedMessage(context.Context, string) (*storage.Message, error) {
	return nil, nil
}

func (r *leaseLossRecorder) GetWillMessage(context.Context, string) (*storage.WillMessage, error) {
	return nil, nil
}

func (r *leaseLossRecorder) containsAll(clientIDs ...string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, clientID := range clientIDs {
		if _, ok := r.clients[clientID]; !ok {
			return false
		}
	}
	return true
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

func TestLeaseRevocationFencesSessionOwnersAndReregistersConsumers(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	recorder := newLeaseLossRecorder()
	c.SetMessageHandler(recorder)

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

	// The keepalive goroutine must detect the dead lease and grant a new one.
	// Session owners are fenced instead of resurrected; queue-consumer
	// registrations are safe to restore under the replacement lease.
	require.Eventually(t, func() bool {
		if currentLease(c) == oldLease {
			return false
		}
		for _, clientID := range clientIDs {
			if etcdKeyExists(t, c, sessionOwnerKey(clientID)) {
				return false
			}
		}
		return etcdKeyExists(t, c, consumerKey) && recorder.containsAll(clientIDs...)
	}, recoveryWait, pollInterval, "session owners were not fenced or consumer key was not restored")

	for _, clientID := range clientIDs {
		_, ok, err := c.GetSessionOwner(ctx, clientID)
		require.NoError(t, err)
		assert.False(t, ok)
	}
}

func TestAcquireWithStaleLeaseDoesNotRestorePreviousOwner(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	existing := "stale-lease-client-existing"
	require.NoError(t, c.AcquireSession(ctx, existing, c.nodeID))

	_, err := c.client.Revoke(ctx, clientv3.LeaseID(currentLease(c)))
	require.NoError(t, err)

	// A claim under the stale lease recovers the lease, but must not revive a
	// previous owner whose connection has been fenced.
	newcomer := "stale-lease-client-new"
	require.NoError(t, c.AcquireSession(ctx, newcomer, c.nodeID))

	require.Eventually(t, func() bool {
		return !etcdKeyExists(t, c, sessionOwnerKey(existing)) &&
			etcdKeyExists(t, c, sessionOwnerKey(newcomer))
	}, recoveryWait, pollInterval, "stale owner was restored by reactive lease recovery")
}

func TestWatchDeleteFencesAndDoesNotReacquireTrackedOwnerKey(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	recorder := newLeaseLossRecorder()
	c.SetMessageHandler(recorder)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientID := "watch-reacquire-client"
	key := sessionOwnerKey(clientID)
	require.NoError(t, c.AcquireSession(ctx, clientID, c.nodeID))

	// Delete behind the broker's back (simulates lease-expiry deletion).
	_, err := c.client.Delete(ctx, key)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, tracked := c.getLeasedKey(key)
		return !tracked && recorder.containsAll(clientID)
	}, recoveryWait, pollInterval, "deleted owner was not untracked and fenced")
	c.selfHealLeasedKeys()
	assert.False(t, etcdKeyExists(t, c, key), "deleted owner key must not be resurrected")

	_, ok, err := c.GetSessionOwner(ctx, clientID)
	require.NoError(t, err)
	assert.False(t, ok)
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

func TestAcquireSessionIsCreateOnlyAndIdempotent(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const clientID = "exclusive-owner-client"
	require.NoError(t, c.AcquireSession(ctx, clientID, "node-a"))
	require.NoError(t, c.AcquireSession(ctx, clientID, "node-a"), "same owner should be idempotent")

	err := c.AcquireSession(ctx, clientID, "node-b")
	require.ErrorIs(t, err, ErrSessionOwned)
	var ownedErr *SessionOwnedError
	require.True(t, errors.As(err, &ownedErr))
	assert.Equal(t, "node-a", ownedErr.Owner)

	resp, err := c.client.Get(ctx, sessionOwnerKey(clientID))
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	assert.Equal(t, "node-a", string(resp.Kvs[0].Value))
}

func TestAcquireSessionFencesMissingTrackedClaim(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)
	recorder := newLeaseLossRecorder()
	c.SetMessageHandler(recorder)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const clientID = "missing-tracked-owner"
	key := sessionOwnerKey(clientID)
	c.recordSessionOwnership(clientID, c.nodeID, key)

	err := c.AcquireSession(ctx, clientID, c.nodeID)
	require.ErrorIs(t, err, ErrSessionOwnershipLost)
	assert.False(t, etcdKeyExists(t, c, key))
	assert.True(t, recorder.containsAll(clientID))
}

func TestGetSessionOwnerBypassesStaleRoutingCache(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const clientID = "stale-owner-cache"
	_, err := c.client.Put(ctx, sessionOwnerKey(clientID), "node-b")
	require.NoError(t, err)
	c.ownerCacheMu.Lock()
	c.ownerCache[clientID] = "node-a"
	c.ownerCacheMu.Unlock()

	owner, ok, err := c.GetSessionOwner(ctx, clientID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "node-b", owner)
}

func TestTakeoverLockAllowsSingleWinner(t *testing.T) {
	c := newSingleNodeEtcdCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const (
		clientID = "takeover-lock-client"
		fromNode = "old-node"
		toNode   = "new-node"
	)
	require.NoError(t, c.AcquireSession(ctx, clientID, fromNode))

	lockKey := sessionTakeoverKey(clientID)
	require.NoError(t, c.acquireTakeoverLock(ctx, clientID, fromNode, lockKey, "winner"))
	require.ErrorIs(t, c.acquireTakeoverLock(ctx, clientID, fromNode, lockKey, "loser"), ErrTakeoverInProgress)
	require.NoError(t, c.finalizeTakeover(ctx, clientID, fromNode, toNode, lockKey, "winner"))

	owner, ok, err := c.GetSessionOwner(ctx, clientID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, toNode, owner)
	assert.False(t, etcdKeyExists(t, c, lockKey))
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
