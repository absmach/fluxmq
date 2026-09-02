// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
)

// benchShareCluster reports a fixed set of remote share group members and
// discards what is sent to them, so a benchmark measures the publishing node's
// own work rather than a stub's bookkeeping.
type benchShareCluster struct {
	cluster.Cluster

	members []cluster.ShareMember
}

func (c *benchShareCluster) NodeID() string { return testNodeID }

func (c *benchShareCluster) GetSessionOwner(context.Context, string) (string, bool, error) {
	return testNodeID, true, nil
}

func (c *benchShareCluster) AcquireSession(context.Context, string, string) error { return nil }

func (c *benchShareCluster) ReleaseSession(context.Context, string) error { return nil }

func (c *benchShareCluster) GetSubscriptionsForClient(context.Context, string) ([]*storage.Subscription, error) {
	return nil, nil
}

func (c *benchShareCluster) AddSubscription(context.Context, string, string, byte, storage.SubscribeOptions) error {
	return nil
}

func (c *benchShareCluster) RemoveSubscription(context.Context, string, string) error { return nil }

func (c *benchShareCluster) RemoveAllSubscriptions(context.Context, string) error { return nil }

func (c *benchShareCluster) RoutePublish(context.Context, *message.Envelope) error { return nil }

func (c *benchShareCluster) ShareGroupMembers(_ context.Context, _ string, dst []cluster.ShareMember) ([]cluster.ShareMember, error) {
	return append(dst, c.members...), nil
}

func (c *benchShareCluster) RoutePublishToClient(context.Context, string, string, *message.Envelope) error {
	return nil
}

// BenchmarkMessagePublish_SharedSubscriptionClustered measures the publish path
// for a share group whose members are not all local. The unclustered benchmark
// never reaches the cluster member lookup or the scans over what it returns,
// which is the work a real deployment of this feature actually does.
func BenchmarkMessagePublish_SharedSubscriptionClustered(b *testing.B) {
	const localSubscribers = 5

	for _, remoteCount := range []int{0, 2, 8, 32} {
		b.Run(fmt.Sprintf("%d_remote_members", remoteCount), func(b *testing.B) {
			members := make([]cluster.ShareMember, remoteCount)
			for i := range members {
				members[i] = cluster.ShareMember{
					ClientID:  fmt.Sprintf("remote-%d", i),
					NodeID:    fmt.Sprintf("node-%d", i%3),
					ShareName: testGroupWorkers,
					Filter:    "test/#",
					QoS:       0,
				}
			}

			broker := NewBroker(memory.New(), &benchShareCluster{members: members})
			defer broker.Close()

			for i := range localSubscribers {
				sub := createBenchSession(b, broker, fmt.Sprintf("subscriber-%d", i))
				broker.subscribe(sub, "$share/workers/test/#", 0, storage.SubscribeOptions{}) //nolint:errcheck // benchmark setup
			}

			payload := make([]byte, 1024)

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				msg := message.NewDelivery("test/topic", payload, 0, false)
				broker.distributeLocal(context.Background(), msg, ingressScope) //nolint:errcheck // best-effort
				message.Release(msg)
			}
		})
	}
}
