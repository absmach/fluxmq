// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/mqtt/packets"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shareSend is one directed publish this node made to a share group member on
// another node.
type shareSend struct {
	nodeID   string
	clientID string
	qos      byte
}

// shareStubCluster stands in for a cluster that holds share group members on
// other nodes, without the etcd and gRPC a real one needs.
type shareStubCluster struct {
	cluster.Cluster

	members []cluster.ShareMember

	mu      sync.Mutex
	sent    []shareSend
	sendErr error
}

func (c *shareStubCluster) NodeID() string { return testNodeID }

func (c *shareStubCluster) GetSessionOwner(context.Context, string) (string, bool, error) {
	return testNodeID, true, nil
}

func (c *shareStubCluster) AcquireSession(context.Context, string, string) error { return nil }

func (c *shareStubCluster) ReleaseSession(context.Context, string) error { return nil }

func (c *shareStubCluster) GetSubscriptionsForClient(context.Context, string) ([]*storage.Subscription, error) {
	return nil, nil
}

func (c *shareStubCluster) AddSubscription(context.Context, string, string, byte, storage.SubscribeOptions) error {
	return nil
}

func (c *shareStubCluster) RemoveSubscription(context.Context, string, string) error { return nil }

func (c *shareStubCluster) RemoveAllSubscriptions(context.Context, string) error { return nil }

func (c *shareStubCluster) RoutePublish(context.Context, *message.Envelope) error { return nil }

func (c *shareStubCluster) ShareGroupMembers(_ context.Context, _ string, dst []cluster.ShareMember) ([]cluster.ShareMember, error) {
	return append(dst, c.members...), nil
}

func (c *shareStubCluster) RoutePublishToClient(_ context.Context, nodeID, clientID string, msg *message.Envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sendErr != nil {
		return c.sendErr
	}
	c.sent = append(c.sent, shareSend{nodeID: nodeID, clientID: clientID, qos: msg.BrokerMeta.Delivery.QoS})

	return nil
}

func (c *shareStubCluster) sends() []shareSend {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]shareSend(nil), c.sent...)
}

// newClusteredShareBroker builds a broker joined to cl, with one connected
// local member per client ID subscribed to filter.
func newClusteredShareBroker(t *testing.T, cl cluster.Cluster, filter string, qos byte, clientIDs ...string) (*Broker, map[string]*shareGroupMember) {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(memory.New(), cl, WithLogger(logger))
	t.Cleanup(func() { b.Close() })

	members := make(map[string]*shareGroupMember, len(clientIDs))
	for _, clientID := range clientIDs {
		s, _, err := b.CreateSession(clientID, 5, session.Options{CleanStart: true})
		require.NoError(t, err)

		conn := &mockConnection{}
		_, err = s.Connect(conn)
		require.NoError(t, err)

		require.NoError(t, b.subscribe(s, filter, qos, storage.SubscribeOptions{}))
		members[clientID] = &shareGroupMember{session: s, conn: conn}
	}

	return b, members
}

func remoteMember(clientID, nodeID string, qos byte) cluster.ShareMember {
	return cluster.ShareMember{
		ClientID:  clientID,
		NodeID:    nodeID,
		ShareName: testGroupWorkers,
		Filter:    testTasksFilter,
		QoS:       qos,
	}
}

func publishTasks(t *testing.T, b *Broker, qos byte, scope distributeScope, times int) {
	t.Helper()

	for range times {
		msg := message.New("tasks/job1", []byte("work"))
		msg.BrokerMeta.Delivery.QoS = qos
		_, err := b.distributeLocal(context.Background(), msg, scope)
		require.NoError(t, err)
		message.Release(msg)
	}
}

// TestShareGroup_RemoteOnlyGroupReceives guards the case the local router
// cannot see: every member of the group is connected to another node, so
// nothing here matches the topic, and before the group spanned the cluster the
// message went nowhere.
func TestShareGroup_RemoteOnlyGroupReceives(t *testing.T) {
	cl := &shareStubCluster{members: []cluster.ShareMember{remoteMember("worker-b", "node-b", 1)}}
	b, _ := newClusteredShareBroker(t, cl, testSharedTasksFilter, 1)

	publishTasks(t, b, 1, ingressScope, 1)

	sends := cl.sends()
	require.Len(t, sends, 1)
	assert.Equal(t, shareSend{nodeID: "node-b", clientID: "worker-b", qos: 1}, sends[0])
}

// TestShareGroup_LocalAndRemoteShareOneRotation is the property that makes the
// group a group: members here and members elsewhere take turns in a single
// rotation, so each message reaches exactly one of them.
func TestShareGroup_LocalAndRemoteShareOneRotation(t *testing.T) {
	cl := &shareStubCluster{members: []cluster.ShareMember{remoteMember("worker-b", "node-b", 1)}}
	b, members := newClusteredShareBroker(t, cl, testSharedTasksFilter, 0, testClient1)

	publishTasks(t, b, 0, ingressScope, 4)

	local := len(members[testClient1].conn.packets)
	remote := len(cl.sends())
	assert.Equal(t, 2, local, "the local member takes half the rotation")
	assert.Equal(t, 2, remote, "the remote member takes the other half")
	assert.Equal(t, 4, local+remote, "every message reaches exactly one member")
}

// TestShareGroup_RemoteFailureFallsBackToLocal checks that an unreachable node
// costs the group nothing: the member's turn passes to one that can take it.
func TestShareGroup_RemoteFailureFallsBackToLocal(t *testing.T) {
	cl := &shareStubCluster{
		members: []cluster.ShareMember{remoteMember("worker-b", "node-b", 0)},
		sendErr: errors.New("node unreachable"),
	}
	b, members := newClusteredShareBroker(t, cl, testSharedTasksFilter, 0, testClient1)

	// Two publishes: the first is the local member's turn, the second the
	// remote member's, which fails and falls back.
	publishTasks(t, b, 0, ingressScope, 2)

	assert.Len(t, members[testClient1].conn.packets, 2, "both messages land locally")
}

// TestShareGroup_ForwardedPublishDoesNotPick guards against the duplicate a
// share group would otherwise take: a message forwarded here for an ordinary
// subscription must not also start a second selection.
func TestShareGroup_ForwardedPublishDoesNotPick(t *testing.T) {
	cl := &shareStubCluster{members: []cluster.ShareMember{remoteMember("worker-b", "node-b", 1)}}
	b, members := newClusteredShareBroker(t, cl, testSharedTasksFilter, 1, testClient1)

	publishTasks(t, b, 1, forwardedScope, 3)

	assert.Empty(t, members[testClient1].conn.packets, "the ingress node already chose the member")
	assert.Empty(t, cl.sends(), "and a forwarded message never forwards again")
}

// TestShareGroup_RemoteDeliveryCapsQoS checks the subscription's QoS is honoured
// on the far side, where the receiving node delivers what it is told rather
// than matching the subscription itself.
func TestShareGroup_RemoteDeliveryCapsQoS(t *testing.T) {
	cl := &shareStubCluster{members: []cluster.ShareMember{remoteMember("worker-b", "node-b", 0)}}
	b, _ := newClusteredShareBroker(t, cl, testSharedTasksFilter, 1)

	publishTasks(t, b, 1, ingressScope, 1)

	sends := cl.sends()
	require.Len(t, sends, 1)
	assert.Equal(t, byte(0), sends[0].qos, "a QoS 0 subscription does not receive a QoS 1 delivery")
}

// TestShareGroup_MovedMemberFallsBackWithoutWarning checks the ordinary churn
// case: a member whose session reconnected elsewhere leaves a stale owner entry
// behind, and the group must move on to a member that can take the message.
func TestShareGroup_MovedMemberFallsBackWithoutWarning(t *testing.T) {
	cl := &shareStubCluster{
		members: []cluster.ShareMember{remoteMember("worker-b", "node-b", 0)},
		sendErr: fmt.Errorf("%w: session not found: worker-b", broker.ErrClientNotConnected),
	}
	b, members := newClusteredShareBroker(t, cl, testSharedTasksFilter, 0, testClient1)

	// The first publish is the local member's turn, the second the remote
	// member's, which has moved.
	publishTasks(t, b, 0, ingressScope, 2)

	assert.Len(t, members[testClient1].conn.packets, 2, "both messages land on the member that is there")
}

func groupMember(clientID, nodeID, shareName, filter string, qos byte) cluster.ShareMember {
	return cluster.ShareMember{
		ClientID:  clientID,
		NodeID:    nodeID,
		ShareName: shareName,
		Filter:    filter,
		QoS:       qos,
	}
}

// subscribeSharedQoS connects one client to b and subscribes it at qos.
func subscribeSharedQoS(t *testing.T, b *Broker, clientID, filter string, qos byte) *mockConnection {
	t.Helper()

	s, _, err := b.CreateSession(clientID, 5, session.Options{CleanStart: true})
	require.NoError(t, err)

	conn := &mockConnection{}
	_, err = s.Connect(conn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(s, filter, qos, storage.SubscribeOptions{}))

	return conn
}

// subscribeShared connects one client to b and subscribes it to filter.
func subscribeShared(t *testing.T, b *Broker, clientID, filter string) *mockConnection {
	t.Helper()

	s, _, err := b.CreateSession(clientID, 5, session.Options{CleanStart: true})
	require.NoError(t, err)

	conn := &mockConnection{}
	_, err = s.Connect(conn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(s, filter, 0, storage.SubscribeOptions{}))

	return conn
}

// TestShareGroup_DistinctGroupsEachReceiveACopy checks that two groups matching
// one topic are two groups. A message belongs to each of them once: sharing is
// within a group, never between groups.
func TestShareGroup_DistinctGroupsEachReceiveACopy(t *testing.T) {
	cases := []struct {
		name    string
		filterA string
		filterB string
		shareA  string
		filtA   string
		shareB  string
		filtB   string
	}{
		{
			// Different group names over the same filter.
			name:    "different-names",
			filterA: testSharedTasksFilter,
			filterB: "$share/auditors/tasks/#",
			shareA:  testGroupWorkers, filtA: testTasksFilter,
			shareB: "auditors", filtB: testTasksFilter,
		},
		{
			// One name, two filters. The filter is half the group's identity,
			// so these are separate groups that both match the topic.
			name:    "same-name-different-filters",
			filterA: testSharedTasksFilter,
			filterB: "$share/workers/tasks/job1",
			shareA:  testGroupWorkers, filtA: testTasksFilter,
			shareB: testGroupWorkers, filtB: "tasks/job1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cl := &shareStubCluster{members: []cluster.ShareMember{
				groupMember("remote-a", "node-b", tc.shareA, tc.filtA, 0),
				groupMember("remote-b", "node-c", tc.shareB, tc.filtB, 0),
			}}

			logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
			b := NewBroker(memory.New(), cl, WithLogger(logger))
			t.Cleanup(func() { b.Close() })

			localA := subscribeShared(t, b, "local-a", tc.filterA)
			localB := subscribeShared(t, b, "local-b", tc.filterB)

			publishTasks(t, b, 0, ingressScope, 1)

			// Each group has one local and one remote member, and the rotation
			// starts at the local one, so both groups deliver locally here.
			delivered := len(localA.packets) + len(localB.packets) + len(cl.sends())
			assert.Equal(t, 2, delivered, "each group takes one copy of the message")
			assert.Len(t, localA.packets, 1)
			assert.Len(t, localB.packets, 1)
			assert.Empty(t, cl.sends(), "neither group's turn fell to its remote member")
		})
	}
}

// TestShareGroup_DistinctGroupsRotateIndependently checks the cursors are per
// group: one group's traffic must not advance another's turn.
func TestShareGroup_DistinctGroupsRotateIndependently(t *testing.T) {
	cl := &shareStubCluster{members: []cluster.ShareMember{
		groupMember("remote-a", "node-b", "workers", testTasksFilter, 0),
	}}

	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(memory.New(), cl, WithLogger(logger))
	t.Cleanup(func() { b.Close() })

	// "workers" has a local and a remote member; "auditors" only a local one.
	workers := subscribeShared(t, b, "local-worker", testSharedTasksFilter)
	auditors := subscribeShared(t, b, "local-auditor", "$share/auditors/tasks/#")

	publishTasks(t, b, 0, ingressScope, 4)

	assert.Len(t, auditors.packets, 4, "a group of one takes every message regardless of the group beside it")
	assert.Len(t, workers.packets, 2, "the two-member group alternates on its own cursor")
	assert.Len(t, cl.sends(), 2)
}

// deliveredQoS reports the QoS of each PUBLISH a mock connection received,
// read off the fixed header so it holds for either protocol version.
func deliveredQoS(conn *mockConnection) []byte {
	out := make([]byte, 0, len(conn.packets))
	for _, p := range conn.packets {
		if p.Type() != packets.PublishType {
			continue
		}
		encoded := p.Encode()
		if len(encoded) == 0 {
			continue
		}
		out = append(out, (encoded[0]>>1)&0x03)
	}

	return out
}

// TestShareGroup_MemberKeepsItsOwnQoS guards the rule a share group has no say
// in: each member is delivered to at the QoS it subscribed at.
//
// The group's members used to share whatever QoS its first subscriber asked
// for, because the router entry standing in for the group was registered once,
// when the group was created. A member that joined at a lower QoS was then
// handed deliveries above what it had agreed to acknowledge.
func TestShareGroup_MemberKeepsItsOwnQoS(t *testing.T) {
	cases := []struct {
		name       string
		founderQoS byte
		joinerQoS  byte
	}{
		// The joiner asks for more than the group was founded at.
		{name: "founded-at-qos0/joins-at-qos1", founderQoS: 0, joinerQoS: 1},
		// The joiner asks for less — the direction that over-delivered.
		{name: "founded-at-qos1/joins-at-qos0", founderQoS: 1, joinerQoS: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cl := &shareStubCluster{}
			logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
			b := NewBroker(memory.New(), cl, WithLogger(logger))
			t.Cleanup(func() { b.Close() })

			founder := subscribeSharedQoS(t, b, "founder", testSharedTasksFilter, tc.founderQoS)
			joiner := subscribeSharedQoS(t, b, "joiner", testSharedTasksFilter, tc.joinerQoS)

			// Two members, two publishes: one message each.
			publishTasks(t, b, 1, ingressScope, 2)

			for _, got := range deliveredQoS(founder) {
				assert.Equal(t, tc.founderQoS, got, "the founder is delivered to at its own QoS")
			}
			for _, got := range deliveredQoS(joiner) {
				assert.Equal(t, tc.joinerQoS, got, "a member that joined later keeps its own QoS")
			}
			assert.Len(t, deliveredQoS(founder), 1)
			assert.Len(t, deliveredQoS(joiner), 1)
		})
	}
}

// TestShareGroup_ResubscribeUpdatesMemberQoS checks that a client changing its
// mind is honoured: re-subscribing at a new QoS replaces the stored one rather
// than being ignored as a duplicate member.
func TestShareGroup_ResubscribeUpdatesMemberQoS(t *testing.T) {
	cl := &shareStubCluster{}
	logger := slog.New(slog.NewTextHandler(os.NewFile(0, os.DevNull), nil))
	b := NewBroker(memory.New(), cl, WithLogger(logger))
	t.Cleanup(func() { b.Close() })

	conn := subscribeSharedQoS(t, b, "resubscriber", testSharedTasksFilter, 1)

	group := b.sharedSubs.GetGroup(testGroupWorkers, testTasksFilter)
	require.NotNil(t, group)
	require.Len(t, group.Subscribers, 1)
	require.Equal(t, byte(1), group.Subscribers[0].QoS)

	// Same client, same filter, lower QoS.
	s := b.Get("resubscriber")
	require.NotNil(t, s)
	require.NoError(t, b.subscribe(s, testSharedTasksFilter, 0, storage.SubscribeOptions{}))

	group = b.sharedSubs.GetGroup(testGroupWorkers, testTasksFilter)
	require.Len(t, group.Subscribers, 1, "re-subscribing does not add a second member")
	assert.Equal(t, byte(0), group.Subscribers[0].QoS, "the stored QoS follows the latest subscribe")

	publishTasks(t, b, 1, ingressScope, 1)
	assert.Equal(t, []byte{0}, deliveredQoS(conn), "delivery follows the new QoS, not the old one")
}
