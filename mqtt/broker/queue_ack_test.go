// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/message"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

type mockQueueManager struct {
	ackCalls    []ackCall
	nackCalls   []ackCall
	rejectCalls []rejectCall
}

type ackCall struct {
	queueName string
	offset    uint64
	groupID   string
}

type rejectCall struct {
	queueName string
	offset    uint64
	groupID   string
	reason    string
}

func (m *mockQueueManager) Start(ctx context.Context) error { return nil }
func (m *mockQueueManager) Stop() error                     { return nil }
func (m *mockQueueManager) Publish(ctx context.Context, msg *message.Envelope) error {
	return nil
}

func (m *mockQueueManager) Subscribe(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string) error {
	return nil
}

func (m *mockQueueManager) SubscribeWithCursor(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string, cursor *qtypes.CursorOption) error {
	return nil
}

func (m *mockQueueManager) Unsubscribe(ctx context.Context, queueName, pattern, clientID, groupID string) error {
	return nil
}

func (m *mockQueueManager) Ack(ctx context.Context, queueName, groupID string, offset uint64) error {
	m.ackCalls = append(m.ackCalls, ackCall{queueName: queueName, offset: offset, groupID: groupID})
	return nil
}

func (m *mockQueueManager) Nack(ctx context.Context, queueName, groupID string, offset uint64) error {
	m.nackCalls = append(m.nackCalls, ackCall{queueName: queueName, offset: offset, groupID: groupID})
	return nil
}

func (m *mockQueueManager) Reject(ctx context.Context, queueName, groupID string, offset uint64, reason string) error {
	m.rejectCalls = append(m.rejectCalls, rejectCall{queueName: queueName, offset: offset, groupID: groupID, reason: reason})
	return nil
}

func (m *mockQueueManager) UpdateHeartbeat(ctx context.Context, clientID string) error { return nil }

func (m *mockQueueManager) CreateQueue(ctx context.Context, config qtypes.QueueConfig) error {
	return nil
}
func (m *mockQueueManager) DeleteQueue(ctx context.Context, queueName string) error { return nil }
func (m *mockQueueManager) GetQueue(ctx context.Context, queueName string) (*qtypes.QueueConfig, error) {
	return nil, storage.ErrNotFound
}

func (m *mockQueueManager) ListQueues(ctx context.Context) ([]qtypes.QueueConfig, error) {
	return nil, nil
}

func TestHandleQueueAck_UsesParsedQueueName(t *testing.T) {
	resolver := broker.NewRoutingResolver()
	qm := &mockQueueManager{}
	b := &Broker{
		queueManager:  qm,
		routeResolver: resolver,
		telemetry:     brokerTelemetry{logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
	}

	msg := settlementEnvelope("$queue/orders/$ack", testGroupWorkers, 42)

	route := resolver.Resolve(msg.Topic)
	require.NoError(t, b.handleQueueAck(context.Background(), msg, route))
	require.Len(t, qm.ackCalls, 1)
	require.Equal(t, "orders", qm.ackCalls[0].queueName)
	require.Equal(t, uint64(42), qm.ackCalls[0].offset)
	require.Equal(t, testGroupWorkers, qm.ackCalls[0].groupID)
}

func TestHandleQueueAck_IgnoresRoutingKeyInAckTopic(t *testing.T) {
	resolver := broker.NewRoutingResolver()
	qm := &mockQueueManager{}
	b := &Broker{
		queueManager:  qm,
		routeResolver: resolver,
		telemetry:     brokerTelemetry{logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
	}

	msg := settlementEnvelope("$queue/orders/images/$nack", "workers@images/#", 1)

	route := resolver.Resolve(msg.Topic)
	require.NoError(t, b.handleQueueAck(context.Background(), msg, route))
	require.Len(t, qm.nackCalls, 1)
	require.Equal(t, "orders", qm.nackCalls[0].queueName)
}

func TestHandleQueueAck_InvalidQueueTopic(t *testing.T) {
	resolver := broker.NewRoutingResolver()
	qm := &mockQueueManager{}
	b := &Broker{
		queueManager:  qm,
		routeResolver: resolver,
		telemetry:     brokerTelemetry{logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
	}

	msg := settlementEnvelope("$queue/$ack", testGroupWorkers, 1)

	route := resolver.Resolve(msg.Topic)
	require.Error(t, b.handleQueueAck(context.Background(), msg, route))
	require.Empty(t, qm.ackCalls)
}

// settlementEnvelope builds the envelope a consumer's settlement arrives as:
// user properties in the inbound command namespace, and nothing in the
// broker-owned namespace, because ingress cannot put anything there.
func settlementEnvelope(topic, groupID string, offset uint64) *message.Envelope {
	msg := message.New(topic, nil)
	msg.PublisherMeta.Properties = message.NewPropertyMap(map[string]string{
		qtypes.PropCommitGroupID: groupID,
		qtypes.PropCommitOffset:  strconv.FormatUint(offset, 10),
	})
	return msg
}

// A consumer settles by sending user properties, and every protocol boundary
// strips the broker's own delivery property names from client input. Reading
// the settlement out of Broker.Queue therefore read fields that ingress never
// populates, and every explicit ack, nack and reject failed with "group-id
// required" no matter what the client sent.
func TestHandleQueueAck_ReadsTheClientSettlementProperties(t *testing.T) {
	resolver := broker.NewRoutingResolver()
	qm := &mockQueueManager{}
	b := &Broker{
		queueManager:  qm,
		routeResolver: resolver,
		telemetry:     brokerTelemetry{logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
	}

	// Exactly what survives extractUserProperties for a client that sends the
	// broker's outbound names back: nothing.
	msg := message.New("$queue/orders/$ack", nil)
	msg.PublisherMeta.Properties = message.PropertyMap{}
	route := resolver.Resolve(msg.Topic)
	require.Error(t, b.handleQueueAck(context.Background(), msg, route))
	require.Empty(t, qm.ackCalls)

	settled := settlementEnvelope("$queue/orders/$ack", testGroupWorkers, 7)
	require.NoError(t, b.handleQueueAck(context.Background(), settled, resolver.Resolve(settled.Topic)))
	require.Len(t, qm.ackCalls, 1)
	require.Equal(t, uint64(7), qm.ackCalls[0].offset)
}

// Offset 0 is the first record in a queue, not a missing value.
func TestHandleQueueAck_SettlesOffsetZero(t *testing.T) {
	resolver := broker.NewRoutingResolver()
	qm := &mockQueueManager{}
	b := &Broker{
		queueManager:  qm,
		routeResolver: resolver,
		telemetry:     brokerTelemetry{logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
	}

	msg := settlementEnvelope("$queue/orders/$ack", testGroupWorkers, 0)
	require.NoError(t, b.handleQueueAck(context.Background(), msg, resolver.Resolve(msg.Topic)))
	require.Len(t, qm.ackCalls, 1)
	require.Equal(t, uint64(0), qm.ackCalls[0].offset)
}

// A settlement that names no offset must be refused rather than settling the
// head of the queue.
func TestHandleQueueAck_RejectsMissingOrMalformedOffset(t *testing.T) {
	resolver := broker.NewRoutingResolver()

	for name, properties := range map[string]map[string]string{
		"no offset":        {qtypes.PropCommitGroupID: testGroupWorkers},
		"empty offset":     {qtypes.PropCommitGroupID: testGroupWorkers, qtypes.PropCommitOffset: ""},
		"malformed offset": {qtypes.PropCommitGroupID: testGroupWorkers, qtypes.PropCommitOffset: "orders:42"},
		"no group":         {qtypes.PropCommitOffset: "42"},
	} {
		t.Run(name, func(t *testing.T) {
			qm := &mockQueueManager{}
			b := &Broker{
				queueManager:  qm,
				routeResolver: resolver,
				telemetry:     brokerTelemetry{logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
			}

			msg := message.New("$queue/orders/$ack", nil)
			msg.PublisherMeta.Properties = message.NewPropertyMap(properties)
			require.Error(t, b.handleQueueAck(context.Background(), msg, resolver.Resolve(msg.Topic)))
			require.Empty(t, qm.ackCalls)
		})
	}
}

// The whole boundary, in one place: what a v5 consumer puts on the wire has to
// reach the settlement.
func TestQueueSettlementSurvivesIngress(t *testing.T) {
	sent := &v5.PublishProperties{
		User: []v5.User{
			{Key: qtypes.PropCommitGroupID, Value: testGroupWorkers},
			{Key: qtypes.PropCommitOffset, Value: "42"},
		},
	}

	settlement, err := qtypes.SettlementFromProperties(extractUserProperties(sent))
	require.NoError(t, err)
	require.Equal(t, testGroupWorkers, settlement.GroupID)
	require.Equal(t, uint64(42), settlement.Offset)
}
