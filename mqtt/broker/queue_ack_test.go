// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/message"
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
func (m *mockQueueManager) Publish(ctx context.Context, publish qtypes.PublishRequest) error {
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

	msg := message.New("$queue/orders/$ack", nil)
	msg.Broker.Queue.Offset = 42
	msg.Broker.Queue.GroupID = testGroupWorkers

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

	msg := message.New("$queue/orders/images/$nack", nil)
	msg.Broker.Queue.Offset = 1
	msg.Broker.Queue.GroupID = "workers@images/#"

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

	msg := message.New("$queue/$ack", nil)
	msg.Broker.Queue.Offset = 1
	msg.Broker.Queue.GroupID = testGroupWorkers

	route := resolver.Resolve(msg.Topic)
	require.Error(t, b.handleQueueAck(context.Background(), msg, route))
	require.Empty(t, qm.ackCalls)
}
