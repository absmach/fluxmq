package broker

import (
	"context"
	"io"
	"log/slog"
	"testing"

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
	messageID string
	groupID   string
}

type rejectCall struct {
	queueName string
	messageID string
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
func (m *mockQueueManager) Ack(ctx context.Context, queueName, messageID, groupID string) error {
	m.ackCalls = append(m.ackCalls, ackCall{queueName: queueName, messageID: messageID, groupID: groupID})
	return nil
}
func (m *mockQueueManager) Nack(ctx context.Context, queueName, messageID, groupID string) error {
	m.nackCalls = append(m.nackCalls, ackCall{queueName: queueName, messageID: messageID, groupID: groupID})
	return nil
}
func (m *mockQueueManager) Reject(ctx context.Context, queueName, messageID, groupID, reason string) error {
	m.rejectCalls = append(m.rejectCalls, rejectCall{queueName: queueName, messageID: messageID, groupID: groupID, reason: reason})
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
	qm := &mockQueueManager{}
	b := &Broker{
		queueManager: qm,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	msg := &storage.Message{
		Topic: "$queue/orders/$ack",
		Properties: map[string]string{
			"message-id": "orders:42",
			"group-id":   "workers",
		},
	}

	require.NoError(t, b.handleQueueAck(msg))
	require.Len(t, qm.ackCalls, 1)
	require.Equal(t, "orders", qm.ackCalls[0].queueName)
	require.Equal(t, "orders:42", qm.ackCalls[0].messageID)
	require.Equal(t, "workers", qm.ackCalls[0].groupID)
}

func TestHandleQueueAck_IgnoresRoutingKeyInAckTopic(t *testing.T) {
	qm := &mockQueueManager{}
	b := &Broker{
		queueManager: qm,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	msg := &storage.Message{
		Topic: "$queue/orders/images/$nack",
		Properties: map[string]string{
			"message-id": "orders:1",
			"group-id":   "workers@images/#",
		},
	}

	require.NoError(t, b.handleQueueAck(msg))
	require.Len(t, qm.nackCalls, 1)
	require.Equal(t, "orders", qm.nackCalls[0].queueName)
}

func TestHandleQueueAck_InvalidQueueTopic(t *testing.T) {
	qm := &mockQueueManager{}
	b := &Broker{
		queueManager: qm,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	msg := &storage.Message{
		Topic: "$queue/$ack",
		Properties: map[string]string{
			"message-id": "orders:1",
			"group-id":   "workers",
		},
	}

	require.Error(t, b.handleQueueAck(msg))
	require.Empty(t, qm.ackCalls)
}
