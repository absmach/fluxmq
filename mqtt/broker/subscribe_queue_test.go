// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/absmach/fluxmq/mqtt/session"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

type queueSubscribeCall struct {
	queueName string
	pattern   string
	clientID  string
	groupID   string
	proxyID   string
}

type mockSubscribeQueueManager struct {
	queueCfg           *qtypes.QueueConfig
	subscribeCalls     []queueSubscribeCall
	subscribeWithCalls []struct {
		queueSubscribeCall
		cursor *qtypes.CursorOption
	}
}

func (m *mockSubscribeQueueManager) Start(ctx context.Context) error { return nil }
func (m *mockSubscribeQueueManager) Stop() error                     { return nil }
func (m *mockSubscribeQueueManager) Publish(ctx context.Context, publish qtypes.PublishRequest) error {
	return nil
}
func (m *mockSubscribeQueueManager) Subscribe(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string) error {
	m.subscribeCalls = append(m.subscribeCalls, queueSubscribeCall{
		queueName: queueName,
		pattern:   pattern,
		clientID:  clientID,
		groupID:   groupID,
		proxyID:   proxyNodeID,
	})
	return nil
}
func (m *mockSubscribeQueueManager) SubscribeWithCursor(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string, cursor *qtypes.CursorOption) error {
	m.subscribeWithCalls = append(m.subscribeWithCalls, struct {
		queueSubscribeCall
		cursor *qtypes.CursorOption
	}{
		queueSubscribeCall: queueSubscribeCall{
			queueName: queueName,
			pattern:   pattern,
			clientID:  clientID,
			groupID:   groupID,
			proxyID:   proxyNodeID,
		},
		cursor: cursor,
	})
	return nil
}
func (m *mockSubscribeQueueManager) Unsubscribe(ctx context.Context, queueName, pattern, clientID, groupID string) error {
	return nil
}
func (m *mockSubscribeQueueManager) Ack(ctx context.Context, queueName, messageID, groupID string) error {
	return nil
}
func (m *mockSubscribeQueueManager) Nack(ctx context.Context, queueName, messageID, groupID string) error {
	return nil
}
func (m *mockSubscribeQueueManager) Reject(ctx context.Context, queueName, messageID, groupID, reason string) error {
	return nil
}
func (m *mockSubscribeQueueManager) UpdateHeartbeat(ctx context.Context, clientID string) error {
	return nil
}
func (m *mockSubscribeQueueManager) CreateQueue(ctx context.Context, config qtypes.QueueConfig) error {
	return nil
}
func (m *mockSubscribeQueueManager) DeleteQueue(ctx context.Context, queueName string) error {
	return nil
}
func (m *mockSubscribeQueueManager) GetQueue(ctx context.Context, queueName string) (*qtypes.QueueConfig, error) {
	if m.queueCfg == nil || m.queueCfg.Name != queueName {
		return nil, storage.ErrNotFound
	}
	cfg := *m.queueCfg
	return &cfg, nil
}
func (m *mockSubscribeQueueManager) ListQueues(ctx context.Context) ([]qtypes.QueueConfig, error) {
	return nil, nil
}

func TestQueueSubscribe_UsesStreamModeForStreamQueues(t *testing.T) {
	qm := &mockSubscribeQueueManager{
		queueCfg: &qtypes.QueueConfig{
			Name: "demo-events",
			Type: qtypes.QueueTypeStream,
		},
	}

	b := &Broker{
		queueManager: qm,
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	s := &session.Session{ID: "mqtt-client-1"}
	err := b.subscribe(s, "$queue/demo-events/#", 1, storage.SubscribeOptions{
		ConsumerGroup: "demo-readers",
	})
	require.NoError(t, err)

	require.Len(t, qm.subscribeCalls, 0)
	require.Len(t, qm.subscribeWithCalls, 1)

	call := qm.subscribeWithCalls[0]
	require.Equal(t, "demo-events", call.queueName)
	require.Equal(t, "#", call.pattern)
	require.Equal(t, "mqtt-client-1", call.clientID)
	require.Equal(t, "demo-readers", call.groupID)
	require.NotNil(t, call.cursor)
	require.Equal(t, qtypes.GroupModeStream, call.cursor.Mode)
}
