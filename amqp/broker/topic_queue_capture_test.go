// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/absmach/fluxmq/message"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
)

type recordingChannelTopicQueueManager struct {
	*mockChannelQueueManager
	captures []qtypes.PublishRequest
}

func (m *recordingChannelTopicQueueManager) PublishToMatchingQueues(_ context.Context, publish qtypes.PublishRequest) error {
	publish.Payload = append([]byte(nil), publish.Payload...)
	m.captures = append(m.captures, publish)
	return nil
}

func TestPublishCapturesAMQP091PubSubTopic(t *testing.T) {
	qm := &recordingChannelTopicQueueManager{mockChannelQueueManager: &mockChannelQueueManager{}}
	b := New(nil, nil)
	b.queueManager = qm

	props := map[string]string{message.PropertyClientID: "amqp091:publisher"}
	if err := b.Publish(context.Background(), "m/domain/c/channel/tst", []byte("payload"), props); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if len(qm.captures) != 1 {
		t.Fatalf("expected one queue capture, got %d", len(qm.captures))
	}
	if got := qm.captures[0].Source.ClientID; got != "amqp091:publisher" {
		t.Fatalf("captured client ID = %q", got)
	}
}

// failingTopicQueueManager stands in for a queue whose storage is failing.
type failingTopicQueueManager struct {
	*mockChannelQueueManager
	calls int
}

func (m *failingTopicQueueManager) PublishToMatchingQueues(_ context.Context, _ qtypes.PublishRequest) error {
	m.calls++
	return errors.New("append to queue \"messages\": storage unavailable")
}

// Capture is a broker-side policy the publisher never asked for, so a queue
// failing to store a message must not stop it reaching subscribers: one queue's
// storage error would otherwise silence pub/sub across every topic its pattern
// covers.
func TestPublishSurvivesQueueCaptureFailure(t *testing.T) {
	qm := &failingTopicQueueManager{mockChannelQueueManager: &mockChannelQueueManager{}}
	b := New(nil, nil)
	b.queueManager = qm

	if err := b.router.Subscribe("mqtt-client", "m/#", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	delivered := 0
	b.SetCrossDeliver(func(_ context.Context, _ string, _ string, _ []byte, _ byte, _ map[string]string) {
		delivered++
	})

	if err := b.Publish(context.Background(), "m/domain/c/channel/tst", []byte("payload"), nil); err != nil {
		t.Fatalf("capture failure must not fail the publish, got %v", err)
	}
	if qm.calls != 1 {
		t.Fatalf("expected one capture attempt, got %d", qm.calls)
	}
	if delivered != 1 {
		t.Fatalf("expected the subscriber to be delivered to despite capture failure, got %d", delivered)
	}
}
