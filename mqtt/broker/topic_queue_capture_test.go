// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"testing"

	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
)

const (
	topicCapturePublisherID = "publisher"
	topicCaptureTopic       = "m/domain/c/channel/tst"
)

type recordingTopicQueueManager struct {
	mockQueueManager
	captures []qtypes.PublishRequest
}

func (m *recordingTopicQueueManager) PublishToMatchingQueues(_ context.Context, publish qtypes.PublishRequest) error {
	publish.Payload = append([]byte(nil), publish.Payload...)
	m.captures = append(m.captures, publish)
	return nil
}

func TestPublishCapturesOrdinaryTopicInMatchingQueues(t *testing.T) {
	qm := &recordingTopicQueueManager{}
	b := NewBroker(nil, nil)
	t.Cleanup(func() { _ = b.Close() })
	if err := b.SetQueueManager(qm); err != nil {
		t.Fatalf("SetQueueManager failed: %v", err)
	}

	msg := &storage.Message{
		ClientID: topicCapturePublisherID,
		Topic:    topicCaptureTopic,
	}
	msg.SetPayloadFromBytes([]byte("payload"))
	if err := b.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if len(qm.captures) != 1 {
		t.Fatalf("expected one queue capture, got %d", len(qm.captures))
	}
	if got := qm.captures[0].Topic; got != topicCaptureTopic {
		t.Fatalf("captured topic = %q", got)
	}
	if got := string(qm.captures[0].Payload); got != "payload" {
		t.Fatalf("captured payload = %q", got)
	}
}

func TestPublishDoesNotRecaptureExplicitQueueTopic(t *testing.T) {
	qm := &recordingTopicQueueManager{}
	b := NewBroker(nil, nil)
	t.Cleanup(func() { _ = b.Close() })
	if err := b.SetQueueManager(qm); err != nil {
		t.Fatalf("SetQueueManager failed: %v", err)
	}

	msg := &storage.Message{Topic: "$queue/events/item"}
	msg.SetPayloadFromBytes([]byte("payload"))
	if err := b.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if len(qm.captures) != 0 {
		t.Fatalf("explicit queue publish was recaptured %d times", len(qm.captures))
	}
}

// failingTopicQueueManager stands in for a queue whose storage is failing.
type failingTopicQueueManager struct {
	mockQueueManager
	calls int
}

func (m *failingTopicQueueManager) PublishToMatchingQueues(_ context.Context, _ qtypes.PublishRequest) error {
	m.calls++
	return errors.New("append to queue \"messages\": storage unavailable")
}

// Capture is a broker-side policy the publisher never asked for, so a queue
// failing to store a message must not stop it reaching subscribers: one queue's
// storage error would otherwise silence pub/sub across every topic its pattern
// covers. The publish must also still release its payload exactly once.
func TestPublishSurvivesQueueCaptureFailure(t *testing.T) {
	qm := &failingTopicQueueManager{}
	b := NewBroker(nil, nil)
	t.Cleanup(func() { _ = b.Close() })
	if err := b.SetQueueManager(qm); err != nil {
		t.Fatalf("SetQueueManager failed: %v", err)
	}

	if err := b.router.Subscribe("amqp:consumer", "m/#", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	delivered := 0
	b.SetCrossDeliver(func(_ context.Context, _ string, _ string, _ []byte, _ byte, _ map[string]string) {
		delivered++
	})

	msg := &storage.Message{
		ClientID: topicCapturePublisherID,
		Topic:    topicCaptureTopic,
	}
	msg.SetPayloadFromBytes([]byte("payload"))
	if err := b.Publish(context.Background(), msg); err != nil {
		t.Fatalf("capture failure must not fail the publish, got %v", err)
	}

	if qm.calls != 1 {
		t.Fatalf("expected one capture attempt, got %d", qm.calls)
	}
	if delivered != 1 {
		t.Fatalf("expected the subscriber to be delivered to despite capture failure, got %d", delivered)
	}
}
