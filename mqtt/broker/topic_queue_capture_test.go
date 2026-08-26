// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/storage"
)

const (
	topicCapturePublisherID = "publisher"
	topicCaptureTopic       = "m/domain/c/channel/tst"
)

type recordingTopicQueueManager struct {
	mockQueueManager
	captures []*message.Envelope
}

// The envelope is only borrowed for the call, so the recorder takes its own
// reference — which is what the interface asks every implementation to do.
func (m *recordingTopicQueueManager) PublishToMatchingQueues(_ context.Context, msg *message.Envelope) error {
	m.captures = append(m.captures, msg.Clone())
	return nil
}

func TestPublishCapturesOrdinaryTopicInMatchingQueues(t *testing.T) {
	qm := &recordingTopicQueueManager{}
	b := NewBroker(nil, nil)
	t.Cleanup(func() { _ = b.Close() })
	if err := b.SetQueueManager(qm); err != nil {
		t.Fatalf("SetQueueManager failed: %v", err)
	}

	msg := message.New(topicCaptureTopic, []byte("payload"))
	msg.Broker.Source.ClientID = topicCapturePublisherID
	if err := b.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if len(qm.captures) != 1 {
		t.Fatalf("expected one queue capture, got %d", len(qm.captures))
	}
	if got := qm.captures[0].Topic; got != topicCaptureTopic {
		t.Fatalf("captured topic = %q", got)
	}
	if got := string(qm.captures[0].PayloadBytes()); got != "payload" {
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

	msg := message.New("$queue/events/item", []byte("payload"))
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

func (m *failingTopicQueueManager) PublishToMatchingQueues(_ context.Context, _ *message.Envelope) error {
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

	msg := message.New(topicCaptureTopic, []byte("payload"))
	msg.Broker.Source.ClientID = topicCapturePublisherID
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
