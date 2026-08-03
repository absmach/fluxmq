// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"testing"

	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
)

const topicCapturePublisherID = "publisher"

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
		Topic:    "m/domain/c/channel/tst",
	}
	msg.SetPayloadFromBytes([]byte("payload"))
	if err := b.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if len(qm.captures) != 1 {
		t.Fatalf("expected one queue capture, got %d", len(qm.captures))
	}
	if got := qm.captures[0].Topic; got != "m/domain/c/channel/tst" {
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
