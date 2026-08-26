// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"testing"

	coremessage "github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/storage"
)

type recordingAMQP1TopicQueueManager struct {
	*mockAMQP1QueueLinkManager
	captures []*coremessage.Envelope
}

// The envelope is only borrowed for the call, so the recorder takes its own
// reference — which is what the interface asks every implementation to do.
func (m *recordingAMQP1TopicQueueManager) PublishToMatchingQueues(_ context.Context, msg *coremessage.Envelope) error {
	m.captures = append(m.captures, msg.Clone())
	return nil
}

func TestPublishCapturesAMQP1PubSubTopic(t *testing.T) {
	qm := &recordingAMQP1TopicQueueManager{mockAMQP1QueueLinkManager: &mockAMQP1QueueLinkManager{}}
	b := New(nil, nil, nil)
	b.queueLinkManager = qm

	props := map[string]string{coremessage.PropertyClientID: "amqp:publisher"}
	b.Publish(context.Background(), "m/domain/c/channel/tst", []byte("payload"), props)

	if len(qm.captures) != 1 {
		t.Fatalf("expected one queue capture, got %d", len(qm.captures))
	}
	if got := qm.captures[0].Broker.Source.ClientID; got != "amqp:publisher" {
		t.Fatalf("captured client ID = %q", got)
	}
}

// failingAMQP1TopicQueueManager stands in for a queue whose storage is failing.
type failingAMQP1TopicQueueManager struct {
	*mockAMQP1QueueLinkManager
	calls int
}

func (m *failingAMQP1TopicQueueManager) PublishToMatchingQueues(_ context.Context, _ *coremessage.Envelope) error {
	m.calls++
	return errors.New("append to queue \"messages\": storage unavailable")
}

// Capture is a broker-side policy the publisher never asked for, so a queue
// failing to store a message must not stop it reaching subscribers: one queue's
// storage error would otherwise silence pub/sub across every topic its pattern
// covers.
func TestPublishSurvivesQueueCaptureFailure(t *testing.T) {
	qm := &failingAMQP1TopicQueueManager{mockAMQP1QueueLinkManager: &mockAMQP1QueueLinkManager{}}
	b := New(nil, nil, nil)
	t.Cleanup(b.Close)
	b.queueLinkManager = qm

	if err := b.router.Subscribe("mqtt-client", "m/#", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	delivered := 0
	b.SetCrossDeliver(func(_ context.Context, _ string, _ string, _ []byte, _ byte, _ map[string]string) {
		delivered++
	})

	b.Publish(context.Background(), "m/domain/c/channel/tst", []byte("payload"), nil)

	if qm.calls != 1 {
		t.Fatalf("expected one capture attempt, got %d", qm.calls)
	}
	if delivered != 1 {
		t.Fatalf("expected the subscriber to be delivered to despite capture failure, got %d", delivered)
	}
}
