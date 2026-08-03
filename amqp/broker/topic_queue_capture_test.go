// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"testing"

	corebroker "github.com/absmach/fluxmq/broker"
	qtypes "github.com/absmach/fluxmq/queue/types"
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

	props := map[string]string{corebroker.ClientIDProperty: "amqp091:publisher"}
	if err := b.Publish("m/domain/c/channel/tst", []byte("payload"), props); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if len(qm.captures) != 1 {
		t.Fatalf("expected one queue capture, got %d", len(qm.captures))
	}
	if got := qm.captures[0].ClientID; got != "amqp091:publisher" {
		t.Fatalf("captured client ID = %q", got)
	}
}
