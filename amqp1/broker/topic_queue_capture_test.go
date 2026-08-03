// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"testing"

	corebroker "github.com/absmach/fluxmq/broker"
	qtypes "github.com/absmach/fluxmq/queue/types"
)

type recordingAMQP1TopicQueueManager struct {
	*mockAMQP1QueueLinkManager
	captures []qtypes.PublishRequest
}

func (m *recordingAMQP1TopicQueueManager) PublishToMatchingQueues(_ context.Context, publish qtypes.PublishRequest) error {
	publish.Payload = append([]byte(nil), publish.Payload...)
	m.captures = append(m.captures, publish)
	return nil
}

func TestPublishCapturesAMQP1PubSubTopic(t *testing.T) {
	qm := &recordingAMQP1TopicQueueManager{mockAMQP1QueueLinkManager: &mockAMQP1QueueLinkManager{}}
	b := New(nil, nil, nil)
	b.queueLinkManager = qm

	props := map[string]string{corebroker.ClientIDProperty: "amqp:publisher"}
	b.Publish(context.Background(), "m/domain/c/channel/tst", []byte("payload"), props)

	if len(qm.captures) != 1 {
		t.Fatalf("expected one queue capture, got %d", len(qm.captures))
	}
	if got := qm.captures[0].ClientID; got != "amqp:publisher" {
		t.Fatalf("captured client ID = %q", got)
	}
}
