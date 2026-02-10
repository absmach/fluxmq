// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeliveryTargetFunc(t *testing.T) {
	var captured struct {
		clientID string
		topic    string
	}

	dt := DeliveryTargetFunc(func(ctx context.Context, clientID string, msg *storage.Message) error {
		captured.clientID = clientID
		captured.topic = msg.Topic
		return nil
	})

	// Verify it satisfies the interface
	var _ Deliverer = dt

	msg := &storage.Message{Topic: "test/topic", QoS: 1}
	err := dt.Deliver(context.Background(), "client-1", msg)
	require.NoError(t, err)
	assert.Equal(t, "client-1", captured.clientID)
	assert.Equal(t, "test/topic", captured.topic)
}
