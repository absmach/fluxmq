// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const retainedTestTopic = "sensors/1/state"

// A single node runs with NoopCluster, not a nil cluster, and its retained
// messages live in the local store. A subscriber must find them there.
func TestRetainedMessagesWithoutClustering(t *testing.T) {
	tests := []struct {
		name    string
		cluster cluster.Cluster
	}{
		{"nil_cluster", nil},
		{"noop_cluster", cluster.NewNoopCluster("single")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBroker(memory.New(), tt.cluster)
			t.Cleanup(func() { _ = b.Close() })

			require.NoError(t, b.Publish(context.Background(), message.NewDelivery(retainedTestTopic, []byte("on"), 1, true)))

			for _, filter := range []string{retainedTestTopic, "sensors/+/state", "sensors/+/+"} {
				matched, err := b.GetRetainedMatching(filter)
				require.NoError(t, err, filter)
				require.Len(t, matched, 1, filter)
				assert.Equal(t, "on", string(matched[0].PayloadBytes()), filter)
				for _, msg := range matched {
					message.Release(msg)
				}
			}
		})
	}
}
