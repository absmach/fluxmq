// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A configuration read from the manager is the caller's copy. Editing it must
// not change the stored configuration; only an accepted update may.
func TestQueueConfigReadsAreDetached(t *testing.T) {
	backends := map[string]func(t *testing.T) storage.QueueStore{
		"memory": func(*testing.T) storage.QueueStore { return memlog.New() },
		"logstorage": func(t *testing.T) storage.QueueStore {
			adapter, err := logstorage.NewAdapter(t.TempDir(), logstorage.DefaultAdapterConfig())
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, adapter.Close()) })
			return adapter
		},
	}

	const (
		queueName   = "orders"
		otherFilter = "elsewhere/#"
	)
	original := types.DefaultQueueConfig(queueName, "orders/#")

	scenarios := map[string]func(t *testing.T, ctx context.Context, m *Manager){
		"get/edits stay local": func(t *testing.T, ctx context.Context, m *Manager) {
			cfg, err := m.GetQueue(ctx, queueName)
			require.NoError(t, err)
			cfg.Type = types.QueueTypeStream
			cfg.MessageTTL = time.Hour
			cfg.Topics[0] = otherFilter
		},
		"list/edits stay local": func(t *testing.T, ctx context.Context, m *Manager) {
			cfgs, err := m.ListQueues(ctx)
			require.NoError(t, err)
			for i := range cfgs {
				cfgs[i].Durable = false
				cfgs[i].Topics[0] = otherFilter
			}
		},
		"update/rejected edit leaves store unchanged": func(t *testing.T, ctx context.Context, m *Manager) {
			cfg, err := m.GetQueue(ctx, queueName)
			require.NoError(t, err)
			cfg.MessageTTL = time.Hour
			cfg.Topics[0] = "#/never-matches"
			require.Error(t, m.UpdateQueue(ctx, *cfg))
		},
		"create/caller slice is not retained": func(t *testing.T, ctx context.Context, m *Manager) {
			require.NoError(t, m.DeleteQueue(ctx, queueName))
			cfg := original.Clone()
			require.NoError(t, m.CreateQueue(ctx, cfg))
			cfg.Topics[0] = otherFilter
		},
	}

	for backendName, newStore := range backends {
		for scenarioName, scenario := range scenarios {
			t.Run(backendName+"/"+scenarioName, func(t *testing.T) {
				ctx := context.Background()
				m := NewManager(newStore(t), newMockGroupStore(), nil, DefaultConfig(),
					slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
				require.NoError(t, m.CreateQueue(ctx, original.Clone()))

				scenario(t, ctx, m)

				stored, err := m.GetQueue(ctx, queueName)
				require.NoError(t, err)
				assert.Equal(t, original.Topics, stored.Topics)
				assert.Equal(t, original.Type, stored.Type)
				assert.Equal(t, original.MessageTTL, stored.MessageTTL)
				assert.Equal(t, original.Durable, stored.Durable)
			})
		}
	}
}
