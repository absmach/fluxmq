// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueueConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  QueueConfig
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  DefaultQueueConfig("$queue/test"),
			wantErr: false,
		},
		{
			name: "empty name",
			config: QueueConfig{
				Name:       "",
				Partitions: 1,
			},
			wantErr: true,
		},
		{
			name: "invalid name prefix",
			config: QueueConfig{
				Name:       "invalid/test",
				Partitions: 1,
			},
			wantErr: true,
		},
		{
			name: "zero partitions",
			config: QueueConfig{
				Name:       "$queue/test",
				Partitions: 0,
			},
			wantErr: true,
		},
		{
			name: "too many partitions",
			config: QueueConfig{
				Name:       "$queue/test",
				Partitions: 1001,
			},
			wantErr: true,
		},
		{
			name: "invalid ordering mode",
			config: QueueConfig{
				Name:       "$queue/test",
				Partitions: 1,
				Ordering:   "invalid",
			},
			wantErr: true,
		},
		{
			name: "strict ordering with multiple partitions",
			config: QueueConfig{
				Name:       "$queue/test",
				Partitions: 5,
				Ordering:   OrderingStrict,
			},
			wantErr: true,
		},
		{
			name: "strict ordering with single partition",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
				cfg.Partitions = 1
				cfg.Ordering = OrderingStrict
				return cfg
			}(),
			wantErr: false,
		},
		{
			name: "zero max message size",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
				cfg.MaxMessageSize = 0
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "zero max queue depth",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
				cfg.MaxQueueDepth = 0
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "zero delivery timeout",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
				cfg.DeliveryTimeout = 0
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "zero batch size",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
				cfg.BatchSize = 0
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "negative max retries",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
				cfg.RetryPolicy.MaxRetries = -1
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "negative initial backoff",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
				cfg.RetryPolicy.InitialBackoff = -1 * time.Second
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "max backoff less than initial",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
				cfg.RetryPolicy.InitialBackoff = 10 * time.Second
				cfg.RetryPolicy.MaxBackoff = 5 * time.Second
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "backoff multiplier less than 1",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
				cfg.RetryPolicy.BackoffMultiplier = 0.5
				return cfg
			}(),
			wantErr: true,
		},
		{
			name: "negative total timeout",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
				cfg.RetryPolicy.TotalTimeout = -1 * time.Hour
				return cfg
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidConfig)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDefaultQueueConfig(t *testing.T) {
	config := DefaultQueueConfig("$queue/test")

	assert.Equal(t, "$queue/test", config.Name)
	assert.Equal(t, 10, config.Partitions)
	assert.Equal(t, OrderingPartition, config.Ordering)
	assert.Equal(t, int64(1024*1024), config.MaxMessageSize)
	assert.Equal(t, int64(100000), config.MaxQueueDepth)
	assert.Equal(t, 7*24*time.Hour, config.MessageTTL)
	assert.Equal(t, 30*time.Second, config.DeliveryTimeout)
	assert.Equal(t, 100, config.BatchSize)

	// Retry policy
	assert.Equal(t, 10, config.RetryPolicy.MaxRetries)
	assert.Equal(t, 5*time.Second, config.RetryPolicy.InitialBackoff)
	assert.Equal(t, 5*time.Minute, config.RetryPolicy.MaxBackoff)
	assert.Equal(t, 2.0, config.RetryPolicy.BackoffMultiplier)
	assert.Equal(t, 3*time.Hour, config.RetryPolicy.TotalTimeout)

	// DLQ config
	assert.True(t, config.DLQConfig.Enabled)
	assert.Equal(t, "", config.DLQConfig.Topic) // Auto-generated
	assert.Equal(t, "", config.DLQConfig.AlertWebhook)

	// Validate default config
	err := config.Validate()
	assert.NoError(t, err)
}
