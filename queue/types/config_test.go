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
				Name: "",
			},
			wantErr: true,
		},
		{
			name: "invalid name prefix",
			config: QueueConfig{
				Name: "invalid/test",
			},
			wantErr: true,
		},
		{
			name: "zero partitions",
			config: QueueConfig{
				Name: "$queue/test",
			},
			wantErr: true,
		},
		{
			name: "too many partitions",
			config: QueueConfig{
				Name: "$queue/test",
			},
			wantErr: true,
		},
		{
			name: "invalid ordering mode",
			config: QueueConfig{
				Name: "$queue/test",
			},
			wantErr: true,
		},
		{
			name: "strict ordering with multiple partitions",
			config: QueueConfig{
				Name: "$queue/test",
			},
			wantErr: true,
		},
		{
			name: "strict ordering with single partition",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
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
				cfg.MaxDepth = 0
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
		{
			name: "ephemeral with zero expires after",
			config: func() QueueConfig {
				cfg := DefaultQueueConfig("$queue/test")
				cfg.Durable = false
				cfg.ExpiresAfter = 0
				return cfg
			}(),
			wantErr: true,
		},
		{
			name:    "valid ephemeral config",
			config:  DefaultEphemeralQueueConfig("$queue/test"),
			wantErr: false,
		},
		{
			name: "reserved non-durable rejected",
			config: func() QueueConfig {
				cfg := DefaultEphemeralQueueConfig("$queue/test")
				cfg.Reserved = true
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
	assert.True(t, config.Durable)
	assert.Equal(t, int64(1024*1024*10), config.MaxMessageSize, "unexpected max message size")
	assert.Equal(t, 7*24*time.Hour, config.MessageTTL, "unexpected message TTL")
	assert.Equal(t, 30*time.Second, config.DeliveryTimeout, "unexpected delivery TO")
	assert.Equal(t, 100, config.BatchSize, "unexpected batch size")

	// Retry policy
	assert.Equal(t, 10, config.RetryPolicy.MaxRetries, "unexpected max reties")
	assert.Equal(t, 5*time.Second, config.RetryPolicy.InitialBackoff, "unexpected initial backoff")
	assert.Equal(t, 5*time.Minute, config.RetryPolicy.MaxBackoff, "unexpected max backoff")
	assert.Equal(t, 2.0, config.RetryPolicy.BackoffMultiplier, "unexpected max backoff multiplier")
	assert.Equal(t, 24*time.Hour, config.RetryPolicy.TotalTimeout, "unexpected total retry timeout")

	// DLQ config
	assert.True(t, config.DLQConfig.Enabled, "DLQ expected to be enabled")
	assert.Equal(t, "", config.DLQConfig.Topic, "DLQ topic is not empty")
	assert.Equal(t, "", config.DLQConfig.AlertWebhook, "DLQ alert webhook is not empty")

	// Validate default config
	err := config.Validate()
	assert.NoError(t, err)
}

func TestDefaultEphemeralQueueConfig(t *testing.T) {
	config := DefaultEphemeralQueueConfig("test-queue", "test/#")

	assert.Equal(t, "test-queue", config.Name)
	assert.False(t, config.Durable)
	assert.Equal(t, 5*time.Minute, config.ExpiresAfter)
	assert.Equal(t, []string{"test/#"}, config.Topics)
	assert.True(t, config.LastConsumerDisconnect.IsZero())

	err := config.Validate()
	assert.NoError(t, err)
}

func TestMQTTQueueConfig_Durable(t *testing.T) {
	config := MQTTQueueConfig()
	assert.True(t, config.Durable)
	assert.True(t, config.Reserved)
}

func TestFromInput_Durable(t *testing.T) {
	input := QueueConfigInput{
		Name:   "test",
		Topics: []string{"#"},
	}
	config := FromInput(input)
	assert.True(t, config.Durable)
}

func TestFromInput_Replication(t *testing.T) {
	input := QueueConfigInput{
		Name:   "test",
		Topics: []string{"#"},
		Replication: ReplicationConfig{
			Enabled:           true,
			ReplicationFactor: 5,
			Mode:              ReplicationAsync,
			MinInSyncReplicas: 3,
			AckTimeout:        2 * time.Second,
		},
	}

	config := FromInput(input)
	assert.True(t, config.Replication.Enabled)
	assert.Equal(t, 5, config.Replication.ReplicationFactor)
	assert.Equal(t, ReplicationAsync, config.Replication.Mode)
	assert.Equal(t, 3, config.Replication.MinInSyncReplicas)
	assert.Equal(t, 2*time.Second, config.Replication.AckTimeout)
}
