// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"time"
)

// Option is a function that configures the store.
type Option func(*StoreConfig)

// WithMaxSegmentSize sets the maximum segment size.
func WithMaxSegmentSize(size int64) Option {
	return func(c *StoreConfig) {
		c.MaxSegmentSize = size
	}
}

// WithMaxSegmentAge sets the maximum segment age.
func WithMaxSegmentAge(age time.Duration) Option {
	return func(c *StoreConfig) {
		c.MaxSegmentAge = age
	}
}

// WithIndexInterval sets the index interval in bytes.
func WithIndexInterval(interval int) Option {
	return func(c *StoreConfig) {
		c.IndexInterval = interval
	}
}

// WithCompression sets the compression type.
func WithCompression(ct CompressionType) Option {
	return func(c *StoreConfig) {
		c.Compression = ct
	}
}

// WithSyncInterval sets the sync interval.
func WithSyncInterval(interval time.Duration) Option {
	return func(c *StoreConfig) {
		c.SyncInterval = interval
	}
}

// WithRetentionBytes sets the retention limit in bytes.
func WithRetentionBytes(bytes int64) Option {
	return func(c *StoreConfig) {
		c.RetentionBytes = bytes
	}
}

// WithRetentionDuration sets the retention duration.
func WithRetentionDuration(duration time.Duration) Option {
	return func(c *StoreConfig) {
		c.RetentionDuration = duration
	}
}

// WithAutoCreate enables or disables auto-creation of queues and partitions.
func WithAutoCreate(enabled bool) Option {
	return func(c *StoreConfig) {
		c.AutoCreate = enabled
	}
}

// Apply applies options to a store configuration.
func (c *StoreConfig) Apply(opts ...Option) {
	for _, opt := range opts {
		opt(c)
	}
}


// Compression presets

// FastCompression returns options for fast compression (S2 - default).
// S2 is Snappy-compatible but faster and uses less memory.
func FastCompression() Option {
	return WithCompression(CompressionS2)
}

// HighCompression returns options for high compression ratio (Zstd).
// Zstd provides ~30-50% better compression than S2 with slightly more CPU.
func HighCompression() Option {
	return WithCompression(CompressionZstd)
}

// NoCompression disables compression.
// Use when payloads are already compressed (images, video, etc.).
func NoCompression() Option {
	return WithCompression(CompressionNone)
}

// Size presets

// SmallSegments returns options for small segment sizes (16MB).
func SmallSegments() Option {
	return WithMaxSegmentSize(16 * 1024 * 1024)
}

// LargeSegments returns options for large segment sizes (256MB).
func LargeSegments() Option {
	return WithMaxSegmentSize(256 * 1024 * 1024)
}

// Durability presets

// HighDurability returns options for maximum durability.
func HighDurability() Option {
	return WithSyncInterval(0) // Sync on every write
}

// LowLatency returns options for low latency (less frequent syncs).
func LowLatency() Option {
	return WithSyncInterval(5 * time.Second)
}

// Retention presets

// RetentionOneDay returns options for 1-day retention.
func RetentionOneDay() Option {
	return WithRetentionDuration(24 * time.Hour)
}

// RetentionOneWeek returns options for 1-week retention.
func RetentionOneWeek() Option {
	return WithRetentionDuration(7 * 24 * time.Hour)
}

// Retention1GB returns options for 1GB retention limit.
func Retention1GB() Option {
	return WithRetentionBytes(1024 * 1024 * 1024)
}

// Retention10GB returns options for 10GB retention limit.
func Retention10GB() Option {
	return WithRetentionBytes(10 * 1024 * 1024 * 1024)
}
