// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithMaxSegmentSize(t *testing.T) {
	config := DefaultStoreConfig()
	opt := WithMaxSegmentSize(128 * 1024 * 1024)
	opt(&config)

	assert.Equal(t, int64(128*1024*1024), config.MaxSegmentSize)
}

func TestWithMaxSegmentAge(t *testing.T) {
	config := DefaultStoreConfig()
	opt := WithMaxSegmentAge(2 * time.Hour)
	opt(&config)

	assert.Equal(t, 2*time.Hour, config.MaxSegmentAge)
}

func TestWithIndexInterval(t *testing.T) {
	config := DefaultStoreConfig()
	opt := WithIndexInterval(8192)
	opt(&config)

	assert.Equal(t, 8192, config.IndexInterval)
}

func TestWithCompression(t *testing.T) {
	config := DefaultStoreConfig()

	opt := WithCompression(CompressionZstd)
	opt(&config)
	assert.Equal(t, CompressionZstd, config.Compression)

	opt = WithCompression(CompressionS2)
	opt(&config)
	assert.Equal(t, CompressionS2, config.Compression)

	opt = WithCompression(CompressionNone)
	opt(&config)
	assert.Equal(t, CompressionNone, config.Compression)
}

func TestWithSyncInterval(t *testing.T) {
	config := DefaultStoreConfig()
	opt := WithSyncInterval(5 * time.Second)
	opt(&config)

	assert.Equal(t, 5*time.Second, config.SyncInterval)
}

func TestWithRetentionBytes(t *testing.T) {
	config := DefaultStoreConfig()
	opt := WithRetentionBytes(10 * 1024 * 1024 * 1024)
	opt(&config)

	assert.Equal(t, int64(10*1024*1024*1024), config.RetentionBytes)
}

func TestWithRetentionDuration(t *testing.T) {
	config := DefaultStoreConfig()
	opt := WithRetentionDuration(7 * 24 * time.Hour)
	opt(&config)

	assert.Equal(t, 7*24*time.Hour, config.RetentionDuration)
}

func TestWithAutoCreate(t *testing.T) {
	config := DefaultStoreConfig()

	opt := WithAutoCreate(true)
	opt(&config)
	assert.True(t, config.AutoCreate)

	opt = WithAutoCreate(false)
	opt(&config)
	assert.False(t, config.AutoCreate)
}

func TestStoreConfig_Apply(t *testing.T) {
	config := DefaultStoreConfig()

	config.Apply(
		WithMaxSegmentSize(256*1024*1024),
		WithCompression(CompressionZstd),
		WithSyncInterval(10*time.Second),
	)

	assert.Equal(t, int64(256*1024*1024), config.MaxSegmentSize)
	assert.Equal(t, CompressionZstd, config.Compression)
	assert.Equal(t, 10*time.Second, config.SyncInterval)
}

func TestFastCompression(t *testing.T) {
	config := DefaultStoreConfig()
	opt := FastCompression()
	opt(&config)

	assert.Equal(t, CompressionS2, config.Compression)
}

func TestHighCompression(t *testing.T) {
	config := DefaultStoreConfig()
	opt := HighCompression()
	opt(&config)

	assert.Equal(t, CompressionZstd, config.Compression)
}

func TestNoCompression(t *testing.T) {
	config := DefaultStoreConfig()
	opt := NoCompression()
	opt(&config)

	assert.Equal(t, CompressionNone, config.Compression)
}

func TestSmallSegments(t *testing.T) {
	config := DefaultStoreConfig()
	opt := SmallSegments()
	opt(&config)

	assert.Equal(t, int64(16*1024*1024), config.MaxSegmentSize)
}

func TestLargeSegments(t *testing.T) {
	config := DefaultStoreConfig()
	opt := LargeSegments()
	opt(&config)

	assert.Equal(t, int64(256*1024*1024), config.MaxSegmentSize)
}

func TestHighDurability(t *testing.T) {
	config := DefaultStoreConfig()
	opt := HighDurability()
	opt(&config)

	assert.Equal(t, time.Duration(0), config.SyncInterval)
}

func TestLowLatency(t *testing.T) {
	config := DefaultStoreConfig()
	opt := LowLatency()
	opt(&config)

	assert.Equal(t, 5*time.Second, config.SyncInterval)
}

func TestRetentionOneDay(t *testing.T) {
	config := DefaultStoreConfig()
	opt := RetentionOneDay()
	opt(&config)

	assert.Equal(t, 24*time.Hour, config.RetentionDuration)
}

func TestRetentionOneWeek(t *testing.T) {
	config := DefaultStoreConfig()
	opt := RetentionOneWeek()
	opt(&config)

	assert.Equal(t, 7*24*time.Hour, config.RetentionDuration)
}

func TestRetention1GB(t *testing.T) {
	config := DefaultStoreConfig()
	opt := Retention1GB()
	opt(&config)

	assert.Equal(t, int64(1024*1024*1024), config.RetentionBytes)
}

func TestRetention10GB(t *testing.T) {
	config := DefaultStoreConfig()
	opt := Retention10GB()
	opt(&config)

	assert.Equal(t, int64(10*1024*1024*1024), config.RetentionBytes)
}
