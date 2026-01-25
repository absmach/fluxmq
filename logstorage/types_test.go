// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressionType_String(t *testing.T) {
	tests := []struct {
		ct       CompressionType
		expected string
	}{
		{CompressionNone, "none"},
		{CompressionS2, "s2"},
		{CompressionZstd, "zstd"},
		{CompressionType(99), "unknown"},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.ct.String())
	}
}

func TestBatchFlags(t *testing.T) {
	var flags BatchFlags

	assert.False(t, flags.IsCompressed())
	assert.False(t, flags.HasTimestamp())
	assert.False(t, flags.HasKeys())
	assert.False(t, flags.HasHeaders())
	assert.False(t, flags.IsTransactional())

	flags = BatchFlagCompressed | BatchFlagHasTimestamp | BatchFlagHasKeys | BatchFlagHasHeaders | BatchFlagTransactional

	assert.True(t, flags.IsCompressed())
	assert.True(t, flags.HasTimestamp())
	assert.True(t, flags.HasKeys())
	assert.True(t, flags.HasHeaders())
	assert.True(t, flags.IsTransactional())

	flags = BatchFlagCompressed
	assert.True(t, flags.IsCompressed())
	assert.False(t, flags.HasTimestamp())
}

func TestMagicConstants(t *testing.T) {
	assert.Equal(t, uint32(0x464C5558), SegmentMagic)
	assert.Equal(t, uint32(0x464C5849), IndexMagic)
	assert.Equal(t, uint32(0x464C5854), TimeIndexMagic)
}

func TestVersionConstants(t *testing.T) {
	assert.Equal(t, uint8(1), SegmentVersion)
	assert.Equal(t, uint8(1), IndexVersion)
	assert.Equal(t, uint8(1), TimeIndexVersion)
	assert.Equal(t, uint8(1), BatchVersion)
	assert.Equal(t, uint8(1), CursorVersion)
	assert.Equal(t, uint8(1), PELVersion)
}

func TestDefaultConstants(t *testing.T) {
	assert.Equal(t, int64(64*1024*1024), int64(DefaultMaxSegmentSize))
	assert.Equal(t, 4096, DefaultIndexIntervalBytes)
	assert.Equal(t, 1024*1024, DefaultMaxBatchSize)
	assert.Equal(t, 64*1024, DefaultWriteBufferSize)
	assert.Equal(t, 32*1024, DefaultReadBufferSize)
	assert.Equal(t, 10000, DefaultPELCompactThreshold)
}

func TestFileExtensions(t *testing.T) {
	assert.Equal(t, ".seg", SegmentExtension)
	assert.Equal(t, ".idx", IndexExtension)
	assert.Equal(t, ".tix", TimeIndexExtension)
	assert.Equal(t, ".cur", CursorExtension)
	assert.Equal(t, ".pel", PELExtension)
	assert.Equal(t, ".tmp", TempExtension)
}

func TestPELOperationConstants(t *testing.T) {
	assert.Equal(t, PELOperation(1), PELOpAdd)
	assert.Equal(t, PELOperation(2), PELOpAck)
	assert.Equal(t, PELOperation(3), PELOpClaim)
	assert.Equal(t, PELOperation(4), PELOpExpire)
}

func TestErrors(t *testing.T) {
	assert.NotNil(t, ErrSegmentNotFound)
	assert.NotNil(t, ErrOffsetOutOfRange)
	assert.NotNil(t, ErrSegmentClosed)
	assert.NotNil(t, ErrSegmentCorrupted)
	assert.NotNil(t, ErrInvalidBatch)
	assert.NotNil(t, ErrCRCMismatch)
	assert.NotNil(t, ErrInvalidMagic)
	assert.NotNil(t, ErrBatchTooLarge)
	assert.NotNil(t, ErrEmptyBatch)
	assert.NotNil(t, ErrConsumerNotFound)
	assert.NotNil(t, ErrGroupNotFound)
	assert.NotNil(t, ErrPartitionNotFound)
	assert.NotNil(t, ErrQueueNotFound)
	assert.NotNil(t, ErrAlreadyExists)
	assert.NotNil(t, ErrInvalidOffset)
	assert.NotNil(t, ErrPELEntryNotFound)
	assert.NotNil(t, ErrIndexCorrupted)
	assert.NotNil(t, ErrRecoveryFailed)
}
