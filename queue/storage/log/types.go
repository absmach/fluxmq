// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"errors"
	"time"
)

// Storage errors.
var (
	ErrSegmentNotFound   = errors.New("segment not found")
	ErrOffsetOutOfRange  = errors.New("offset out of range")
	ErrSegmentClosed     = errors.New("segment is closed")
	ErrSegmentCorrupted  = errors.New("segment corrupted")
	ErrInvalidBatch      = errors.New("invalid batch")
	ErrCRCMismatch       = errors.New("CRC mismatch")
	ErrInvalidMagic      = errors.New("invalid magic number")
	ErrBatchTooLarge     = errors.New("batch exceeds maximum size")
	ErrEmptyBatch        = errors.New("batch contains no records")
	ErrConsumerNotFound  = errors.New("consumer not found")
	ErrGroupNotFound     = errors.New("consumer group not found")
	ErrPartitionNotFound = errors.New("partition not found")
	ErrQueueNotFound     = errors.New("queue not found")
	ErrAlreadyExists     = errors.New("already exists")
	ErrInvalidOffset     = errors.New("invalid offset")
	ErrPELEntryNotFound  = errors.New("PEL entry not found")
	ErrIndexCorrupted    = errors.New("index corrupted")
	ErrRecoveryFailed    = errors.New("recovery failed")
)

// Magic number for segment files (FLUX in ASCII).
const SegmentMagic uint32 = 0x464C5558

// Index file magic number.
const IndexMagic uint32 = 0x464C5849

// Time index magic number.
const TimeIndexMagic uint32 = 0x464C5854

// Current format versions.
const (
	SegmentVersion   uint8 = 1
	IndexVersion     uint8 = 1
	TimeIndexVersion uint8 = 1
	BatchVersion     uint8 = 1
	CursorVersion    uint8 = 1
	PELVersion       uint8 = 1
)

// Compression types.
type CompressionType uint8

const (
	CompressionNone CompressionType = iota // No compression
	CompressionS2                          // S2 (Snappy-compatible, fastest) - DEFAULT
	CompressionZstd                        // Zstd (best compression ratio)
)

// DefaultCompression is the default compression type (S2).
const DefaultCompression = CompressionS2

func (c CompressionType) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionS2:
		return "s2"
	case CompressionZstd:
		return "zstd"
	default:
		return "unknown"
	}
}

// BatchFlags contains batch metadata flags.
type BatchFlags uint16

const (
	BatchFlagCompressed    BatchFlags = 1 << 0
	BatchFlagHasTimestamp  BatchFlags = 1 << 1
	BatchFlagHasKeys       BatchFlags = 1 << 2
	BatchFlagHasHeaders    BatchFlags = 1 << 3
	BatchFlagTransactional BatchFlags = 1 << 4
)

func (f BatchFlags) IsCompressed() bool    { return f&BatchFlagCompressed != 0 }
func (f BatchFlags) HasTimestamp() bool    { return f&BatchFlagHasTimestamp != 0 }
func (f BatchFlags) HasKeys() bool         { return f&BatchFlagHasKeys != 0 }
func (f BatchFlags) HasHeaders() bool      { return f&BatchFlagHasHeaders != 0 }
func (f BatchFlags) IsTransactional() bool { return f&BatchFlagTransactional != 0 }

// Record represents a single message in a batch.
type Record struct {
	OffsetDelta    uint32
	TimestampDelta int64
	Key            []byte
	Value          []byte
	Headers        map[string][]byte
}

// Message represents a fully resolved message with absolute values.
type Message struct {
	Offset    uint64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
}

// IndexEntry represents a sparse index entry.
type IndexEntry struct {
	RelativeOffset uint32 // Offset relative to segment base
	FilePosition   uint32 // Position in segment file
}

// TimeIndexEntry represents a time index entry.
type TimeIndexEntry struct {
	Timestamp      int64  // Unix timestamp in milliseconds
	RelativeOffset uint32 // Offset relative to segment base
}

// PartitionCursor tracks consumer position in a partition.
type PartitionCursor struct {
	Cursor    uint64 // Next offset to read
	Committed uint64 // Last committed offset (safe for truncation)
	UpdatedAt int64  // Last update timestamp (unix millis)
}

// PELEntry represents a pending entry in the Pending Entry List.
type PELEntry struct {
	Offset        uint64
	PartitionID   uint32
	ConsumerID    string
	ClaimedAt     int64 // Unix timestamp in milliseconds
	DeliveryCount uint16
}

// PELOperation represents an operation type in the PEL log.
type PELOperation uint8

const (
	PELOpAdd    PELOperation = 1
	PELOpAck    PELOperation = 2
	PELOpClaim  PELOperation = 3
	PELOpExpire PELOperation = 4
)

// SegmentInfo contains metadata about a segment.
type SegmentInfo struct {
	BaseOffset   uint64
	NextOffset   uint64
	Size         int64
	MessageCount uint64
	CreatedAt    time.Time
	ModifiedAt   time.Time
	MinTimestamp time.Time
	MaxTimestamp time.Time
}

// Default configuration values.
const (
	DefaultMaxSegmentSize      = 64 * 1024 * 1024 // 64MB
	DefaultMaxSegmentAge       = time.Hour
	DefaultIndexIntervalBytes  = 4096        // Index every 4KB
	DefaultMaxBatchSize        = 1024 * 1024 // 1MB
	DefaultWriteBufferSize     = 64 * 1024   // 64KB
	DefaultReadBufferSize      = 32 * 1024   // 32KB
	DefaultSyncInterval        = time.Second
	DefaultRetentionBytes      = 0     // No limit
	DefaultRetentionDuration   = 0     // No limit
	DefaultPELCompactThreshold = 10000 // Compact after 10k operations
)

// File extensions.
const (
	SegmentExtension   = ".seg"
	IndexExtension     = ".idx"
	TimeIndexExtension = ".tix"
	CursorExtension    = ".cur"
	PELExtension       = ".pel"
	TempExtension      = ".tmp"
)
