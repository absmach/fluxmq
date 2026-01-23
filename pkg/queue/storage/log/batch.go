// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"fmt"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// Batch header size in bytes.
// Magic(4) + CRC(4) + BaseOffset(8) + BatchLen(4) + Count(2) + Flags(2) +
// Compression(1) + Version(1) + Reserved(2) + BaseTimestamp(8) + MaxTimestamp(8) = 44 bytes
const BatchHeaderSize = 44

// Batch represents a batch of records to be written together.
type Batch struct {
	BaseOffset    uint64
	BaseTimestamp int64 // Unix millis
	MaxTimestamp  int64 // Unix millis
	Flags         BatchFlags
	Compression   CompressionType
	Records       []Record

	// Cached encoded data for zero-copy reads
	rawData []byte
}

// NewBatch creates a new empty batch with the given base offset.
func NewBatch(baseOffset uint64) *Batch {
	return &Batch{
		BaseOffset:    baseOffset,
		BaseTimestamp: time.Now().UnixMilli(),
		MaxTimestamp:  0,
		Flags:         BatchFlagHasTimestamp,
		Compression:   CompressionNone,
		Records:       make([]Record, 0, 16),
	}
}

// Append adds a record to the batch.
func (b *Batch) Append(value []byte, key []byte, headers map[string][]byte) {
	now := time.Now().UnixMilli()

	record := Record{
		OffsetDelta:    uint32(len(b.Records)),
		TimestampDelta: now - b.BaseTimestamp,
		Value:          value,
	}

	if key != nil {
		record.Key = key
		b.Flags |= BatchFlagHasKeys
	}

	if len(headers) > 0 {
		record.Headers = headers
		b.Flags |= BatchFlagHasHeaders
	}

	if now > b.MaxTimestamp {
		b.MaxTimestamp = now
	}

	b.Records = append(b.Records, record)
}

// Count returns the number of records in the batch.
func (b *Batch) Count() int {
	return len(b.Records)
}

// LastOffset returns the last offset in the batch.
func (b *Batch) LastOffset() uint64 {
	if len(b.Records) == 0 {
		return b.BaseOffset
	}
	return b.BaseOffset + uint64(len(b.Records)) - 1
}

// NextOffset returns the next offset after this batch.
func (b *Batch) NextOffset() uint64 {
	return b.BaseOffset + uint64(len(b.Records))
}

// Encode serializes the batch to bytes.
func (b *Batch) Encode() ([]byte, error) {
	if len(b.Records) == 0 {
		return nil, ErrEmptyBatch
	}

	// Encode records first
	recordsData := b.encodeRecords()

	// Compress if needed
	var compressedData []byte
	var err error

	if b.Compression != CompressionNone && len(recordsData) > 256 {
		compressedData, err = compress(recordsData, b.Compression)
		if err != nil {
			return nil, fmt.Errorf("compression failed: %w", err)
		}
		// Only use compression if it actually reduces size
		if len(compressedData) < len(recordsData) {
			b.Flags |= BatchFlagCompressed
			recordsData = compressedData
		} else {
			b.Flags &^= BatchFlagCompressed
		}
	}

	// Calculate total size
	totalSize := BatchHeaderSize + len(recordsData)

	// Allocate buffer
	w := NewBufferWriter(totalSize)

	// Write header (CRC placeholder first)
	w.WriteUint32(SegmentMagic)
	crcPos := w.Len()
	w.WriteUint32(0) // CRC placeholder
	w.WriteUint64(b.BaseOffset)
	w.WriteUint32(uint32(len(recordsData)))
	w.WriteUint16(uint16(len(b.Records)))
	w.WriteUint16(uint16(b.Flags))
	w.WriteUint8(uint8(b.Compression))
	w.WriteUint8(BatchVersion)
	w.WriteUint16(0) // Reserved
	w.WriteUint64(uint64(b.BaseTimestamp))
	w.WriteUint64(uint64(b.MaxTimestamp))

	// Write records
	w.WriteRawBytes(recordsData)

	// Compute and write CRC (over everything after CRC field)
	data := w.Bytes()
	crc := Checksum(data[crcPos+4:])
	PutUint32(data[crcPos:], crc)

	return data, nil
}

// encodeRecords encodes all records in the batch.
func (b *Batch) encodeRecords() []byte {
	w := NewBufferWriter(len(b.Records) * 64) // Estimate

	for _, r := range b.Records {
		// Offset delta (varint)
		w.WriteUvarint(uint64(r.OffsetDelta))

		// Timestamp delta (varint)
		w.WriteVarint(r.TimestampDelta)

		// Key (length-prefixed, -1 if nil)
		if r.Key == nil {
			w.WriteVarint(-1)
		} else {
			w.WriteVarint(int64(len(r.Key)))
			w.WriteRawBytes(r.Key)
		}

		// Value (length-prefixed, -1 if nil)
		if r.Value == nil {
			w.WriteVarint(-1)
		} else {
			w.WriteVarint(int64(len(r.Value)))
			w.WriteRawBytes(r.Value)
		}

		// Headers count and data
		w.WriteUvarint(uint64(len(r.Headers)))
		for k, v := range r.Headers {
			w.WriteUvarint(uint64(len(k)))
			w.WriteRawBytes([]byte(k))
			w.WriteUvarint(uint64(len(v)))
			w.WriteRawBytes(v)
		}
	}

	return w.Bytes()
}

// Decode deserializes a batch from bytes.
func DecodeBatch(data []byte) (*Batch, error) {
	if len(data) < BatchHeaderSize {
		return nil, ErrInvalidBatch
	}

	r := NewBufferReader(data)

	// Read and validate magic
	magic, _ := r.ReadUint32()
	if magic != SegmentMagic {
		return nil, ErrInvalidMagic
	}

	// Read CRC
	storedCRC, _ := r.ReadUint32()

	// Validate CRC
	computedCRC := Checksum(data[8:])
	if storedCRC != computedCRC {
		return nil, ErrCRCMismatch
	}

	// Read header fields
	baseOffset, _ := r.ReadUint64()
	batchLen, _ := r.ReadUint32()
	count, _ := r.ReadUint16()
	flags, _ := r.ReadUint16()
	compression, _ := r.ReadUint8()
	version, _ := r.ReadUint8()
	_, _ = r.ReadUint16() // Reserved
	baseTimestamp, _ := r.ReadUint64()
	maxTimestamp, _ := r.ReadUint64()

	if version > BatchVersion {
		return nil, fmt.Errorf("unsupported batch version: %d", version)
	}

	// Read records data
	recordsData, err := r.ReadRawBytes(int(batchLen))
	if err != nil {
		return nil, fmt.Errorf("failed to read records: %w", err)
	}

	// Decompress if needed
	batchFlags := BatchFlags(flags)
	if batchFlags.IsCompressed() {
		recordsData, err = decompress(recordsData, CompressionType(compression))
		if err != nil {
			return nil, fmt.Errorf("decompression failed: %w", err)
		}
	}

	// Decode records
	records, err := decodeRecords(recordsData, int(count))
	if err != nil {
		return nil, fmt.Errorf("failed to decode records: %w", err)
	}

	return &Batch{
		BaseOffset:    baseOffset,
		BaseTimestamp: int64(baseTimestamp),
		MaxTimestamp:  int64(maxTimestamp),
		Flags:         batchFlags,
		Compression:   CompressionType(compression),
		Records:       records,
		rawData:       data,
	}, nil
}

// decodeRecords decodes records from the records data.
func decodeRecords(data []byte, count int) ([]Record, error) {
	records := make([]Record, 0, count)
	r := NewBufferReader(data)

	for i := 0; i < count; i++ {
		offsetDelta, err := r.ReadUvarint()
		if err != nil {
			return nil, err
		}

		timestampDelta, err := r.ReadVarint()
		if err != nil {
			return nil, err
		}

		// Key
		var key []byte
		keyLen, err := r.ReadVarint()
		if err != nil {
			return nil, err
		}
		if keyLen >= 0 {
			key, err = r.ReadRawBytes(int(keyLen))
			if err != nil {
				return nil, err
			}
		}

		// Value
		var value []byte
		valueLen, err := r.ReadVarint()
		if err != nil {
			return nil, err
		}
		if valueLen >= 0 {
			value, err = r.ReadRawBytes(int(valueLen))
			if err != nil {
				return nil, err
			}
		}

		// Headers
		headerCount, err := r.ReadUvarint()
		if err != nil {
			return nil, err
		}

		var headers map[string][]byte
		if headerCount > 0 {
			headers = make(map[string][]byte, headerCount)
			for j := uint64(0); j < headerCount; j++ {
				hKey, err := r.ReadBytes()
				if err != nil {
					return nil, err
				}
				hValue, err := r.ReadBytes()
				if err != nil {
					return nil, err
				}
				headers[string(hKey)] = hValue
			}
		}

		records = append(records, Record{
			OffsetDelta:    uint32(offsetDelta),
			TimestampDelta: timestampDelta,
			Key:            key,
			Value:          value,
			Headers:        headers,
		})
	}

	return records, nil
}

// Zstd encoder/decoder pools for reuse.
var (
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
)

func init() {
	var err error
	zstdEncoder, err = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		panic("failed to create zstd encoder: " + err.Error())
	}

	zstdDecoder, err = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
	)
	if err != nil {
		panic("failed to create zstd decoder: " + err.Error())
	}
}

// compress compresses data using the specified compression type.
func compress(data []byte, ct CompressionType) ([]byte, error) {
	switch ct {
	case CompressionS2:
		// S2 is Snappy-compatible but faster, uses less memory
		return s2.Encode(nil, data), nil

	case CompressionZstd:
		// Zstd provides best compression ratio
		return zstdEncoder.EncodeAll(data, nil), nil

	default:
		return data, nil
	}
}

// decompress decompresses data using the specified compression type.
func decompress(data []byte, ct CompressionType) ([]byte, error) {
	switch ct {
	case CompressionS2:
		// S2 decoder handles both S2 and legacy Snappy formats
		return s2.Decode(nil, data)

	case CompressionZstd:
		return zstdDecoder.DecodeAll(data, nil)

	default:
		return data, nil
	}
}

// Size returns the encoded size of the batch without actually encoding.
func (b *Batch) Size() int {
	size := BatchHeaderSize
	for _, r := range b.Records {
		size += UvarintSize(uint64(r.OffsetDelta))
		size += VarintSize(r.TimestampDelta)
		if r.Key == nil {
			size += VarintSize(-1)
		} else {
			size += UvarintSize(uint64(len(r.Key))) + len(r.Key)
		}
		if r.Value == nil {
			size += VarintSize(-1)
		} else {
			size += UvarintSize(uint64(len(r.Value))) + len(r.Value)
		}
		size += UvarintSize(uint64(len(r.Headers)))
		for k, v := range r.Headers {
			size += UvarintSize(uint64(len(k))) + len(k)
			size += UvarintSize(uint64(len(v))) + len(v)
		}
	}
	return size
}

// ToMessages converts batch records to fully resolved Messages.
func (b *Batch) ToMessages() []Message {
	messages := make([]Message, len(b.Records))
	for i, r := range b.Records {
		messages[i] = Message{
			Offset:    b.BaseOffset + uint64(r.OffsetDelta),
			Timestamp: time.UnixMilli(b.BaseTimestamp + r.TimestampDelta),
			Key:       r.Key,
			Value:     r.Value,
			Headers:   r.Headers,
		}
	}
	return messages
}

// GetMessage returns a single message by offset from the batch.
func (b *Batch) GetMessage(offset uint64) (*Message, error) {
	if offset < b.BaseOffset || offset > b.LastOffset() {
		return nil, ErrOffsetOutOfRange
	}

	idx := int(offset - b.BaseOffset)
	r := b.Records[idx]

	return &Message{
		Offset:    offset,
		Timestamp: time.UnixMilli(b.BaseTimestamp + r.TimestampDelta),
		Key:       r.Key,
		Value:     r.Value,
		Headers:   r.Headers,
	}, nil
}
