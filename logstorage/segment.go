// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Segment represents a single segment file containing batches of messages.
type Segment struct {
	mu sync.RWMutex

	path       string
	baseOffset uint64
	nextOffset uint64
	size       int64
	msgCount   uint64
	createdAt  time.Time
	modifiedAt time.Time

	file      *os.File
	index     *Index
	timeIndex *TimeIndex

	closed   bool
	readonly bool

	// Write buffer for batching small writes
	writeBuf []byte
	writePos int

	// Cached batch positions for efficient lookup
	batchPositions []batchPosition
}

type batchPosition struct {
	offset   uint64
	position int64
	size     int
}

// SegmentConfig holds segment configuration.
type SegmentConfig struct {
	MaxSize         int64
	MaxAge          time.Duration
	IndexInterval   int
	WriteBufferSize int
	Compression     CompressionType
}

// DefaultSegmentConfig returns default segment configuration.
func DefaultSegmentConfig() SegmentConfig {
	return SegmentConfig{
		MaxSize:         DefaultMaxSegmentSize,
		MaxAge:          DefaultMaxSegmentAge,
		IndexInterval:   DefaultIndexIntervalBytes,
		WriteBufferSize: DefaultWriteBufferSize,
		Compression:     CompressionNone,
	}
}

// OpenSegment opens an existing segment file.
func OpenSegment(dir string, baseOffset uint64, readonly bool) (*Segment, error) {
	path := filepath.Join(dir, FormatSegmentName(baseOffset))

	flags := os.O_RDWR
	if readonly {
		flags = os.O_RDONLY
	}

	file, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat segment file: %w", err)
	}

	seg := &Segment{
		path:       path,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		size:       info.Size(),
		createdAt:  info.ModTime(),
		modifiedAt: info.ModTime(),
		file:       file,
		readonly:   readonly,
		writeBuf:   make([]byte, DefaultWriteBufferSize),
	}

	// Load index
	indexPath := filepath.Join(dir, FormatIndexName(baseOffset))
	seg.index, err = OpenIndex(indexPath, baseOffset, readonly)
	if err != nil {
		// Index missing or corrupted, will rebuild
		seg.index = nil
	}

	// Load time index
	timeIndexPath := filepath.Join(dir, FormatTimeIndexName(baseOffset))
	seg.timeIndex, err = OpenTimeIndex(timeIndexPath, baseOffset, readonly)
	if err != nil {
		seg.timeIndex = nil
	}

	// Scan to determine next offset and build batch positions
	if err := seg.scan(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to scan segment: %w", err)
	}

	return seg, nil
}

// CreateSegment creates a new segment file.
func CreateSegment(dir string, baseOffset uint64, config SegmentConfig) (*Segment, error) {
	path := filepath.Join(dir, FormatSegmentName(baseOffset))

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment file: %w", err)
	}

	now := time.Now()
	seg := &Segment{
		path:           path,
		baseOffset:     baseOffset,
		nextOffset:     baseOffset,
		size:           0,
		createdAt:      now,
		modifiedAt:     now,
		file:           file,
		readonly:       false,
		writeBuf:       make([]byte, config.WriteBufferSize),
		batchPositions: make([]batchPosition, 0, 64),
	}

	// Create index
	indexPath := filepath.Join(dir, FormatIndexName(baseOffset))
	seg.index, err = CreateIndex(indexPath, baseOffset, config.IndexInterval)
	if err != nil {
		file.Close()
		os.Remove(path)
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	// Create time index
	timeIndexPath := filepath.Join(dir, FormatTimeIndexName(baseOffset))
	seg.timeIndex, err = CreateTimeIndex(timeIndexPath, baseOffset)
	if err != nil {
		file.Close()
		seg.index.Close()
		os.Remove(path)
		return nil, fmt.Errorf("failed to create time index: %w", err)
	}

	return seg, nil
}

// scan reads through the segment to build batch positions and determine state.
func (s *Segment) scan() error {
	s.batchPositions = make([]batchPosition, 0, 64)

	var pos int64 = 0
	header := make([]byte, BatchHeaderSize)

	for {
		n, err := s.file.ReadAt(header, pos)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if n < BatchHeaderSize {
			break
		}

		// Validate magic
		magic := GetUint32(header[0:4])
		if magic != SegmentMagic {
			// Possibly corrupted, stop here
			break
		}

		// Read batch length (bytes 16-19)
		batchLen := GetUint32(header[16:20])
		totalSize := BatchHeaderSize + int(batchLen)

		// Read base offset (bytes 8-15) and count (bytes 20-21)
		baseOffset := GetUint64(header[8:16])
		count := GetUint16(header[20:22])

		// Record batch position
		s.batchPositions = append(s.batchPositions, batchPosition{
			offset:   baseOffset,
			position: pos,
			size:     totalSize,
		})

		// Update segment state
		s.nextOffset = baseOffset + uint64(count)
		s.msgCount += uint64(count)

		pos += int64(totalSize)
	}

	s.size = pos
	return nil
}

// Append writes a batch to the segment and returns the base offset.
func (s *Segment) Append(batch *Batch) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrSegmentClosed
	}
	if s.readonly {
		return 0, fmt.Errorf("segment is readonly")
	}

	// Set base offset if not set
	if batch.BaseOffset == 0 {
		batch.BaseOffset = s.nextOffset
	}

	// Encode batch
	data, err := batch.Encode()
	if err != nil {
		return 0, fmt.Errorf("failed to encode batch: %w", err)
	}

	// Write to file
	pos := s.size
	n, err := s.file.WriteAt(data, pos)
	if err != nil {
		return 0, fmt.Errorf("failed to write batch: %w", err)
	}

	// Update state
	s.batchPositions = append(s.batchPositions, batchPosition{
		offset:   batch.BaseOffset,
		position: pos,
		size:     n,
	})
	s.size += int64(n)
	s.nextOffset = batch.NextOffset()
	s.msgCount += uint64(len(batch.Records))
	s.modifiedAt = time.Now()

	// Update indexes
	if s.index != nil {
		if s.index.EntryCount() == 0 || s.index.ShouldIndex(n) {
			s.index.Append(uint32(batch.BaseOffset-s.baseOffset), uint32(pos))
		}
	}
	if s.timeIndex != nil && batch.MaxTimestamp > 0 {
		s.timeIndex.Append(batch.MaxTimestamp, uint32(batch.BaseOffset-s.baseOffset))
	}

	return batch.BaseOffset, nil
}

// Read reads a message at the given offset.
func (s *Segment) Read(offset uint64) (*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, ErrOffsetOutOfRange
	}

	// Find the batch containing this offset
	batch, err := s.readBatchContaining(offset)
	if err != nil {
		return nil, err
	}

	return batch.GetMessage(offset)
}

// ReadBatch reads a batch starting at or containing the given offset.
func (s *Segment) ReadBatch(offset uint64) (*Batch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	return s.readBatchContaining(offset)
}

// readBatchContaining finds and reads the batch containing the given offset.
func (s *Segment) readBatchContaining(offset uint64) (*Batch, error) {
	// Binary search for the batch
	pos, size, found := s.findBatchPosition(offset)
	if !found {
		return nil, ErrOffsetOutOfRange
	}

	// Read batch data
	data := make([]byte, size)
	_, err := s.file.ReadAt(data, pos)
	if err != nil {
		return nil, fmt.Errorf("failed to read batch: %w", err)
	}

	return DecodeBatch(data)
}

// findBatchPosition finds the file position of the batch containing offset.
func (s *Segment) findBatchPosition(offset uint64) (pos int64, size int, found bool) {
	// Binary search through batch positions
	left, right := 0, len(s.batchPositions)-1

	for left <= right {
		mid := (left + right) / 2
		bp := s.batchPositions[mid]

		if offset < bp.offset {
			right = mid - 1
		} else if mid+1 < len(s.batchPositions) && offset >= s.batchPositions[mid+1].offset {
			left = mid + 1
		} else {
			return bp.position, bp.size, true
		}
	}

	return 0, 0, false
}

// ReadRange reads all messages in the given offset range.
func (s *Segment) ReadRange(startOffset, endOffset uint64, maxMessages int) ([]Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}

	messages := make([]Message, 0, maxMessages)
	currentOffset := startOffset

	for currentOffset < endOffset && len(messages) < maxMessages {
		batch, err := s.readBatchContaining(currentOffset)
		if err != nil {
			if err == ErrOffsetOutOfRange {
				break
			}
			return nil, err
		}

		for _, msg := range batch.ToMessages() {
			if msg.Offset >= startOffset && msg.Offset < endOffset {
				messages = append(messages, msg)
				if len(messages) >= maxMessages {
					break
				}
			}
		}

		currentOffset = batch.NextOffset()
	}

	return messages, nil
}

// Sync flushes any buffered data to disk.
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.readonly {
		return nil
	}

	if err := s.file.Sync(); err != nil {
		return err
	}

	if s.index != nil {
		if err := s.index.Sync(); err != nil {
			return err
		}
	}

	if s.timeIndex != nil {
		if err := s.timeIndex.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// Close closes the segment file.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	var errs []error

	if s.index != nil {
		if err := s.index.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if s.timeIndex != nil {
		if err := s.timeIndex.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if err := s.file.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing segment: %v", errs)
	}

	return nil
}

// Delete removes the segment and its indexes from disk.
func (s *Segment) Delete() error {
	if err := s.Close(); err != nil {
		return err
	}

	dir := filepath.Dir(s.path)

	// Remove segment file
	if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Remove index files
	indexPath := filepath.Join(dir, FormatIndexName(s.baseOffset))
	os.Remove(indexPath)

	timeIndexPath := filepath.Join(dir, FormatTimeIndexName(s.baseOffset))
	os.Remove(timeIndexPath)

	return nil
}

// Info returns segment information.
func (s *Segment) Info() SegmentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info := SegmentInfo{
		BaseOffset:   s.baseOffset,
		NextOffset:   s.nextOffset,
		Size:         s.size,
		MessageCount: s.msgCount,
		CreatedAt:    s.createdAt,
		ModifiedAt:   s.modifiedAt,
	}

	if s.timeIndex != nil {
		info.MinTimestamp = s.timeIndex.MinTimestamp()
		info.MaxTimestamp = s.timeIndex.MaxTimestamp()
	}

	return info
}

// BaseOffset returns the base offset of the segment.
func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

// NextOffset returns the next offset to be written.
func (s *Segment) NextOffset() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

// Size returns the current size of the segment file.
func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// Age returns the age of the segment.
func (s *Segment) Age() time.Duration {
	return time.Since(s.createdAt)
}

// IsFull checks if the segment has reached its size limit.
func (s *Segment) IsFull(maxSize int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size >= maxSize
}

// IsExpired checks if the segment has exceeded its age limit.
func (s *Segment) IsExpired(maxAge time.Duration) bool {
	if maxAge <= 0 {
		return false
	}
	return time.Since(s.createdAt) > maxAge
}

// SetReadonly marks the segment as readonly.
func (s *Segment) SetReadonly() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readonly = true
}

// RebuildIndex rebuilds the index from the segment data.
func (s *Segment) RebuildIndex(indexInterval int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.path)
	indexPath := filepath.Join(dir, FormatIndexName(s.baseOffset))

	// Remove old index
	if s.index != nil {
		s.index.Close()
		os.Remove(indexPath)
	}

	// Create new index
	var err error
	s.index, err = CreateIndex(indexPath, s.baseOffset, indexInterval)
	if err != nil {
		return err
	}

	// Scan through batches and add to index
	var bytesScanned int64 = 0
	for _, bp := range s.batchPositions {
		relOffset := uint32(bp.offset - s.baseOffset)
		if bytesScanned == 0 || bytesScanned >= int64(indexInterval) {
			s.index.Append(relOffset, uint32(bp.position))
			bytesScanned = 0
		}
		bytesScanned += int64(bp.size)
	}

	return s.index.Sync()
}

// RebuildTimeIndex rebuilds the time index from the segment data.
func (s *Segment) RebuildTimeIndex(minInterval time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.path)
	timeIndexPath := filepath.Join(dir, FormatTimeIndexName(s.baseOffset))

	// Remove old time index
	if s.timeIndex != nil {
		s.timeIndex.Close()
		os.Remove(timeIndexPath)
	}

	// Create new time index
	timeIndex, err := CreateTimeIndex(timeIndexPath, s.baseOffset)
	if err != nil {
		return err
	}
	timeIndex.SetMinInterval(minInterval)

	// Scan through batches and add to time index
	for _, bp := range s.batchPositions {
		data := make([]byte, bp.size)
		_, err := s.file.ReadAt(data, bp.position)
		if err != nil {
			timeIndex.Close()
			return err
		}

		batch, err := DecodeBatch(data)
		if err != nil {
			timeIndex.Close()
			return err
		}

		if batch.MaxTimestamp > 0 {
			if err := timeIndex.Append(batch.MaxTimestamp, uint32(bp.offset-s.baseOffset)); err != nil {
				timeIndex.Close()
				return err
			}
		}
	}

	s.timeIndex = timeIndex
	return s.timeIndex.Sync()
}

// FormatSegmentName formats a segment file name from base offset.
func FormatSegmentName(baseOffset uint64) string {
	return fmt.Sprintf("%020d%s", baseOffset, SegmentExtension)
}

// FormatIndexName formats an index file name from base offset.
func FormatIndexName(baseOffset uint64) string {
	return fmt.Sprintf("%020d%s", baseOffset, IndexExtension)
}

// FormatTimeIndexName formats a time index file name from base offset.
func FormatTimeIndexName(baseOffset uint64) string {
	return fmt.Sprintf("%020d%s", baseOffset, TimeIndexExtension)
}

// ParseSegmentName extracts the base offset from a segment file name.
func ParseSegmentName(name string) (uint64, error) {
	var offset uint64
	_, err := fmt.Sscanf(name, "%020d"+SegmentExtension, &offset)
	return offset, err
}
