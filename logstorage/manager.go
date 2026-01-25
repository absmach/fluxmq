// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// SegmentManager manages segments for a single partition.
type SegmentManager struct {
	mu sync.RWMutex

	dir           string
	config        ManagerConfig
	segments      []*Segment
	activeSegment *Segment

	headOffset uint64 // Earliest available offset (after truncation)
	tailOffset uint64 // Next offset to be written

	closed     bool
	syncTicker *time.Ticker
	closeCh    chan struct{}
}

// ManagerConfig holds segment manager configuration.
type ManagerConfig struct {
	MaxSegmentSize    int64
	MaxSegmentAge     time.Duration
	IndexInterval     int
	Compression       CompressionType
	SyncInterval      time.Duration
	RetentionBytes    int64
	RetentionDuration time.Duration
}

// DefaultManagerConfig returns default manager configuration.
func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		MaxSegmentSize:    DefaultMaxSegmentSize,
		MaxSegmentAge:     DefaultMaxSegmentAge,
		IndexInterval:     DefaultIndexIntervalBytes,
		Compression:       DefaultCompression, // S2 by default
		SyncInterval:      DefaultSyncInterval,
		RetentionBytes:    DefaultRetentionBytes,
		RetentionDuration: DefaultRetentionDuration,
	}
}

// NewSegmentManager creates a new segment manager for the given directory.
func NewSegmentManager(dir string, config ManagerConfig) (*SegmentManager, error) {
	// Ensure directory exists
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create segment directory: %w", err)
	}

	m := &SegmentManager{
		dir:      dir,
		config:   config,
		segments: make([]*Segment, 0),
		closeCh:  make(chan struct{}),
	}

	// Load existing segments
	if err := m.loadSegments(); err != nil {
		return nil, fmt.Errorf("failed to load segments: %w", err)
	}

	// Create initial segment if none exist
	if len(m.segments) == 0 {
		if err := m.createSegment(0); err != nil {
			return nil, fmt.Errorf("failed to create initial segment: %w", err)
		}
	} else {
		// Set active segment to the last one
		m.activeSegment = m.segments[len(m.segments)-1]
		m.headOffset = m.segments[0].BaseOffset()
		m.tailOffset = m.activeSegment.NextOffset()
	}

	// Start background sync
	if config.SyncInterval > 0 {
		m.syncTicker = time.NewTicker(config.SyncInterval)
		go m.syncLoop()
	}

	return m, nil
}

// loadSegments loads existing segments from the directory.
func (m *SegmentManager) loadSegments() error {
	pattern := filepath.Join(m.dir, "*"+SegmentExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	// Parse and sort by base offset
	type segmentFile struct {
		path       string
		baseOffset uint64
	}

	segmentFiles := make([]segmentFile, 0, len(files))
	for _, f := range files {
		name := filepath.Base(f)
		offset, err := ParseSegmentName(name)
		if err != nil {
			continue // Skip invalid files
		}
		segmentFiles = append(segmentFiles, segmentFile{path: f, baseOffset: offset})
	}

	sort.Slice(segmentFiles, func(i, j int) bool {
		return segmentFiles[i].baseOffset < segmentFiles[j].baseOffset
	})

	// Open segments
	for i, sf := range segmentFiles {
		// Last segment is writable, others are readonly
		readonly := i < len(segmentFiles)-1

		seg, err := OpenSegment(m.dir, sf.baseOffset, readonly)
		if err != nil {
			return fmt.Errorf("failed to open segment %d: %w", sf.baseOffset, err)
		}

		m.segments = append(m.segments, seg)
	}

	return nil
}

// createSegment creates a new segment starting at the given offset.
func (m *SegmentManager) createSegment(baseOffset uint64) error {
	config := SegmentConfig{
		MaxSize:         m.config.MaxSegmentSize,
		MaxAge:          m.config.MaxSegmentAge,
		IndexInterval:   m.config.IndexInterval,
		WriteBufferSize: DefaultWriteBufferSize,
		Compression:     m.config.Compression,
	}

	seg, err := CreateSegment(m.dir, baseOffset, config)
	if err != nil {
		return err
	}

	// Mark previous active segment as readonly
	if m.activeSegment != nil {
		m.activeSegment.SetReadonly()
	}

	m.segments = append(m.segments, seg)
	m.activeSegment = seg

	if len(m.segments) == 1 {
		m.headOffset = baseOffset
	}
	m.tailOffset = baseOffset

	return nil
}

// Append appends a batch to the log and returns the base offset.
func (m *SegmentManager) Append(batch *Batch) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, ErrSegmentClosed
	}

	// Check if we need to rotate
	if m.shouldRotate() {
		if err := m.rotate(); err != nil {
			return 0, fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	// Set batch base offset
	batch.BaseOffset = m.tailOffset
	batch.Compression = m.config.Compression

	// Append to active segment
	offset, err := m.activeSegment.Append(batch)
	if err != nil {
		return 0, err
	}

	m.tailOffset = batch.NextOffset()

	return offset, nil
}

// AppendMessage appends a single message and returns its offset.
func (m *SegmentManager) AppendMessage(value []byte, key []byte, headers map[string][]byte) (uint64, error) {
	batch := NewBatch(0)
	batch.Append(value, key, headers)
	return m.Append(batch)
}

// shouldRotate checks if the active segment should be rotated.
func (m *SegmentManager) shouldRotate() bool {
	if m.activeSegment == nil {
		return true
	}

	if m.activeSegment.IsFull(m.config.MaxSegmentSize) {
		return true
	}

	if m.config.MaxSegmentAge > 0 && m.activeSegment.IsExpired(m.config.MaxSegmentAge) {
		return true
	}

	return false
}

// rotate creates a new active segment.
func (m *SegmentManager) rotate() error {
	return m.createSegment(m.tailOffset)
}

// Read reads a message at the given offset.
func (m *SegmentManager) Read(offset uint64) (*Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrSegmentClosed
	}

	seg := m.findSegment(offset)
	if seg == nil {
		return nil, ErrOffsetOutOfRange
	}

	return seg.Read(offset)
}

// ReadBatch reads a batch starting at or containing the given offset.
func (m *SegmentManager) ReadBatch(offset uint64) (*Batch, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrSegmentClosed
	}

	seg := m.findSegment(offset)
	if seg == nil {
		return nil, ErrOffsetOutOfRange
	}

	return seg.ReadBatch(offset)
}

// ReadRange reads messages in the given range.
func (m *SegmentManager) ReadRange(startOffset, endOffset uint64, maxMessages int) ([]Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrSegmentClosed
	}

	if startOffset >= endOffset {
		return nil, nil
	}

	messages := make([]Message, 0, maxMessages)
	currentOffset := startOffset

	for currentOffset < endOffset && len(messages) < maxMessages {
		seg := m.findSegment(currentOffset)
		if seg == nil {
			break
		}

		// Read from this segment up to its end or the target end offset
		segEnd := seg.NextOffset()
		if segEnd > endOffset {
			segEnd = endOffset
		}

		remaining := maxMessages - len(messages)
		segMessages, err := seg.ReadRange(currentOffset, segEnd, remaining)
		if err != nil {
			return nil, err
		}

		messages = append(messages, segMessages...)
		currentOffset = segEnd
	}

	return messages, nil
}

// findSegment finds the segment containing the given offset.
func (m *SegmentManager) findSegment(offset uint64) *Segment {
	// Binary search for the segment
	i := sort.Search(len(m.segments), func(i int) bool {
		return m.segments[i].BaseOffset() > offset
	})

	if i == 0 {
		// Offset is before all segments
		if len(m.segments) > 0 && offset >= m.segments[0].BaseOffset() {
			return m.segments[0]
		}
		return nil
	}

	seg := m.segments[i-1]
	if offset >= seg.BaseOffset() && offset < seg.NextOffset() {
		return seg
	}

	return nil
}

// LookupByTime finds the offset for the given timestamp.
func (m *SegmentManager) LookupByTime(timestamp time.Time) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.segments) == 0 {
		return m.headOffset, nil
	}

	ts := timestamp.UnixMilli()

	// Find the segment that might contain this timestamp
	for i := len(m.segments) - 1; i >= 0; i-- {
		seg := m.segments[i]
		if seg.timeIndex != nil {
			minTs := seg.timeIndex.MinTimestamp()
			if !minTs.IsZero() && minTs.UnixMilli() <= ts {
				return seg.timeIndex.Lookup(ts)
			}
		}
	}

	return m.headOffset, nil
}

// Truncate removes all messages before the given offset.
func (m *SegmentManager) Truncate(beforeOffset uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrSegmentClosed
	}

	// Find segments to delete
	var toDelete []*Segment
	var toKeep []*Segment

	for _, seg := range m.segments {
		if seg.NextOffset() <= beforeOffset {
			toDelete = append(toDelete, seg)
		} else {
			toKeep = append(toKeep, seg)
		}
	}

	// Delete old segments
	for _, seg := range toDelete {
		if err := seg.Delete(); err != nil {
			return fmt.Errorf("failed to delete segment: %w", err)
		}
	}

	m.segments = toKeep

	if len(m.segments) > 0 {
		m.headOffset = m.segments[0].BaseOffset()
	} else {
		m.headOffset = beforeOffset
		// Create new segment
		if err := m.createSegment(beforeOffset); err != nil {
			return err
		}
	}

	return nil
}

// ApplyRetention applies retention policies and removes old segments.
func (m *SegmentManager) ApplyRetention() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	// Apply time-based retention
	if m.config.RetentionDuration > 0 {
		cutoff := time.Now().Add(-m.config.RetentionDuration)
		if err := m.applyTimeRetention(cutoff); err != nil {
			return err
		}
	}

	// Apply size-based retention
	if m.config.RetentionBytes > 0 {
		if err := m.applySizeRetention(); err != nil {
			return err
		}
	}

	return nil
}

// applyTimeRetention removes segments older than the cutoff time.
func (m *SegmentManager) applyTimeRetention(cutoff time.Time) error {
	// Keep at least one segment (the active one)
	for len(m.segments) > 1 {
		seg := m.segments[0]

		// Check if segment is old enough to delete
		maxTime := seg.timeIndex.MaxTimestamp()
		if maxTime.IsZero() || maxTime.After(cutoff) {
			break
		}

		// Delete segment
		if err := seg.Delete(); err != nil {
			return err
		}

		m.segments = m.segments[1:]
	}

	if len(m.segments) > 0 {
		m.headOffset = m.segments[0].BaseOffset()
	}

	return nil
}

// applySizeRetention removes segments to stay under the size limit.
func (m *SegmentManager) applySizeRetention() error {
	var totalSize int64
	for _, seg := range m.segments {
		totalSize += seg.Size()
	}

	// Keep at least one segment (the active one)
	for len(m.segments) > 1 && totalSize > m.config.RetentionBytes {
		seg := m.segments[0]
		totalSize -= seg.Size()

		if err := seg.Delete(); err != nil {
			return err
		}

		m.segments = m.segments[1:]
	}

	if len(m.segments) > 0 {
		m.headOffset = m.segments[0].BaseOffset()
	}

	return nil
}

// syncLoop periodically syncs the active segment.
func (m *SegmentManager) syncLoop() {
	for {
		select {
		case <-m.syncTicker.C:
			m.Sync()
		case <-m.closeCh:
			return
		}
	}
}

// Sync flushes all pending writes to disk.
func (m *SegmentManager) Sync() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed || m.activeSegment == nil {
		return nil
	}

	return m.activeSegment.Sync()
}

// Close closes all segments.
func (m *SegmentManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true

	// Stop sync ticker
	if m.syncTicker != nil {
		m.syncTicker.Stop()
		close(m.closeCh)
	}

	// Close all segments
	var lastErr error
	for _, seg := range m.segments {
		if err := seg.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// Head returns the head (earliest) offset.
func (m *SegmentManager) Head() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.headOffset
}

// Tail returns the tail (next) offset.
func (m *SegmentManager) Tail() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tailOffset
}

// Count returns the number of messages in the log.
func (m *SegmentManager) Count() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tailOffset - m.headOffset
}

// Size returns the total size of all segments.
func (m *SegmentManager) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var total int64
	for _, seg := range m.segments {
		total += seg.Size()
	}
	return total
}

// SegmentCount returns the number of segments.
func (m *SegmentManager) SegmentCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.segments)
}

// Segments returns information about all segments.
func (m *SegmentManager) Segments() []SegmentInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	infos := make([]SegmentInfo, len(m.segments))
	for i, seg := range m.segments {
		infos[i] = seg.Info()
	}
	return infos
}
