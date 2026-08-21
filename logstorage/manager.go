// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"fmt"
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
	syncErr    error
}

// ManagerConfig holds segment manager configuration.
type ManagerConfig struct {
	MaxSegmentSize       int64
	MaxSegmentAge        time.Duration
	IndexInterval        int
	Compression          CompressionType
	SyncInterval         time.Duration
	TimeIndexMinInterval time.Duration
	RetentionBytes       int64
	RetentionDuration    time.Duration
}

// DefaultManagerConfig returns default manager configuration.
func DefaultManagerConfig() ManagerConfig {
	return ManagerConfig{
		MaxSegmentSize:       DefaultMaxSegmentSize,
		MaxSegmentAge:        DefaultMaxSegmentAge,
		IndexInterval:        DefaultIndexIntervalBytes,
		Compression:          DefaultCompression, // S2 by default
		SyncInterval:         DefaultSyncInterval,
		TimeIndexMinInterval: DefaultTimeIndexMinInterval,
		RetentionBytes:       DefaultRetentionBytes,
		RetentionDuration:    DefaultRetentionDuration,
	}
}

// NewSegmentManager creates a new segment manager for the given directory.
func NewSegmentManager(dir string, config ManagerConfig) (*SegmentManager, error) {
	// Ensure the directory and every missing parent are durable before any
	// segment inside them can be synced on a publisher's behalf.
	if err := MkdirAllSynced(dir, 0o755); err != nil {
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

		if seg.index == nil {
			if err := seg.RebuildIndex(m.config.IndexInterval); err != nil {
				return fmt.Errorf("failed to rebuild index for segment %d: %w", sf.baseOffset, err)
			}
		}

		if seg.timeIndex == nil {
			if err := seg.RebuildTimeIndex(m.config.TimeIndexMinInterval); err != nil {
				return fmt.Errorf("failed to rebuild time index for segment %d: %w", sf.baseOffset, err)
			}
		}

		if seg.timeIndex != nil {
			seg.timeIndex.SetMinInterval(m.config.TimeIndexMinInterval)
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

	if seg.timeIndex != nil {
		seg.timeIndex.SetMinInterval(m.config.TimeIndexMinInterval)
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

// appendWithBarrier appends a batch and, when barrier is non-nil, runs the
// barrier against the exact segment that accepted the batch before releasing
// the manager lock. Keeping the lock across both operations prevents another
// append from rotating the active segment between the write and its durability
// barrier.
// appendLocked appends a batch and reports where it landed: the segment holding
// it and the tail offset after it. The caller runs any durability barrier after
// this returns, with no lock held, so appends continue while one is in flight.
func (m *SegmentManager) appendLocked(batch *Batch) (offset uint64, target *Segment, through uint64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, nil, 0, ErrSegmentClosed
	}
	// A failed background sync means the configured crash-loss window was not
	// established. Before accepting another append, retry that barrier while
	// holding the manager lock so no write can be acknowledged in between.
	if m.syncErr != nil {
		if err := m.activeSegment.Sync(); err != nil {
			m.syncErr = err
			return 0, nil, 0, fmt.Errorf("previous queue log sync failure persists: %w", err)
		}
		m.syncErr = nil
	}

	// Check if we need to rotate
	if m.shouldRotate() {
		if err := m.rotate(); err != nil {
			return 0, nil, 0, fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	// Set batch base offset
	batch.BaseOffset = m.tailOffset
	batch.Compression = m.config.Compression

	// Capture the target so a durability barrier always applies to the segment
	// containing this batch, even if rotation behavior changes in the future.
	target = m.activeSegment
	offset, err = target.Append(batch)
	if err != nil {
		return 0, nil, 0, err
	}

	m.tailOffset = batch.NextOffset()
	return offset, target, m.tailOffset, nil
}

// appendDurable appends a batch and returns once it is on disk.
//
// The barrier runs after the manager lock is released, so concurrent
// publishers share one fsync instead of queueing for their own. The segment is
// captured before the lock is released, so a rotation racing this still leaves
// the batch synced in the segment that holds it.
func (m *SegmentManager) appendDurable(batch *Batch) (uint64, error) {
	offset, target, through, err := m.appendLocked(batch)
	if err != nil {
		return 0, err
	}

	// The barrier records its own failure before waking anyone, so the append
	// that follows a broken fsync is refused by the retry above rather than
	// racing this return.
	if err := target.SyncThrough(through, m.recordSyncFailure); err != nil {
		return offset, fmt.Errorf("durability barrier for offset %d: %w", offset, err)
	}
	return offset, nil
}

// recordSyncFailure makes a failed barrier stick, so the next append retries it
// under the lock rather than accepting a write on top of an unestablished
// crash-loss window. It runs inside the barrier, before its waiters wake, which
// is what keeps the failure visible to every publisher that shared it.
//
// An append can still be accepted while a barrier is in flight — that is the
// point of sharing one — but acceptance is not acknowledgement: in fsync mode
// that append takes a barrier of its own and fails the same way, and in
// buffered mode it was never promised more than the sync interval.
// TestAppendNeverReportsSuccessOnAFailingDevice holds that line.
func (m *SegmentManager) recordSyncFailure(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncErr = err
}

// Append appends a batch to the log and returns the base offset. A zero sync
// interval means sync-on-write, so the append does not return until the exact
// segment that accepted it is durable. Positive intervals use the background
// ticker instead.
func (m *SegmentManager) Append(batch *Batch) (uint64, error) {
	if m.config.SyncInterval == 0 {
		return m.appendDurable(batch)
	}
	offset, _, _, err := m.appendLocked(batch)
	return offset, err
}

// AppendAndSync appends a batch and returns once the segment containing it is
// durable, sharing that barrier with any concurrent publisher waiting on the
// same segment.
func (m *SegmentManager) AppendAndSync(batch *Batch) (uint64, error) {
	return m.appendDurable(batch)
}

// AppendMessage appends a single message and returns its offset.
func (m *SegmentManager) AppendMessage(value []byte, key []byte, headers map[string][]byte) (uint64, error) {
	batch := NewBatch(0)
	batch.Append(value, key, headers)
	return m.Append(batch)
}

// AppendMessageAndSync appends one message and syncs the exact segment
// containing it before returning.
func (m *SegmentManager) AppendMessageAndSync(value []byte, key []byte, headers map[string][]byte) (uint64, error) {
	batch := NewBatch(0)
	batch.Append(value, key, headers)
	return m.AppendAndSync(batch)
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
	// A segment stops being the active target after rotation, and the periodic
	// sync loop only visits the active segment. Seal the old segment durably
	// before making it readonly or buffered appends could remain unsynced
	// forever when a busy queue rotates faster than its sync interval.
	if m.activeSegment != nil {
		if err := m.activeSegment.Sync(); err != nil {
			return fmt.Errorf("syncing segment %d before rotation: %w", m.activeSegment.BaseOffset(), err)
		}
	}
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

// RetentionOffsetBySize returns the offset to keep when enforcing size retention.
// It uses segment granularity (does not split segments).
func (m *SegmentManager) RetentionOffsetBySize(retentionBytes int64) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if retentionBytes <= 0 {
		return m.headOffset, nil
	}

	var totalSize int64
	for _, seg := range m.segments {
		totalSize += seg.Size()
	}

	if totalSize <= retentionBytes {
		return m.headOffset, nil
	}

	sizeToTrim := totalSize - retentionBytes
	var trimmed int64

	for _, seg := range m.segments {
		segSize := seg.Size()
		if trimmed+segSize < sizeToTrim {
			trimmed += segSize
			continue
		}
		return seg.BaseOffset(), nil
	}

	return m.headOffset, nil
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
			m.Sync() //nolint:errcheck // Sync records failures for the next append
		case <-m.closeCh:
			return
		}
	}
}

// Sync flushes all pending writes to disk.
func (m *SegmentManager) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed || m.activeSegment == nil {
		return nil
	}

	err := m.activeSegment.Sync()
	m.syncErr = err
	return err
}

// Close closes all segments.
func (m *SegmentManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	// Stop sync ticker
	if m.syncTicker != nil {
		m.syncTicker.Stop()
		close(m.closeCh)
	}

	// A graceful shutdown must not leave the active segment's last interval in
	// the page cache. Older segments were synced when they rotated.
	var lastErr error
	if m.activeSegment != nil {
		lastErr = m.activeSegment.Sync()
	}
	m.closed = true

	// Close all segments
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
