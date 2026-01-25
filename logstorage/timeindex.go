// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

// TimeIndex header size: Magic(4) + Version(1) + BaseOffset(8) + EntryCount(4) + MinTS(8) + MaxTS(8) + Reserved(3) = 36 bytes
const TimeIndexHeaderSize = 36

// TimeIndex entry size: Timestamp(8) + RelativeOffset(4) = 12 bytes
const TimeIndexEntrySize = 12

// TimeIndex provides timestamp-to-offset mapping for time-based seeks.
type TimeIndex struct {
	mu sync.RWMutex

	path       string
	baseOffset uint64

	file         *os.File
	entries      []TimeIndexEntry
	minTimestamp int64
	maxTimestamp int64
	readonly     bool
	dirty        bool

	// Only add entries when timestamp changes significantly
	lastTimestamp int64
	minInterval   int64 // Minimum milliseconds between entries
}

// CreateTimeIndex creates a new time index file.
func CreateTimeIndex(path string, baseOffset uint64) (*TimeIndex, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create time index file: %w", err)
	}

	tidx := &TimeIndex{
		path:        path,
		baseOffset:  baseOffset,
		file:        file,
		entries:     make([]TimeIndexEntry, 0, 256),
		readonly:    false,
		dirty:       true,
		minInterval: 1000, // 1 second default
	}

	if err := tidx.writeHeader(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}

	return tidx, nil
}

// OpenTimeIndex opens an existing time index file.
func OpenTimeIndex(path string, baseOffset uint64, readonly bool) (*TimeIndex, error) {
	flags := os.O_RDWR
	if readonly {
		flags = os.O_RDONLY
	}

	file, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open time index file: %w", err)
	}

	tidx := &TimeIndex{
		path:        path,
		baseOffset:  baseOffset,
		file:        file,
		readonly:    readonly,
		minInterval: 1000,
	}

	if err := tidx.readHeader(); err != nil {
		file.Close()
		return nil, err
	}

	if err := tidx.loadEntries(); err != nil {
		file.Close()
		return nil, err
	}

	return tidx, nil
}

// writeHeader writes the time index file header.
func (tidx *TimeIndex) writeHeader() error {
	header := make([]byte, TimeIndexHeaderSize)

	PutUint32(header[0:4], TimeIndexMagic)
	header[4] = TimeIndexVersion
	PutUint64(header[5:13], tidx.baseOffset)
	PutUint32(header[13:17], uint32(len(tidx.entries)))
	PutUint64(header[17:25], uint64(tidx.minTimestamp))
	PutUint64(header[25:33], uint64(tidx.maxTimestamp))

	_, err := tidx.file.WriteAt(header, 0)
	return err
}

// readHeader reads and validates the time index file header.
func (tidx *TimeIndex) readHeader() error {
	header := make([]byte, TimeIndexHeaderSize)

	n, err := tidx.file.ReadAt(header, 0)
	if err != nil {
		return fmt.Errorf("failed to read time index header: %w", err)
	}
	if n < TimeIndexHeaderSize {
		return ErrIndexCorrupted
	}

	magic := GetUint32(header[0:4])
	if magic != TimeIndexMagic {
		return ErrIndexCorrupted
	}

	version := header[4]
	if version > TimeIndexVersion {
		return fmt.Errorf("unsupported time index version: %d", version)
	}

	storedBaseOffset := GetUint64(header[5:13])
	if storedBaseOffset != tidx.baseOffset {
		return fmt.Errorf("time index base offset mismatch")
	}

	tidx.minTimestamp = int64(GetUint64(header[17:25]))
	tidx.maxTimestamp = int64(GetUint64(header[25:33]))

	return nil
}

// loadEntries loads all time index entries from the file.
func (tidx *TimeIndex) loadEntries() error {
	info, err := tidx.file.Stat()
	if err != nil {
		return err
	}

	entriesSize := info.Size() - TimeIndexHeaderSize
	if entriesSize < 0 || entriesSize%TimeIndexEntrySize != 0 {
		return ErrIndexCorrupted
	}

	entryCount := int(entriesSize / TimeIndexEntrySize)
	tidx.entries = make([]TimeIndexEntry, entryCount)

	if entryCount == 0 {
		return nil
	}

	data := make([]byte, entriesSize)
	_, err = tidx.file.ReadAt(data, TimeIndexHeaderSize)
	if err != nil {
		return fmt.Errorf("failed to read time index entries: %w", err)
	}

	for i := 0; i < entryCount; i++ {
		offset := i * TimeIndexEntrySize
		tidx.entries[i] = TimeIndexEntry{
			Timestamp:      int64(GetUint64(data[offset : offset+8])),
			RelativeOffset: GetUint32(data[offset+8 : offset+12]),
		}
	}

	if entryCount > 0 {
		tidx.lastTimestamp = tidx.entries[entryCount-1].Timestamp
	}

	return nil
}

// Append adds a new entry to the time index.
func (tidx *TimeIndex) Append(timestamp int64, relativeOffset uint32) error {
	tidx.mu.Lock()
	defer tidx.mu.Unlock()

	if tidx.readonly {
		return fmt.Errorf("time index is readonly")
	}

	// Only add if timestamp is significantly different
	if timestamp-tidx.lastTimestamp < tidx.minInterval && len(tidx.entries) > 0 {
		return nil
	}

	entry := TimeIndexEntry{
		Timestamp:      timestamp,
		RelativeOffset: relativeOffset,
	}

	tidx.entries = append(tidx.entries, entry)
	tidx.lastTimestamp = timestamp
	tidx.dirty = true

	// Update min/max
	if tidx.minTimestamp == 0 || timestamp < tidx.minTimestamp {
		tidx.minTimestamp = timestamp
	}
	if timestamp > tidx.maxTimestamp {
		tidx.maxTimestamp = timestamp
	}

	// Write entry to file
	data := make([]byte, TimeIndexEntrySize)
	PutUint64(data[0:8], uint64(entry.Timestamp))
	PutUint32(data[8:12], entry.RelativeOffset)

	position := TimeIndexHeaderSize + int64(len(tidx.entries)-1)*TimeIndexEntrySize
	_, err := tidx.file.WriteAt(data, position)

	return err
}

// Lookup finds the offset for messages at or after the given timestamp.
func (tidx *TimeIndex) Lookup(timestamp int64) (uint64, error) {
	tidx.mu.RLock()
	defer tidx.mu.RUnlock()

	if len(tidx.entries) == 0 {
		return tidx.baseOffset, nil
	}

	// Binary search for the first entry with timestamp >= target
	i := sort.Search(len(tidx.entries), func(i int) bool {
		return tidx.entries[i].Timestamp >= timestamp
	})

	if i == 0 {
		return tidx.baseOffset + uint64(tidx.entries[0].RelativeOffset), nil
	}

	if i >= len(tidx.entries) {
		// Return the last known offset
		return tidx.baseOffset + uint64(tidx.entries[len(tidx.entries)-1].RelativeOffset), nil
	}

	// Return the entry just before the target timestamp
	return tidx.baseOffset + uint64(tidx.entries[i-1].RelativeOffset), nil
}

// LookupBefore finds the offset for messages before the given timestamp.
func (tidx *TimeIndex) LookupBefore(timestamp int64) (uint64, error) {
	tidx.mu.RLock()
	defer tidx.mu.RUnlock()

	if len(tidx.entries) == 0 {
		return tidx.baseOffset, nil
	}

	// Binary search for the last entry with timestamp < target
	i := sort.Search(len(tidx.entries), func(i int) bool {
		return tidx.entries[i].Timestamp >= timestamp
	})

	if i == 0 {
		return tidx.baseOffset, nil
	}

	return tidx.baseOffset + uint64(tidx.entries[i-1].RelativeOffset), nil
}

// MinTimestamp returns the minimum timestamp in the index.
func (tidx *TimeIndex) MinTimestamp() time.Time {
	tidx.mu.RLock()
	defer tidx.mu.RUnlock()

	if tidx.minTimestamp == 0 {
		return time.Time{}
	}
	return time.UnixMilli(tidx.minTimestamp)
}

// MaxTimestamp returns the maximum timestamp in the index.
func (tidx *TimeIndex) MaxTimestamp() time.Time {
	tidx.mu.RLock()
	defer tidx.mu.RUnlock()

	if tidx.maxTimestamp == 0 {
		return time.Time{}
	}
	return time.UnixMilli(tidx.maxTimestamp)
}

// Entries returns all time index entries.
func (tidx *TimeIndex) Entries() []TimeIndexEntry {
	tidx.mu.RLock()
	defer tidx.mu.RUnlock()

	result := make([]TimeIndexEntry, len(tidx.entries))
	copy(result, tidx.entries)
	return result
}

// EntryCount returns the number of entries in the time index.
func (tidx *TimeIndex) EntryCount() int {
	tidx.mu.RLock()
	defer tidx.mu.RUnlock()
	return len(tidx.entries)
}

// SetMinInterval sets the minimum timestamp interval between entries.
func (tidx *TimeIndex) SetMinInterval(interval time.Duration) {
	tidx.mu.Lock()
	defer tidx.mu.Unlock()
	tidx.minInterval = interval.Milliseconds()
}

// Sync flushes the time index to disk.
func (tidx *TimeIndex) Sync() error {
	tidx.mu.Lock()
	defer tidx.mu.Unlock()

	if tidx.readonly || !tidx.dirty {
		return nil
	}

	if err := tidx.writeHeader(); err != nil {
		return err
	}

	tidx.dirty = false
	return tidx.file.Sync()
}

// Close closes the time index file.
func (tidx *TimeIndex) Close() error {
	tidx.mu.Lock()
	defer tidx.mu.Unlock()

	if tidx.dirty && !tidx.readonly {
		tidx.writeHeader()
		tidx.file.Sync()
	}

	return tidx.file.Close()
}

// Truncate removes entries beyond the given offset.
func (tidx *TimeIndex) Truncate(offset uint64) error {
	tidx.mu.Lock()
	defer tidx.mu.Unlock()

	if tidx.readonly {
		return fmt.Errorf("time index is readonly")
	}

	relOffset := uint32(offset - tidx.baseOffset)

	// Find first entry to remove
	i := sort.Search(len(tidx.entries), func(i int) bool {
		return tidx.entries[i].RelativeOffset >= relOffset
	})

	if i < len(tidx.entries) {
		tidx.entries = tidx.entries[:i]
		tidx.dirty = true

		// Update max timestamp
		if len(tidx.entries) > 0 {
			tidx.maxTimestamp = tidx.entries[len(tidx.entries)-1].Timestamp
		} else {
			tidx.maxTimestamp = 0
			tidx.minTimestamp = 0
		}

		newSize := TimeIndexHeaderSize + int64(len(tidx.entries))*TimeIndexEntrySize
		return tidx.file.Truncate(newSize)
	}

	return nil
}
