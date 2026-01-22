// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"fmt"
	"os"
	"sort"
	"sync"
)

// Index header size: Magic(4) + Version(1) + BaseOffset(8) + EntryCount(4) + IndexInterval(4) + Reserved(11) = 32 bytes
const IndexHeaderSize = 32

// Index entry size: RelativeOffset(4) + FilePosition(4) = 8 bytes
const IndexEntrySize = 8

// Index provides sparse offset-to-position mapping for a segment.
type Index struct {
	mu sync.RWMutex

	path          string
	baseOffset    uint64
	indexInterval int

	file     *os.File
	entries  []IndexEntry
	readonly bool
	dirty    bool

	// Cached for fast append
	lastOffset          uint32
	bytesSinceLastEntry int
}

// CreateIndex creates a new index file.
func CreateIndex(path string, baseOffset uint64, indexInterval int) (*Index, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create index file: %w", err)
	}

	idx := &Index{
		path:          path,
		baseOffset:    baseOffset,
		indexInterval: indexInterval,
		file:          file,
		entries:       make([]IndexEntry, 0, 1024),
		readonly:      false,
		dirty:         true,
	}

	// Write header
	if err := idx.writeHeader(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}

	return idx, nil
}

// OpenIndex opens an existing index file.
func OpenIndex(path string, baseOffset uint64, readonly bool) (*Index, error) {
	flags := os.O_RDWR
	if readonly {
		flags = os.O_RDONLY
	}

	file, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	idx := &Index{
		path:       path,
		baseOffset: baseOffset,
		file:       file,
		readonly:   readonly,
	}

	// Read and validate header
	if err := idx.readHeader(); err != nil {
		file.Close()
		return nil, err
	}

	// Load entries
	if err := idx.loadEntries(); err != nil {
		file.Close()
		return nil, err
	}

	return idx, nil
}

// writeHeader writes the index file header.
func (idx *Index) writeHeader() error {
	header := make([]byte, IndexHeaderSize)

	PutUint32(header[0:4], IndexMagic)
	header[4] = IndexVersion
	PutUint64(header[5:13], idx.baseOffset)
	PutUint32(header[13:17], uint32(len(idx.entries)))
	PutUint32(header[17:21], uint32(idx.indexInterval))
	// Remaining bytes are reserved/padding

	_, err := idx.file.WriteAt(header, 0)
	return err
}

// readHeader reads and validates the index file header.
func (idx *Index) readHeader() error {
	header := make([]byte, IndexHeaderSize)

	n, err := idx.file.ReadAt(header, 0)
	if err != nil {
		return fmt.Errorf("failed to read index header: %w", err)
	}
	if n < IndexHeaderSize {
		return ErrIndexCorrupted
	}

	// Validate magic
	magic := GetUint32(header[0:4])
	if magic != IndexMagic {
		return ErrIndexCorrupted
	}

	// Read version
	version := header[4]
	if version > IndexVersion {
		return fmt.Errorf("unsupported index version: %d", version)
	}

	// Read base offset
	storedBaseOffset := GetUint64(header[5:13])
	if storedBaseOffset != idx.baseOffset {
		return fmt.Errorf("index base offset mismatch: expected %d, got %d", idx.baseOffset, storedBaseOffset)
	}

	idx.indexInterval = int(GetUint32(header[17:21]))
	if idx.indexInterval == 0 {
		idx.indexInterval = DefaultIndexIntervalBytes
	}

	return nil
}

// loadEntries loads all index entries from the file.
func (idx *Index) loadEntries() error {
	info, err := idx.file.Stat()
	if err != nil {
		return err
	}

	entriesSize := info.Size() - IndexHeaderSize
	if entriesSize < 0 || entriesSize%IndexEntrySize != 0 {
		return ErrIndexCorrupted
	}

	entryCount := int(entriesSize / IndexEntrySize)
	idx.entries = make([]IndexEntry, entryCount)

	if entryCount == 0 {
		return nil
	}

	// Read all entries
	data := make([]byte, entriesSize)
	_, err = idx.file.ReadAt(data, IndexHeaderSize)
	if err != nil {
		return fmt.Errorf("failed to read index entries: %w", err)
	}

	// Parse entries
	for i := 0; i < entryCount; i++ {
		offset := i * IndexEntrySize
		idx.entries[i] = IndexEntry{
			RelativeOffset: GetUint32(data[offset : offset+4]),
			FilePosition:   GetUint32(data[offset+4 : offset+8]),
		}
	}

	if entryCount > 0 {
		idx.lastOffset = idx.entries[entryCount-1].RelativeOffset
	}

	return nil
}

// Append adds a new entry to the index.
func (idx *Index) Append(relativeOffset uint32, filePosition uint32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.readonly {
		return fmt.Errorf("index is readonly")
	}

	entry := IndexEntry{
		RelativeOffset: relativeOffset,
		FilePosition:   filePosition,
	}

	idx.entries = append(idx.entries, entry)
	idx.lastOffset = relativeOffset
	idx.dirty = true

	// Write entry to file
	data := make([]byte, IndexEntrySize)
	PutUint32(data[0:4], entry.RelativeOffset)
	PutUint32(data[4:8], entry.FilePosition)

	position := IndexHeaderSize + int64(len(idx.entries)-1)*IndexEntrySize
	_, err := idx.file.WriteAt(data, position)

	return err
}

// Lookup finds the file position for the given offset.
// Returns the position of the batch containing or preceding the offset.
func (idx *Index) Lookup(offset uint64) (int64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return 0, nil
	}

	relOffset := uint32(offset - idx.baseOffset)

	// Binary search for the largest entry <= relOffset
	i := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].RelativeOffset > relOffset
	})

	if i == 0 {
		return int64(idx.entries[0].FilePosition), nil
	}

	return int64(idx.entries[i-1].FilePosition), nil
}

// LookupEntry returns the index entry for the given offset.
func (idx *Index) LookupEntry(offset uint64) (IndexEntry, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return IndexEntry{}, false
	}

	relOffset := uint32(offset - idx.baseOffset)

	i := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].RelativeOffset > relOffset
	})

	if i == 0 {
		return idx.entries[0], true
	}

	return idx.entries[i-1], true
}

// ShouldIndex returns true if we should add an index entry based on bytes written.
func (idx *Index) ShouldIndex(bytesWritten int) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.bytesSinceLastEntry += bytesWritten
	if idx.bytesSinceLastEntry >= idx.indexInterval {
		idx.bytesSinceLastEntry = 0
		return true
	}
	return false
}

// Entries returns all index entries.
func (idx *Index) Entries() []IndexEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make([]IndexEntry, len(idx.entries))
	copy(result, idx.entries)
	return result
}

// EntryCount returns the number of entries in the index.
func (idx *Index) EntryCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// Sync flushes the index to disk.
func (idx *Index) Sync() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.readonly || !idx.dirty {
		return nil
	}

	// Update header with entry count
	if err := idx.writeHeader(); err != nil {
		return err
	}

	idx.dirty = false
	return idx.file.Sync()
}

// Close closes the index file.
func (idx *Index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.dirty && !idx.readonly {
		idx.writeHeader()
		idx.file.Sync()
	}

	return idx.file.Close()
}

// Truncate removes entries beyond the given offset.
func (idx *Index) Truncate(offset uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.readonly {
		return fmt.Errorf("index is readonly")
	}

	relOffset := uint32(offset - idx.baseOffset)

	// Find first entry to keep
	i := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].RelativeOffset >= relOffset
	})

	if i < len(idx.entries) {
		idx.entries = idx.entries[:i]
		idx.dirty = true

		// Truncate file
		newSize := IndexHeaderSize + int64(len(idx.entries))*IndexEntrySize
		return idx.file.Truncate(newSize)
	}

	return nil
}
