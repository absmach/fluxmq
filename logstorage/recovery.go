// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// RecoveryResult contains the result of a recovery operation.
type RecoveryResult struct {
	SegmentsRecovered int
	SegmentsTruncated int
	IndexesRebuilt    int
	MessagesLost      uint64
	BytesTruncated    int64
	Errors            []error
}

// RecoverSegment attempts to recover a corrupted segment.
// It scans from the beginning and truncates at the first corruption point.
func RecoverSegment(dir string, baseOffset uint64) (*RecoveryResult, error) {
	result := &RecoveryResult{}

	segPath := filepath.Join(dir, FormatSegmentName(baseOffset))
	file, err := os.OpenFile(segPath, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment for recovery: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// Scan through batches
	var pos int64 = 0
	var lastValidPos int64 = 0
	var messagesRecovered uint64 = 0
	header := make([]byte, BatchHeaderSize)

	for pos < info.Size() {
		n, err := file.ReadAt(header, pos)
		if err == io.EOF {
			break
		}
		if err != nil || n < BatchHeaderSize {
			// Corruption or incomplete batch
			result.BytesTruncated = info.Size() - lastValidPos
			break
		}

		// Validate magic
		magic := GetUint32(header[0:4])
		if magic != SegmentMagic {
			result.BytesTruncated = info.Size() - lastValidPos
			break
		}

		// Read and validate CRC
		storedCRC := GetUint32(header[4:8])
		batchLen := GetUint32(header[12:16])
		count := GetUint16(header[16:18])
		totalSize := BatchHeaderSize + int(batchLen)

		if pos+int64(totalSize) > info.Size() {
			// Incomplete batch
			result.BytesTruncated = info.Size() - lastValidPos
			break
		}

		// Read full batch for CRC validation
		batchData := make([]byte, totalSize)
		_, err = file.ReadAt(batchData, pos)
		if err != nil {
			result.BytesTruncated = info.Size() - lastValidPos
			break
		}

		// Validate CRC (over data after CRC field)
		computedCRC := Checksum(batchData[8:])
		if storedCRC != computedCRC {
			result.BytesTruncated = info.Size() - lastValidPos
			result.Errors = append(result.Errors, fmt.Errorf("CRC mismatch at position %d", pos))
			break
		}

		// Batch is valid
		lastValidPos = pos + int64(totalSize)
		messagesRecovered += uint64(count)
		pos = lastValidPos
	}

	// Truncate if needed
	if lastValidPos < info.Size() {
		result.SegmentsTruncated = 1
		result.MessagesLost = messagesRecovered // Estimate, actual may differ

		if err := file.Truncate(lastValidPos); err != nil {
			return result, fmt.Errorf("failed to truncate segment: %w", err)
		}
	}

	result.SegmentsRecovered = 1

	// Rebuild indexes
	if err := rebuildIndexes(dir, baseOffset); err != nil {
		result.Errors = append(result.Errors, err)
	} else {
		result.IndexesRebuilt = 1
	}

	return result, nil
}

// rebuildIndexes rebuilds both offset and time indexes for a segment.
func rebuildIndexes(dir string, baseOffset uint64) error {
	// Open segment in readonly mode
	seg, err := OpenSegment(dir, baseOffset, true)
	if err != nil {
		return err
	}
	defer seg.Close()

	// Rebuild offset index
	indexPath := filepath.Join(dir, FormatIndexName(baseOffset))
	os.Remove(indexPath)

	index, err := CreateIndex(indexPath, baseOffset, DefaultIndexIntervalBytes)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer index.Close()

	// Rebuild time index
	timeIndexPath := filepath.Join(dir, FormatTimeIndexName(baseOffset))
	os.Remove(timeIndexPath)

	timeIndex, err := CreateTimeIndex(timeIndexPath, baseOffset)
	if err != nil {
		return fmt.Errorf("failed to create time index: %w", err)
	}
	defer timeIndex.Close()

	// Scan segment and build indexes
	var bytesSinceLastIndex int
	for _, bp := range seg.batchPositions {
		relOffset := uint32(bp.offset - baseOffset)

		// Add to offset index at intervals
		if bytesSinceLastIndex == 0 || bytesSinceLastIndex >= DefaultIndexIntervalBytes {
			index.Append(relOffset, uint32(bp.position))
			bytesSinceLastIndex = 0
		}
		bytesSinceLastIndex += bp.size

		// Read batch for timestamp
		batch, err := seg.ReadBatch(bp.offset)
		if err == nil && batch.MaxTimestamp > 0 {
			timeIndex.Append(batch.MaxTimestamp, relOffset)
		}
	}

	return nil
}

// RecoverSegments recovers all segments in a directory.
func RecoverSegments(dir string) (*RecoveryResult, error) {
	result := &RecoveryResult{}

	pattern := filepath.Join(dir, "*"+SegmentExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		name := filepath.Base(f)
		baseOffset, err := ParseSegmentName(name)
		if err != nil {
			continue
		}

		segResult, err := RecoverSegment(dir, baseOffset)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("segment %d: %w", baseOffset, err))
			continue
		}

		result.SegmentsRecovered += segResult.SegmentsRecovered
		result.SegmentsTruncated += segResult.SegmentsTruncated
		result.IndexesRebuilt += segResult.IndexesRebuilt
		result.MessagesLost += segResult.MessagesLost
		result.BytesTruncated += segResult.BytesTruncated
		result.Errors = append(result.Errors, segResult.Errors...)
	}

	return result, nil
}
