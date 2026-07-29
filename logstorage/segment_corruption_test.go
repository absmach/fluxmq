// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeSegmentWithBatches creates a segment holding one batch per value slice
// and returns its directory and file path.
func writeSegmentWithBatches(t *testing.T, values ...[]string) (dir, path string) {
	t.Helper()

	dir = t.TempDir()
	seg, err := CreateSegment(dir, 0, DefaultSegmentConfig())
	require.NoError(t, err)

	var offset uint64
	for _, batchValues := range values {
		batch := NewBatch(offset)
		for _, v := range batchValues {
			batch.Append([]byte(v), nil, nil)
		}
		_, err := seg.Append(batch)
		require.NoError(t, err)
		offset += uint64(len(batchValues))
	}
	require.NoError(t, seg.Close())

	return dir, filepath.Join(dir, FormatSegmentName(0))
}

func appendToSegmentFile(t *testing.T, path string, data []byte) {
	t.Helper()

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// TestOpenSegmentFailsClosedOnCorruption covers the damage shapes a crashed or
// bit-rotted segment can take. Each must be reported, never silently accepted:
// the scan position becomes the next append offset, so treating the corruption
// point as the end of the log would overwrite or write past the damage.
func TestOpenSegmentFailsClosedOnCorruption(t *testing.T) {
	tests := []struct {
		name   string
		damage func(t *testing.T, path string)
	}{
		{
			name: "partial batch header",
			damage: func(t *testing.T, path string) {
				appendToSegmentFile(t, path, make([]byte, BatchHeaderSize-1))
			},
		},
		{
			name: "trailing garbage with bad magic",
			damage: func(t *testing.T, path string) {
				appendToSegmentFile(t, path, make([]byte, BatchHeaderSize+16))
			},
		},
		{
			name: "batch length past end of file",
			damage: func(t *testing.T, path string) {
				f, err := os.OpenFile(path, os.O_RDWR, 0o644)
				require.NoError(t, err)
				defer f.Close()

				// Inflate the first batch's declared length.
				var buf [4]byte
				PutUint32(buf[:], 1<<20)
				_, err = f.WriteAt(buf[:], 16)
				require.NoError(t, err)
			},
		},
		{
			name: "magic corrupted in place",
			damage: func(t *testing.T, path string) {
				f, err := os.OpenFile(path, os.O_RDWR, 0o644)
				require.NoError(t, err)
				defer f.Close()

				_, err = f.WriteAt([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 0)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, path := writeSegmentWithBatches(t, []string{"a", "b"})
			tt.damage(t, path)

			seg, err := OpenSegment(dir, 0, false)
			if seg != nil {
				seg.Close() //nolint:errcheck // only reached when the assertion below fails
			}
			require.ErrorIs(t, err, ErrSegmentCorrupted)
		})
	}
}

func TestOpenSegmentAcceptsIntactSegment(t *testing.T) {
	dir, _ := writeSegmentWithBatches(t, []string{"a", "b"}, []string{"c"})

	seg, err := OpenSegment(dir, 0, false)
	require.NoError(t, err)
	defer seg.Close()

	assert.Equal(t, uint64(3), seg.NextOffset())
	assert.Len(t, seg.batchPositions, 2, "both batches must be indexed by the scan")
}

// TestRecoverSegmentTruncatesThenOpens is the supported path out of corruption:
// recovery discards the damaged tail, after which the segment opens cleanly and
// appends land after the last valid batch rather than on top of it.
func TestRecoverSegmentTruncatesThenOpens(t *testing.T) {
	dir, path := writeSegmentWithBatches(t, []string{"a", "b"})

	intact, err := os.Stat(path)
	require.NoError(t, err)
	intactSize := intact.Size()

	appendToSegmentFile(t, path, make([]byte, BatchHeaderSize+7))

	_, err = OpenSegment(dir, 0, false)
	require.ErrorIs(t, err, ErrSegmentCorrupted, "damaged segment must not open before recovery")

	result, err := RecoverSegment(dir, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, result.SegmentsTruncated)

	truncated, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, intactSize, truncated.Size(), "recovery must cut back to the last valid batch")

	seg, err := OpenSegment(dir, 0, false)
	require.NoError(t, err)
	defer seg.Close()

	require.Equal(t, uint64(2), seg.NextOffset())

	// The recovered segment must keep appending after the surviving data.
	batch := NewBatch(2)
	batch.Append([]byte("c"), nil, nil)
	_, err = seg.Append(batch)
	require.NoError(t, err)

	msg, err := seg.Read(0)
	require.NoError(t, err)
	assert.Equal(t, []byte("a"), msg.Value, "recovered data must survive the following append")

	msg, err = seg.Read(2)
	require.NoError(t, err)
	assert.Equal(t, []byte("c"), msg.Value)
}
