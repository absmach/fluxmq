// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"time"

	raftv1 "github.com/absmach/fluxmq/pkg/proto/raft/v1"
	"github.com/absmach/fluxmq/queue/types"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const snapshotWireVersion uint32 = 1

// snapshotReadBuffer sizes the reader that frames are decoded through. A frame
// is one record at most, so this bounds the read side no matter how long the
// stream is.
const snapshotReadBuffer = 64 << 10

// snapshotMaxFrame caps a single frame.
//
// protodelim defaults to 4 MiB, which is below what a queue may hold: a record
// frame carries a whole envelope, and max_message_size defaults to 10 MiB and
// can be configured higher. A cap under that produces snapshots this build
// writes happily and then cannot read, so it has to be well clear of any record
// the broker would have accepted in the first place.
const snapshotMaxFrame = 128 << 20

var errMalformedSnapshot = errors.New("malformed queue raft snapshot")

// snapshotWriter writes a snapshot as length-delimited frames.
//
// Framing rather than one message is what keeps the cost of a snapshot
// independent of how much a queue holds: a record is encoded, written, and
// dropped before the next one is touched, on both sides of the trip.
type snapshotWriter struct {
	w       io.Writer
	frame   raftv1.SnapshotFrame
	written int64
}

func newSnapshotWriter(w io.Writer) *snapshotWriter {
	return &snapshotWriter{w: w}
}

func (sw *snapshotWriter) writeFrame(frame *raftv1.SnapshotFrame) error {
	n, err := protodelim.MarshalTo(sw.w, frame)
	if err != nil {
		return fmt.Errorf("write queue raft snapshot frame: %w", err)
	}
	sw.written += int64(n)
	return nil
}

func (sw *snapshotWriter) WriteHeader(timestamp time.Time) error {
	header := &raftv1.SnapshotHeader{Version: snapshotWireVersion}
	if !timestamp.IsZero() {
		header.Timestamp = timestamppb.New(timestamp)
	}
	sw.frame.Frame = &raftv1.SnapshotFrame_Header{Header: header}
	return sw.writeFrame(&sw.frame)
}

// WriteQueue opens a queue. Every record written after it belongs to this
// queue until the next call.
func (sw *snapshotWriter) WriteQueue(queue QueueSnapshotData) error {
	wire := &raftv1.QueueSnapshot{
		QueueName: queue.QueueName,
		Head:      queue.Head,
		Tail:      queue.Tail,
	}
	if queue.QueueConfig != nil {
		config, err := encodeOperationQueueConfig(queue.QueueConfig)
		if err != nil {
			return fmt.Errorf("%w: queue %q: %w", errMalformedSnapshot, queue.QueueName, err)
		}
		wire.Config = config
	}
	for _, group := range queue.Groups {
		if group == nil {
			continue
		}
		state, err := encodeOperationGroup(group)
		if err != nil {
			return fmt.Errorf("%w: queue %q group: %w", errMalformedSnapshot, queue.QueueName, err)
		}
		wire.Groups = append(wire.Groups, state)
	}

	sw.frame.Frame = &raftv1.SnapshotFrame_Queue{Queue: wire}
	return sw.writeFrame(&sw.frame)
}

func (sw *snapshotWriter) WriteRecord(offset uint64, envelope []byte) error {
	sw.frame.Frame = &raftv1.SnapshotFrame_Record{Record: &raftv1.SnapshotRecord{
		Offset:   offset,
		Envelope: envelope,
	}}
	return sw.writeFrame(&sw.frame)
}

// QueueSnapshotData is one queue's replicated state: its configuration, its
// consumer groups, and the offset range its log covers. The records themselves
// are streamed beside it rather than held here.
type QueueSnapshotData struct {
	QueueName   string
	QueueConfig *types.QueueConfig
	Groups      []*types.ConsumerGroup
	Head        uint64
	Tail        uint64
}

// snapshotReader walks a snapshot one frame at a time.
type snapshotReader struct {
	r         protodelim.Reader
	unmarshal protodelim.UnmarshalOptions
	timestamp time.Time
}

func newSnapshotReader(r io.Reader) *snapshotReader {
	return &snapshotReader{
		r:         bufio.NewReaderSize(r, snapshotReadBuffer),
		unmarshal: protodelim.UnmarshalOptions{MaxSize: snapshotMaxFrame},
	}
}

// ReadHeader consumes the leading header frame. A stream that does not open
// with one is not a snapshot this build wrote.
func (sr *snapshotReader) ReadHeader() error {
	frame := new(raftv1.SnapshotFrame)
	if err := sr.next(frame); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: empty payload", errMalformedSnapshot)
		}
		return err
	}

	header, ok := frame.Frame.(*raftv1.SnapshotFrame_Header)
	if !ok || header.Header == nil {
		return fmt.Errorf("%w: stream does not open with a header", errMalformedSnapshot)
	}
	if header.Header.Version != snapshotWireVersion {
		return fmt.Errorf("%w: unsupported version %d", errMalformedSnapshot, header.Header.Version)
	}

	timestamp, err := decodeOperationTime(header.Header.Timestamp, "timestamp")
	if err != nil {
		return fmt.Errorf("%w: %w", errMalformedSnapshot, err)
	}
	sr.timestamp = timestamp
	return nil
}

// snapshotEntry is one decoded frame after the header: either a queue opening
// or a record belonging to the queue most recently opened.
type snapshotEntry struct {
	Queue  *QueueSnapshotData
	Record *raftv1.SnapshotRecord
}

// Next returns the next entry, or io.EOF at the end of the stream.
func (sr *snapshotReader) Next() (snapshotEntry, error) {
	frame := new(raftv1.SnapshotFrame)
	if err := sr.next(frame); err != nil {
		return snapshotEntry{}, err
	}

	switch payload := frame.Frame.(type) {
	case *raftv1.SnapshotFrame_Queue:
		if payload.Queue == nil {
			return snapshotEntry{}, fmt.Errorf("%w: queue frame is missing", errMalformedSnapshot)
		}
		queue, err := decodeQueueSnapshot(payload.Queue)
		if err != nil {
			return snapshotEntry{}, err
		}
		return snapshotEntry{Queue: queue}, nil
	case *raftv1.SnapshotFrame_Record:
		if payload.Record == nil {
			return snapshotEntry{}, fmt.Errorf("%w: record frame is missing", errMalformedSnapshot)
		}
		return snapshotEntry{Record: payload.Record}, nil
	case *raftv1.SnapshotFrame_Header:
		return snapshotEntry{}, fmt.Errorf("%w: a second header frame", errMalformedSnapshot)
	default:
		return snapshotEntry{}, fmt.Errorf("%w: unsupported frame %T", errMalformedSnapshot, payload)
	}
}

func (sr *snapshotReader) next(frame *raftv1.SnapshotFrame) error {
	if err := sr.unmarshal.UnmarshalFrom(sr.r, frame); err != nil {
		if errors.Is(err, io.EOF) {
			return io.EOF
		}
		return fmt.Errorf("%w: decode protobuf: %w", errMalformedSnapshot, err)
	}
	if err := rejectUnknownFields(frame.ProtoReflect()); err != nil {
		return fmt.Errorf("%w: %w", errMalformedSnapshot, err)
	}
	return nil
}

func decodeQueueSnapshot(wire *raftv1.QueueSnapshot) (*QueueSnapshotData, error) {
	queue := &QueueSnapshotData{
		QueueName: wire.QueueName,
		Head:      wire.Head,
		Tail:      wire.Tail,
	}
	if wire.Tail < wire.Head {
		return nil, fmt.Errorf("%w: queue %q tail %d precedes head %d", errMalformedSnapshot, wire.QueueName, wire.Tail, wire.Head)
	}
	if wire.Config != nil {
		config, err := decodeOperationQueueConfig(wire.Config)
		if err != nil {
			return nil, fmt.Errorf("%w: queue %q: %w", errMalformedSnapshot, wire.QueueName, err)
		}
		queue.QueueConfig = config
	}
	for _, state := range wire.Groups {
		group, err := decodeOperationGroup(state)
		if err != nil {
			return nil, fmt.Errorf("%w: queue %q group: %w", errMalformedSnapshot, wire.QueueName, err)
		}
		queue.Groups = append(queue.Groups, group)
	}
	return queue, nil
}
