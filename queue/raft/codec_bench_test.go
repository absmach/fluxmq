// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/types"
	hraft "github.com/hashicorp/raft"
)

var (
	benchmarkCodecBytes []byte
	benchmarkCodecOp    *Operation
	benchmarkCodecLog   hraft.Log
)

func BenchmarkOperationCodecMarshal(b *testing.B) {
	op := benchmarkAppendOperation()
	b.ReportAllocs()
	b.SetBytes(int64(len(op.Message)))
	b.ResetTimer()
	for range b.N {
		var err error
		benchmarkCodecBytes, err = marshalOperation(op)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOperationCodecUnmarshal(b *testing.B) {
	data, err := marshalOperation(benchmarkAppendOperation())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for range b.N {
		benchmarkCodecOp, err = unmarshalOperation(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogEntryCodecMarshal(b *testing.B) {
	data, err := marshalOperation(benchmarkAppendOperation())
	if err != nil {
		b.Fatal(err)
	}
	entry := &hraft.Log{Index: 42, Term: 7, Type: hraft.LogCommand, Data: data, AppendedAt: time.Unix(1_777_000_000, 123).UTC()}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for range b.N {
		benchmarkCodecBytes, err = marshalLogEntry(entry)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogEntryCodecUnmarshal(b *testing.B) {
	operation, err := marshalOperation(benchmarkAppendOperation())
	if err != nil {
		b.Fatal(err)
	}
	data, err := marshalLogEntry(&hraft.Log{Index: 42, Term: 7, Type: hraft.LogCommand, Data: operation, AppendedAt: time.Unix(1_777_000_000, 123).UTC()})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for range b.N {
		var entry hraft.Log
		if err = unmarshalLogEntry(data, &entry); err != nil {
			b.Fatal(err)
		}
		benchmarkCodecLog = entry
	}
}

func benchmarkAppendOperation() *Operation {
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	return &Operation{
		Type:      OpAppend,
		Timestamp: time.Unix(1_777_000_000, 123).UTC(),
		QueueName: "benchmark-queue",
		Message:   payload,
		DedupeKey: "transfer-benchmark-key",
	}
}

// A snapshot is written per compaction rather than per publish. What matters is
// that it stays linear in the state it carries and that neither side has to
// hold a queue's records in memory at once, so these drive the frame writer and
// reader over a stream rather than a single message.
func BenchmarkSnapshotCodecWrite(b *testing.B) {
	queues, records := benchmarkSnapshotQueues()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var sink nopWriter
		writer := newSnapshotWriter(&sink)
		if err := writer.WriteHeader(conformanceTime); err != nil {
			b.Fatal(err)
		}
		for _, queue := range queues {
			if err := writer.WriteQueue(queue); err != nil {
				b.Fatal(err)
			}
			for offset, record := range records {
				if err := writer.WriteRecord(uint64(offset), record); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

func BenchmarkSnapshotCodecRead(b *testing.B) {
	queues, records := benchmarkSnapshotQueues()
	var buf bytes.Buffer
	writer := newSnapshotWriter(&buf)
	if err := writer.WriteHeader(conformanceTime); err != nil {
		b.Fatal(err)
	}
	for _, queue := range queues {
		if err := writer.WriteQueue(queue); err != nil {
			b.Fatal(err)
		}
		for offset, record := range records {
			if err := writer.WriteRecord(uint64(offset), record); err != nil {
				b.Fatal(err)
			}
		}
	}
	data := buf.Bytes()

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for range b.N {
		reader := newSnapshotReader(bytes.NewReader(data))
		if err := reader.ReadHeader(); err != nil {
			b.Fatal(err)
		}
		for {
			entry, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			benchmarkCodecEntry = entry
		}
	}
}

var benchmarkCodecEntry snapshotEntry

func benchmarkSnapshotQueues() ([]QueueSnapshotData, [][]byte) {
	queues := make([]QueueSnapshotData, 0, 16)
	for range 16 {
		config := conformanceQueueConfig()
		queues = append(queues, QueueSnapshotData{
			QueueName:   config.Name,
			QueueConfig: &config,
			Groups:      []*types.ConsumerGroup{conformanceConsumerGroup(conformanceTime)},
			Tail:        8,
		})
	}
	records := make([][]byte, 8)
	for i := range records {
		records[i] = make([]byte, 1024)
	}
	return queues, records
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
