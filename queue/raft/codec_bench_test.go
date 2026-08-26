// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
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

var benchmarkCodecSnapshot *GlobalSnapshotData

func BenchmarkSnapshotCodecMarshal(b *testing.B) {
	snapshot := benchmarkSnapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var err error
		benchmarkCodecBytes, err = marshalSnapshot(snapshot)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSnapshotCodecUnmarshal(b *testing.B) {
	data, err := marshalSnapshot(benchmarkSnapshot())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for range b.N {
		benchmarkCodecSnapshot, err = unmarshalSnapshot(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// A snapshot is written per compaction rather than per publish, so what matters
// here is that it stays linear in the state it carries.
func benchmarkSnapshot() *GlobalSnapshotData {
	queues := make([]QueueSnapshotData, 0, 16)
	for range 16 {
		config := conformanceQueueConfig()
		queues = append(queues, QueueSnapshotData{
			QueueName:   config.Name,
			QueueConfig: &config,
			Groups:      []*types.ConsumerGroup{conformanceConsumerGroup(conformanceTime)},
		})
	}
	return &GlobalSnapshotData{Queues: queues, Timestamp: conformanceTime}
}
