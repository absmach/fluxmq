// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"encoding/json"
	"testing"
	"time"

	raftv1 "github.com/absmach/fluxmq/pkg/proto/raft/v1"
	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLogEntryCodecRoundTripsEveryType(t *testing.T) {
	now := time.Date(2026, 8, 26, 10, 11, 12, 13, time.UTC)
	logTypes := []hraft.LogType{
		hraft.LogCommand,
		hraft.LogNoop,
		hraft.LogAddPeerDeprecated,
		hraft.LogRemovePeerDeprecated,
		hraft.LogBarrier,
		hraft.LogConfiguration,
	}
	for _, logType := range logTypes {
		t.Run(logType.String(), func(t *testing.T) {
			entry := &hraft.Log{Index: 7, Term: 4, Type: logType, Data: []byte{0, 1, 2, 255}, Extensions: []byte("extension"), AppendedAt: now}
			data, err := marshalLogEntry(entry)
			require.NoError(t, err)

			var decoded hraft.Log
			require.NoError(t, unmarshalLogEntry(data, &decoded))
			assert.Equal(t, *entry, decoded)

			reencoded, err := marshalLogEntry(&decoded)
			require.NoError(t, err)
			assert.Equal(t, data, reencoded)
		})
	}
}

func TestLogEntryCodecRejectsMalformedWire(t *testing.T) {
	tests := map[string]*raftv1.LogEntry{
		caseUnsupportedVersion: {Version: logEntryWireVersion + 1, Type: raftv1.LogType_LOG_TYPE_COMMAND},
		"unspecified log type": {Version: logEntryWireVersion},
		"unknown log type":     {Version: logEntryWireVersion, Type: raftv1.LogType(99)},
		caseInvalidTimestamp:   {Version: logEntryWireVersion, Type: raftv1.LogType_LOG_TYPE_COMMAND, AppendedAt: &timestamppb.Timestamp{Seconds: 253402300800}},
	}
	for name, wire := range tests {
		t.Run(name, func(t *testing.T) {
			data, err := proto.Marshal(wire)
			require.NoError(t, err)
			assert.ErrorIs(t, unmarshalLogEntry(data, new(hraft.Log)), errMalformedLogEntry)
		})
	}

	assert.ErrorIs(t, unmarshalLogEntry(nil, new(hraft.Log)), errMalformedLogEntry)
	assert.ErrorIs(t, unmarshalLogEntry([]byte{0x08, 0x01}, nil), errMalformedLogEntry)
	_, err := marshalLogEntry(nil)
	assert.ErrorIs(t, err, errMalformedLogEntry)
	_, err = marshalLogEntry(&hraft.Log{Type: hraft.LogType(255)})
	assert.ErrorIs(t, err, errMalformedLogEntry)

	legacyJSON, err := json.Marshal(&hraft.Log{Index: 1, Type: hraft.LogCommand})
	require.NoError(t, err)
	assert.ErrorIs(t, unmarshalLogEntry(legacyJSON, new(hraft.Log)), errMalformedLogEntry)
}

func TestLogEntryCodecRejectsUnknownFields(t *testing.T) {
	data, err := proto.Marshal(&raftv1.LogEntry{Version: logEntryWireVersion, Index: 9, Type: raftv1.LogType_LOG_TYPE_COMMAND})
	require.NoError(t, err)
	data = append(data, 0x98, 0x06, 0x01) // field 99, varint 1

	assert.ErrorIs(t, unmarshalLogEntry(data, new(hraft.Log)), errMalformedLogEntry)
}
