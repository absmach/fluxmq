// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"errors"
	"fmt"

	raftv1 "github.com/absmach/fluxmq/pkg/proto/raft/v1"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const logEntryWireVersion uint32 = 1

var errMalformedLogEntry = errors.New("malformed queue raft log entry")

func marshalLogEntry(entry *hraft.Log) ([]byte, error) {
	if entry == nil {
		return nil, fmt.Errorf("%w: entry is missing", errMalformedLogEntry)
	}
	logType, err := encodeLogType(entry.Type)
	if err != nil {
		return nil, err
	}
	wire := &raftv1.LogEntry{
		Version:    logEntryWireVersion,
		Index:      entry.Index,
		Term:       entry.Term,
		Type:       logType,
		Data:       entry.Data,
		Extensions: entry.Extensions,
	}
	if !entry.AppendedAt.IsZero() {
		wire.AppendedAt = timestamppb.New(entry.AppendedAt)
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal queue raft log entry: %w", err)
	}
	return data, nil
}

func unmarshalLogEntry(data []byte, entry *hraft.Log) error {
	if entry == nil {
		return fmt.Errorf("%w: destination is missing", errMalformedLogEntry)
	}
	if len(data) == 0 {
		return fmt.Errorf("%w: empty payload", errMalformedLogEntry)
	}

	wire := new(raftv1.LogEntry)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, wire); err != nil {
		return fmt.Errorf("%w: decode protobuf: %w", errMalformedLogEntry, err)
	}
	if err := rejectUnknownFields(wire.ProtoReflect()); err != nil {
		return fmt.Errorf("%w: %w", errMalformedLogEntry, err)
	}
	if wire.Version != logEntryWireVersion {
		return fmt.Errorf("%w: unsupported version %d", errMalformedLogEntry, wire.Version)
	}
	logType, err := decodeLogType(wire.Type)
	if err != nil {
		return err
	}

	appendedAt, err := decodeOperationTime(wire.AppendedAt, "appended_at")
	if err != nil {
		return fmt.Errorf("%w: %w", errMalformedLogEntry, err)
	}
	*entry = hraft.Log{
		Index:      wire.Index,
		Term:       wire.Term,
		Type:       logType,
		Data:       append([]byte(nil), wire.Data...),
		Extensions: append([]byte(nil), wire.Extensions...),
		AppendedAt: appendedAt,
	}
	return nil
}

func encodeLogType(logType hraft.LogType) (raftv1.LogType, error) {
	switch logType {
	case hraft.LogCommand:
		return raftv1.LogType_LOG_TYPE_COMMAND, nil
	case hraft.LogNoop:
		return raftv1.LogType_LOG_TYPE_NOOP, nil
	case hraft.LogAddPeerDeprecated:
		return raftv1.LogType_LOG_TYPE_ADD_PEER_DEPRECATED, nil
	case hraft.LogRemovePeerDeprecated:
		return raftv1.LogType_LOG_TYPE_REMOVE_PEER_DEPRECATED, nil
	case hraft.LogBarrier:
		return raftv1.LogType_LOG_TYPE_BARRIER, nil
	case hraft.LogConfiguration:
		return raftv1.LogType_LOG_TYPE_CONFIGURATION, nil
	default:
		return 0, fmt.Errorf("%w: unknown log type %d", errMalformedLogEntry, logType)
	}
}

func decodeLogType(logType raftv1.LogType) (hraft.LogType, error) {
	switch logType {
	case raftv1.LogType_LOG_TYPE_COMMAND:
		return hraft.LogCommand, nil
	case raftv1.LogType_LOG_TYPE_NOOP:
		return hraft.LogNoop, nil
	case raftv1.LogType_LOG_TYPE_ADD_PEER_DEPRECATED:
		return hraft.LogAddPeerDeprecated, nil
	case raftv1.LogType_LOG_TYPE_REMOVE_PEER_DEPRECATED:
		return hraft.LogRemovePeerDeprecated, nil
	case raftv1.LogType_LOG_TYPE_BARRIER:
		return hraft.LogBarrier, nil
	case raftv1.LogType_LOG_TYPE_CONFIGURATION:
		return hraft.LogConfiguration, nil
	default:
		return 0, fmt.Errorf("%w: unknown log type %d", errMalformedLogEntry, logType)
	}
}
