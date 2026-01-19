// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/absmach/fluxmq/queue/types"
	"github.com/hashicorp/raft"
)

// RaftOpType defines the type of Raft operation for log-based queues.
type RaftOpType uint8

const (
	// Log operations
	OpAppend RaftOpType = iota + 1
	OpTruncate

	// Consumer group operations
	OpCreateGroup
	OpDeleteGroup
	OpUpdateCursor
	OpUpdateCommitted
	OpAddPending
	OpRemovePending
	OpTransferPending

	// Consumer operations
	OpRegisterConsumer
	OpUnregisterConsumer
)

// RaftCommand represents a command to be applied to the FSM.
type RaftCommand struct {
	Op          RaftOpType `json:"op"`
	QueueName   string     `json:"queue_name"`
	PartitionID int        `json:"partition_id,omitempty"`
	GroupID     string     `json:"group_id,omitempty"`
	ConsumerID  string     `json:"consumer_id,omitempty"`

	// For append operations
	Message *types.Message `json:"message,omitempty"`

	// For truncate operations
	MinOffset uint64 `json:"min_offset,omitempty"`

	// For cursor operations
	Cursor    uint64 `json:"cursor,omitempty"`
	Committed uint64 `json:"committed,omitempty"`

	// For pending operations
	PendingEntry *types.PendingEntry `json:"pending_entry,omitempty"`
	Offset       uint64              `json:"offset,omitempty"`
	ToConsumer   string              `json:"to_consumer,omitempty"`

	// For consumer operations
	ConsumerInfo *types.ConsumerInfo `json:"consumer_info,omitempty"`

	// For group operations
	GroupState *types.ConsumerGroupState `json:"group_state,omitempty"`
	Pattern    string                    `json:"pattern,omitempty"`
}

// RaftResponse is the response from applying a command.
type RaftResponse struct {
	Offset uint64 `json:"offset,omitempty"`
	Error  string `json:"error,omitempty"`
}

// FSM implements the raft.FSM interface for log-based queues.
// It wraps a LogStore and ConsumerGroupStore to provide replicated state.
type FSM struct {
	manager *Manager
}

// NewFSM creates a new FSM for log-based queue replication.
func NewFSM(manager *Manager) *FSM {
	return &FSM{
		manager: manager,
	}
}

// Apply applies a Raft log entry to the FSM.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var cmd RaftCommand
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return &RaftResponse{Error: fmt.Sprintf("unmarshal error: %v", err)}
	}

	switch cmd.Op {
	case OpAppend:
		return f.applyAppend(&cmd)
	case OpTruncate:
		return f.applyTruncate(&cmd)
	case OpCreateGroup:
		return f.applyCreateGroup(&cmd)
	case OpDeleteGroup:
		return f.applyDeleteGroup(&cmd)
	case OpUpdateCursor:
		return f.applyUpdateCursor(&cmd)
	case OpUpdateCommitted:
		return f.applyUpdateCommitted(&cmd)
	case OpAddPending:
		return f.applyAddPending(&cmd)
	case OpRemovePending:
		return f.applyRemovePending(&cmd)
	case OpTransferPending:
		return f.applyTransferPending(&cmd)
	case OpRegisterConsumer:
		return f.applyRegisterConsumer(&cmd)
	case OpUnregisterConsumer:
		return f.applyUnregisterConsumer(&cmd)
	default:
		return &RaftResponse{Error: fmt.Sprintf("unknown operation: %d", cmd.Op)}
	}
}

func (f *FSM) applyAppend(cmd *RaftCommand) *RaftResponse {
	ctx := context.TODO()

	offset, err := f.manager.logStore.Append(ctx, cmd.QueueName, cmd.PartitionID, cmd.Message)
	if err != nil {
		return &RaftResponse{Error: err.Error()}
	}

	return &RaftResponse{Offset: offset}
}

func (f *FSM) applyTruncate(cmd *RaftCommand) *RaftResponse {
	ctx := context.TODO()
	err := f.manager.logStore.Truncate(ctx, cmd.QueueName, cmd.PartitionID, cmd.MinOffset)
	if err != nil {
		return &RaftResponse{Error: err.Error()}
	}

	return &RaftResponse{}
}

func (f *FSM) applyCreateGroup(cmd *RaftCommand) *RaftResponse {
	ctx := context.TODO()
	err := f.manager.groupStore.CreateConsumerGroup(ctx, cmd.GroupState)
	if err != nil {
		return &RaftResponse{Error: err.Error()}
	}

	return &RaftResponse{}
}

func (f *FSM) applyDeleteGroup(cmd *RaftCommand) *RaftResponse {
	ctx := context.TODO()
	err := f.manager.groupStore.DeleteConsumerGroup(ctx, cmd.QueueName, cmd.GroupID)
	if err != nil {
		return &RaftResponse{Error: err.Error()}
	}

	return &RaftResponse{}
}

func (f *FSM) applyUpdateCursor(cmd *RaftCommand) *RaftResponse {
	ctx := context.TODO()
	err := f.manager.groupStore.UpdateCursor(ctx, cmd.QueueName, cmd.GroupID, cmd.PartitionID, cmd.Cursor)
	if err != nil {
		return &RaftResponse{Error: err.Error()}
	}

	return &RaftResponse{}
}

func (f *FSM) applyUpdateCommitted(cmd *RaftCommand) *RaftResponse {
	ctx := context.TODO()
	err := f.manager.groupStore.UpdateCommitted(ctx, cmd.QueueName, cmd.GroupID, cmd.PartitionID, cmd.Committed)
	if err != nil {
		return &RaftResponse{Error: err.Error()}
	}

	return &RaftResponse{}
}

func (f *FSM) applyAddPending(cmd *RaftCommand) *RaftResponse {
	ctx := context.TODO()
	err := f.manager.groupStore.AddPendingEntry(ctx, cmd.QueueName, cmd.GroupID, cmd.PendingEntry)
	if err != nil {
		return &RaftResponse{Error: err.Error()}
	}

	return &RaftResponse{}
}

func (f *FSM) applyRemovePending(cmd *RaftCommand) *RaftResponse {
	ctx := context.TODO()
	err := f.manager.groupStore.RemovePendingEntry(ctx, cmd.QueueName, cmd.GroupID, cmd.ConsumerID, cmd.PartitionID, cmd.Offset)
	if err != nil {
		return &RaftResponse{Error: err.Error()}
	}

	return &RaftResponse{}
}

func (f *FSM) applyTransferPending(cmd *RaftCommand) *RaftResponse {
	ctx := context.TODO()
	err := f.manager.groupStore.TransferPendingEntry(ctx, cmd.QueueName, cmd.GroupID, cmd.PartitionID, cmd.Offset, cmd.ConsumerID, cmd.ToConsumer)
	if err != nil {
		return &RaftResponse{Error: err.Error()}
	}

	return &RaftResponse{}
}

func (f *FSM) applyRegisterConsumer(cmd *RaftCommand) *RaftResponse {
	ctx := context.TODO()
	err := f.manager.groupStore.RegisterConsumer(ctx, cmd.QueueName, cmd.GroupID, cmd.ConsumerInfo)
	if err != nil {
		return &RaftResponse{Error: err.Error()}
	}

	return &RaftResponse{}
}

func (f *FSM) applyUnregisterConsumer(cmd *RaftCommand) *RaftResponse {
	ctx := context.TODO()
	err := f.manager.groupStore.UnregisterConsumer(ctx, cmd.QueueName, cmd.GroupID, cmd.ConsumerID)
	if err != nil {
		return &RaftResponse{Error: err.Error()}
	}

	return &RaftResponse{}
}

// Snapshot returns a snapshot of the FSM state.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &FSMSnapshot{
		manager: f.manager,
	}, nil
}

// Restore restores the FSM from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshot SnapshotData
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return err
	}

	ctx := context.TODO()

	// Restore queues
	for _, config := range snapshot.Queues {
		if err := f.manager.logStore.CreateQueue(ctx, config); err != nil {
			// Ignore already exists errors during restore
			continue
		}
	}

	// Restore consumer groups
	for _, group := range snapshot.Groups {
		if err := f.manager.groupStore.CreateConsumerGroup(ctx, group); err != nil {
			// Update if already exists
			f.manager.groupStore.UpdateConsumerGroup(ctx, group)
		}
	}

	return nil
}

// FSMSnapshot implements raft.FSMSnapshot.
type FSMSnapshot struct {
	manager *Manager
}

// SnapshotData is the serialized snapshot format.
type SnapshotData struct {
	Queues []types.QueueConfig           `json:"queues"`
	Groups []*types.ConsumerGroupState   `json:"groups"`
}

// Persist writes the snapshot to the given sink.
func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	ctx := context.TODO()

	// Collect all queues
	queues, err := s.manager.logStore.ListQueues(ctx)
	if err != nil {
		sink.Cancel()
		return err
	}

	// Collect all consumer groups
	var groups []*types.ConsumerGroupState
	for _, queue := range queues {
		queueGroups, err := s.manager.groupStore.ListConsumerGroups(ctx, queue.Name)
		if err != nil {
			continue
		}
		groups = append(groups, queueGroups...)
	}

	// Serialize
	data := SnapshotData{
		Queues: queues,
		Groups: groups,
	}

	if err := json.NewEncoder(sink).Encode(data); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

// Release releases the snapshot resources.
func (s *FSMSnapshot) Release() {}

// ReplicatedManager wraps Manager to route writes through Raft.
type ReplicatedManager struct {
	*Manager
	raft *raft.Raft
}

// NewReplicatedManager creates a replicated manager that routes writes through Raft.
func NewReplicatedManager(manager *Manager, r *raft.Raft) *ReplicatedManager {
	return &ReplicatedManager{
		Manager: manager,
		raft:    r,
	}
}

// Apply applies a command through Raft consensus.
func (m *ReplicatedManager) Apply(cmd *RaftCommand) (*RaftResponse, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	future := m.raft.Apply(data, 5*1000*1000*1000) // 5 second timeout
	if err := future.Error(); err != nil {
		return nil, err
	}

	resp, ok := future.Response().(*RaftResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}

	return resp, nil
}

// IsLeader returns true if this node is the Raft leader.
func (m *ReplicatedManager) IsLeader() bool {
	return m.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader.
func (m *ReplicatedManager) LeaderAddr() string {
	addr, _ := m.raft.LeaderWithID()
	return string(addr)
}
