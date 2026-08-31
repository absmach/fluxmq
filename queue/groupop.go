// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"fmt"
	"time"

	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/queue/raft"
	"github.com/absmach/fluxmq/queue/types"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// ErrUnsupportedGroupOp identifies a forwarded operation this node cannot
	// apply: an operation type that does not travel this RPC at all.
	ErrUnsupportedGroupOp = errors.New("unsupported consumer group operation")

	// ErrMalformedGroupOp identifies an operation of a supported type whose
	// payload is unusable: a required value is absent, an identifier is empty,
	// or a nested identity contradicts the envelope. It is a peer protocol
	// violation, distinct from an operation this node simply does not handle.
	ErrMalformedGroupOp = errors.New("malformed consumer group operation")
)

// This file is the only place the consumer group mutations cross between their
// in-process Go form and the cluster wire. Keeping both directions adjacent is
// what stops the two from drifting; the wire form is a typed oneof rather than
// an opaque blob precisely so a mismatch is a compile error here rather than a
// decode surprise on another node.

// encodeGroupOperation converts a replicated group mutation into its wire form.
// It rejects operation types that are not group mutations: the leader applies
// those directly through Raft and never receives them over this RPC.
func encodeGroupOperation(op *raft.Operation) (*clusterv1.GroupOperation, error) {
	if err := validateGroupOperation(op); err != nil {
		return nil, err
	}

	wire := &clusterv1.GroupOperation{
		QueueName: op.QueueName,
		GroupId:   op.GroupID,
	}
	if !op.Timestamp.IsZero() {
		wire.Timestamp = timestamppb.New(op.Timestamp)
	}

	switch op.Type {
	case raft.OpCreateGroup:
		wire.Operation = &clusterv1.GroupOperation_CreateGroup{
			CreateGroup: &clusterv1.CreateGroupOp{Group: encodeConsumerGroup(op.GroupState)},
		}
	case raft.OpUpdateGroup:
		wire.Operation = &clusterv1.GroupOperation_UpdateGroup{
			UpdateGroup: &clusterv1.UpdateGroupOp{Group: encodeConsumerGroup(op.GroupState)},
		}
	case raft.OpDeleteGroup:
		wire.Operation = &clusterv1.GroupOperation_DeleteGroup{DeleteGroup: &clusterv1.DeleteGroupOp{}}
	case raft.OpUpdateCursor:
		wire.Operation = &clusterv1.GroupOperation_UpdateCursor{
			UpdateCursor: &clusterv1.UpdateCursorOp{Cursor: op.Cursor},
		}
	case raft.OpUpdateCommitted:
		wire.Operation = &clusterv1.GroupOperation_UpdateCommitted{
			UpdateCommitted: &clusterv1.UpdateCommittedOp{Committed: op.Committed},
		}
	case raft.OpAddPending:
		wire.Operation = &clusterv1.GroupOperation_AddPending{
			AddPending: &clusterv1.AddPendingOp{Entry: encodePendingEntry(op.PendingEntry)},
		}
	case raft.OpRemovePending:
		wire.Operation = &clusterv1.GroupOperation_RemovePending{
			RemovePending: &clusterv1.RemovePendingOp{ConsumerId: op.ConsumerID, Offset: op.Offset},
		}
	case raft.OpTransferPending:
		wire.Operation = &clusterv1.GroupOperation_TransferPending{
			TransferPending: &clusterv1.TransferPendingOp{
				Offset:       op.Offset,
				FromConsumer: op.FromConsumer,
				ToConsumer:   op.ToConsumer,
			},
		}
	case raft.OpRegisterConsumer:
		wire.Operation = &clusterv1.GroupOperation_RegisterConsumer{
			RegisterConsumer: &clusterv1.RegisterConsumerOp{Consumer: encodeConsumerInfo(op.ConsumerInfo)},
		}
	case raft.OpUnregisterConsumer:
		wire.Operation = &clusterv1.GroupOperation_UnregisterConsumer{
			UnregisterConsumer: &clusterv1.UnregisterConsumerOp{ConsumerId: op.ConsumerID},
		}
	case raft.OpRequeuePending:
		wire.Operation = &clusterv1.GroupOperation_RequeuePending{
			RequeuePending: &clusterv1.RequeuePendingOp{ConsumerId: op.ConsumerID, Offset: op.Offset},
		}
	default:
		return nil, fmt.Errorf("%w: type %d", ErrUnsupportedGroupOp, op.Type)
	}

	return wire, nil
}

// decodeGroupOperation converts a wire mutation back into its in-process form.
//
// Every field arrives from a peer, so a schema-valid message is not yet a
// usable one: a typed oneof stops a field from being misread, it does not stop
// a required payload from being absent. Each required value is checked here so
// the leader never applies a half-populated mutation. Applying one would
// dereference a nil group in the Raft manager.
func decodeGroupOperation(wire *clusterv1.GroupOperation) (*raft.Operation, error) {
	if wire == nil {
		return nil, fmt.Errorf("%w: operation is missing", ErrMalformedGroupOp)
	}

	op := &raft.Operation{
		QueueName: wire.QueueName,
		GroupID:   wire.GroupId,
	}
	if wire.Timestamp != nil {
		if err := wire.Timestamp.CheckValid(); err != nil {
			return nil, fmt.Errorf("%w: timestamp: %w", ErrMalformedGroupOp, err)
		}
		op.Timestamp = wire.Timestamp.AsTime()
	}

	switch payload := wire.Operation.(type) {
	case *clusterv1.GroupOperation_CreateGroup:
		op.Type = raft.OpCreateGroup
		group, err := decodeConsumerGroup(payload.CreateGroup.GetGroup())
		if err != nil {
			return nil, err
		}
		op.GroupState = group
	case *clusterv1.GroupOperation_UpdateGroup:
		op.Type = raft.OpUpdateGroup
		group, err := decodeConsumerGroup(payload.UpdateGroup.GetGroup())
		if err != nil {
			return nil, err
		}
		op.GroupState = group
	case *clusterv1.GroupOperation_DeleteGroup:
		op.Type = raft.OpDeleteGroup
	case *clusterv1.GroupOperation_UpdateCursor:
		op.Type = raft.OpUpdateCursor
		op.Cursor = payload.UpdateCursor.GetCursor()
	case *clusterv1.GroupOperation_UpdateCommitted:
		op.Type = raft.OpUpdateCommitted
		op.Committed = payload.UpdateCommitted.GetCommitted()
	case *clusterv1.GroupOperation_AddPending:
		op.Type = raft.OpAddPending
		entry, err := decodePendingEntry(payload.AddPending.GetEntry())
		if err != nil {
			return nil, err
		}
		op.PendingEntry = entry
	case *clusterv1.GroupOperation_RemovePending:
		op.Type = raft.OpRemovePending
		op.ConsumerID = payload.RemovePending.GetConsumerId()
		op.Offset = payload.RemovePending.GetOffset()
	case *clusterv1.GroupOperation_TransferPending:
		op.Type = raft.OpTransferPending
		op.Offset = payload.TransferPending.GetOffset()
		op.FromConsumer = payload.TransferPending.GetFromConsumer()
		op.ToConsumer = payload.TransferPending.GetToConsumer()
	case *clusterv1.GroupOperation_RegisterConsumer:
		op.Type = raft.OpRegisterConsumer
		consumer, err := decodeConsumerInfo(payload.RegisterConsumer.GetConsumer())
		if err != nil {
			return nil, err
		}
		op.ConsumerInfo = consumer
	case *clusterv1.GroupOperation_UnregisterConsumer:
		op.Type = raft.OpUnregisterConsumer
		op.ConsumerID = payload.UnregisterConsumer.GetConsumerId()
	case *clusterv1.GroupOperation_RequeuePending:
		op.Type = raft.OpRequeuePending
		op.ConsumerID = payload.RequeuePending.GetConsumerId()
		op.Offset = payload.RequeuePending.GetOffset()
	default:
		if wire.Operation == nil {
			return nil, fmt.Errorf("%w: operation payload is required", ErrMalformedGroupOp)
		}
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedGroupOp, wire.Operation)
	}

	if err := validateGroupOperation(op); err != nil {
		return nil, err
	}
	return op, nil
}

// validateGroupOperation enforces the semantic contract shared by both sides
// of the cluster boundary. The encoder catches local programming errors before
// they cross the network; the decoder repeats the check because peer input is
// never trusted.
func validateGroupOperation(op *raft.Operation) error {
	if op == nil {
		return fmt.Errorf("%w: operation is required", ErrMalformedGroupOp)
	}

	switch op.Type {
	case raft.OpCreateGroup, raft.OpUpdateGroup, raft.OpDeleteGroup,
		raft.OpUpdateCursor, raft.OpUpdateCommitted, raft.OpAddPending,
		raft.OpRemovePending, raft.OpTransferPending, raft.OpRegisterConsumer,
		raft.OpUnregisterConsumer, raft.OpRequeuePending:
	default:
		return fmt.Errorf("%w: type %d", ErrUnsupportedGroupOp, op.Type)
	}

	if op.QueueName == "" {
		return fmt.Errorf("%w: queue name is required", ErrMalformedGroupOp)
	}
	if op.GroupID == "" {
		return fmt.Errorf("%w: group id is required", ErrMalformedGroupOp)
	}
	if err := validateGroupTimestamp("timestamp", op.Timestamp); err != nil {
		return err
	}

	switch op.Type {
	case raft.OpCreateGroup, raft.OpUpdateGroup:
		return validateConsumerGroup(op.GroupState, op.QueueName, op.GroupID)
	case raft.OpAddPending:
		return validatePendingEntry(op.PendingEntry)
	case raft.OpRemovePending:
		if op.ConsumerID == "" {
			return fmt.Errorf("%w: remove pending requires a consumer id", ErrMalformedGroupOp)
		}
	case raft.OpTransferPending:
		if op.FromConsumer == "" || op.ToConsumer == "" {
			return fmt.Errorf("%w: transfer pending requires both consumers", ErrMalformedGroupOp)
		}
	case raft.OpRegisterConsumer:
		return validateConsumerInfo(op.ConsumerInfo)
	case raft.OpUnregisterConsumer:
		if op.ConsumerID == "" {
			return fmt.Errorf("%w: unregister consumer requires a consumer id", ErrMalformedGroupOp)
		}
	case raft.OpRequeuePending:
		if op.ConsumerID == "" {
			return fmt.Errorf("%w: requeue pending requires a consumer id", ErrMalformedGroupOp)
		}
	}

	return nil
}

func validateConsumerGroup(group *types.ConsumerGroup, queueName, groupID string) error {
	if group == nil {
		return fmt.Errorf("%w: group state is required", ErrMalformedGroupOp)
	}

	snapshot := group.Snapshot()
	if snapshot.ID == "" || snapshot.QueueName == "" {
		return fmt.Errorf("%w: group state requires an id and a queue name", ErrMalformedGroupOp)
	}
	if snapshot.ID != groupID || snapshot.QueueName != queueName {
		return fmt.Errorf("%w: group state names %q/%q, operation names %q/%q",
			ErrMalformedGroupOp, snapshot.QueueName, snapshot.ID, queueName, groupID)
	}
	if err := validateGroupTimestamp("group created_at", snapshot.CreatedAt); err != nil {
		return err
	}
	if err := validateGroupTimestamp("group updated_at", snapshot.UpdatedAt); err != nil {
		return err
	}
	for consumerID, entries := range snapshot.PEL {
		for _, entry := range entries {
			if err := validatePendingEntry(entry); err != nil {
				return err
			}
			if entry.ConsumerID != consumerID {
				return fmt.Errorf("%w: pending entry consumer %q is stored under %q",
					ErrMalformedGroupOp, entry.ConsumerID, consumerID)
			}
		}
	}
	for consumerID, consumer := range snapshot.Consumers {
		if err := validateConsumerInfo(consumer); err != nil {
			return err
		}
		if consumer.ID != consumerID {
			return fmt.Errorf("%w: consumer %q is stored under %q",
				ErrMalformedGroupOp, consumer.ID, consumerID)
		}
	}

	return nil
}

func validatePendingEntry(entry *types.PendingEntry) error {
	if entry == nil {
		return fmt.Errorf("%w: add pending requires an entry", ErrMalformedGroupOp)
	}
	if entry.ConsumerID == "" {
		return fmt.Errorf("%w: pending entry requires a consumer id", ErrMalformedGroupOp)
	}
	return validateGroupTimestamp("pending entry claimed_at", entry.ClaimedAt)
}

func validateConsumerInfo(consumer *types.ConsumerInfo) error {
	if consumer == nil || consumer.ID == "" {
		return fmt.Errorf("%w: register consumer requires a consumer with an id", ErrMalformedGroupOp)
	}
	if err := validateGroupTimestamp("consumer registered_at", consumer.RegisteredAt); err != nil {
		return err
	}
	return validateGroupTimestamp("consumer last_heartbeat", consumer.LastHeartbeat)
}

func validateGroupTimestamp(field string, value time.Time) error {
	if value.IsZero() {
		return nil
	}
	if err := timestamppb.New(value).CheckValid(); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrMalformedGroupOp, field, err)
	}
	return nil
}

func encodeConsumerGroup(group *types.ConsumerGroup) *clusterv1.ConsumerGroupState {
	if group == nil {
		return nil
	}

	// Snapshot under the group's own lock: PEL and Consumers are mutable maps.
	snapshot := group.Snapshot()

	state := &clusterv1.ConsumerGroupState{
		Id:         snapshot.ID,
		QueueName:  snapshot.QueueName,
		Pattern:    snapshot.Pattern,
		Mode:       string(snapshot.Mode),
		AutoCommit: snapshot.AutoCommit,
		Cursor: &clusterv1.QueueCursorState{
			Cursor:    snapshot.Cursor.Cursor,
			Committed: snapshot.Cursor.Committed,
		},
	}
	if !snapshot.CreatedAt.IsZero() {
		state.CreatedAt = timestamppb.New(snapshot.CreatedAt)
	}
	if !snapshot.UpdatedAt.IsZero() {
		state.UpdatedAt = timestamppb.New(snapshot.UpdatedAt)
	}
	for _, entries := range snapshot.PEL {
		for _, entry := range entries {
			state.Pending = append(state.Pending, encodePendingEntry(entry))
		}
	}
	for _, consumer := range snapshot.Consumers {
		state.Consumers = append(state.Consumers, encodeConsumerInfo(consumer))
	}
	return state
}

// decodeConsumerGroup rebuilds a group's replicated state. The shared operation
// validator checks its identity against the enclosing operation afterwards.
func decodeConsumerGroup(state *clusterv1.ConsumerGroupState) (*types.ConsumerGroup, error) {
	if state == nil {
		return nil, nil
	}

	group := types.NewConsumerGroupState(state.QueueName, state.Id, state.Pattern)
	group.Mode = types.ConsumerGroupMode(state.Mode)
	group.AutoCommit = state.AutoCommit
	if cursor := state.GetCursor(); cursor != nil {
		group.Cursor = &types.QueueCursor{Cursor: cursor.Cursor, Committed: cursor.Committed}
	}
	var createdAt, updatedAt time.Time
	if state.CreatedAt != nil {
		if err := state.CreatedAt.CheckValid(); err != nil {
			return nil, fmt.Errorf("%w: group created_at: %w", ErrMalformedGroupOp, err)
		}
		createdAt = state.CreatedAt.AsTime()
	}
	if state.UpdatedAt != nil {
		if err := state.UpdatedAt.CheckValid(); err != nil {
			return nil, fmt.Errorf("%w: group updated_at: %w", ErrMalformedGroupOp, err)
		}
		updatedAt = state.UpdatedAt.AsTime()
	}

	// The wire form flattens the pending list; each entry names its consumer,
	// so the per-consumer grouping is rebuilt here.
	pel := make(map[string][]*types.PendingEntry)
	for _, entry := range state.Pending {
		decoded, err := decodePendingEntry(entry)
		if err != nil {
			return nil, err
		}
		if decoded == nil {
			return nil, fmt.Errorf("%w: group state carries an empty pending entry", ErrMalformedGroupOp)
		}
		pel[decoded.ConsumerID] = append(pel[decoded.ConsumerID], decoded)
	}
	group.ReplacePEL(pel)

	consumers := make(map[string]*types.ConsumerInfo, len(state.Consumers))
	for _, consumer := range state.Consumers {
		decoded, err := decodeConsumerInfo(consumer)
		if err != nil {
			return nil, err
		}
		if decoded == nil || decoded.ID == "" {
			return nil, fmt.Errorf("%w: group state carries a consumer without an id", ErrMalformedGroupOp)
		}
		consumers[decoded.ID] = decoded
	}
	group.ReplaceConsumers(consumers)
	if !createdAt.IsZero() {
		group.CreatedAt = createdAt
	}
	if !updatedAt.IsZero() {
		group.UpdatedAt = updatedAt
	}

	return group, nil
}

func encodePendingEntry(entry *types.PendingEntry) *clusterv1.PendingEntryState {
	if entry == nil {
		return nil
	}
	state := &clusterv1.PendingEntryState{
		Offset:        entry.Offset,
		ConsumerId:    entry.ConsumerID,
		DeliveryCount: uint32(entry.DeliveryCount),
	}
	if !entry.ClaimedAt.IsZero() {
		state.ClaimedAt = timestamppb.New(entry.ClaimedAt)
	}
	return state
}

func decodePendingEntry(state *clusterv1.PendingEntryState) (*types.PendingEntry, error) {
	if state == nil {
		return nil, nil
	}
	// A pending entry names the consumer holding it. Without that the entry
	// cannot be filed under an owner, and work stealing would never find it.
	if state.ConsumerId == "" {
		return nil, fmt.Errorf("%w: pending entry requires a consumer id", ErrMalformedGroupOp)
	}
	entry := &types.PendingEntry{
		Offset:        state.Offset,
		ConsumerID:    state.ConsumerId,
		DeliveryCount: int(state.DeliveryCount),
	}
	if state.ClaimedAt != nil {
		if err := state.ClaimedAt.CheckValid(); err != nil {
			return nil, fmt.Errorf("%w: pending entry claimed_at: %w", ErrMalformedGroupOp, err)
		}
		entry.ClaimedAt = state.ClaimedAt.AsTime()
	}
	return entry, nil
}

func encodeConsumerInfo(consumer *types.ConsumerInfo) *clusterv1.ConsumerState {
	if consumer == nil {
		return nil
	}
	state := &clusterv1.ConsumerState{
		Id:          consumer.ID,
		ClientId:    consumer.ClientID,
		ProxyNodeId: consumer.ProxyNodeID,
	}
	if !consumer.RegisteredAt.IsZero() {
		state.RegisteredAt = timestamppb.New(consumer.RegisteredAt)
	}
	if !consumer.LastHeartbeat.IsZero() {
		state.LastHeartbeat = timestamppb.New(consumer.LastHeartbeat)
	}
	return state
}

func decodeConsumerInfo(state *clusterv1.ConsumerState) (*types.ConsumerInfo, error) {
	if state == nil {
		return nil, nil
	}
	consumer := &types.ConsumerInfo{
		ID:          state.Id,
		ClientID:    state.ClientId,
		ProxyNodeID: state.ProxyNodeId,
	}
	if state.RegisteredAt != nil {
		if err := state.RegisteredAt.CheckValid(); err != nil {
			return nil, fmt.Errorf("%w: consumer registered_at: %w", ErrMalformedGroupOp, err)
		}
		consumer.RegisteredAt = state.RegisteredAt.AsTime()
	}
	if state.LastHeartbeat != nil {
		if err := state.LastHeartbeat.CheckValid(); err != nil {
			return nil, fmt.Errorf("%w: consumer last_heartbeat: %w", ErrMalformedGroupOp, err)
		}
		consumer.LastHeartbeat = state.LastHeartbeat.AsTime()
	}
	return consumer, nil
}
