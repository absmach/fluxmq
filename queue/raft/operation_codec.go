// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"errors"
	"fmt"
	"slices"
	"time"

	raftv1 "github.com/absmach/fluxmq/pkg/proto/raft/v1"
	"github.com/absmach/fluxmq/queue/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// operationWireVersion gates the shape of the encoded operation.
//
// A field this binary does not know is a field it cannot apply. Tolerating it
// would not make the entry harmless: the domain conversion below reads named
// fields only, so an unknown one is dropped, this replica applies the zero
// value where a newer one applied the real thing, and the difference vanishes
// from the next snapshot this node writes. Refusing to decode stops the node
// instead, which is the loud half of the same choice Apply makes.
//
// That makes a rolling upgrade a matter of not emitting a field until every
// peer understands it — a version bump the writer gates on — rather than of
// hoping older peers ignore it safely.
const operationWireVersion uint32 = 1

var errMalformedOperation = errors.New("malformed queue raft operation")

func marshalOperation(op *Operation) ([]byte, error) {
	wire, err := encodeOperation(op)
	if err != nil {
		return nil, err
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal queue raft operation: %w", err)
	}
	return data, nil
}

func unmarshalOperation(data []byte) (*Operation, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty payload", errMalformedOperation)
	}

	wire := new(raftv1.Operation)
	if err := proto.Unmarshal(data, wire); err != nil {
		return nil, fmt.Errorf("%w: decode protobuf: %w", errMalformedOperation, err)
	}
	if err := rejectUnknownFields(wire.ProtoReflect()); err != nil {
		return nil, fmt.Errorf("%w: %w", errMalformedOperation, err)
	}
	return decodeOperation(wire)
}

func encodeOperation(op *Operation) (*raftv1.Operation, error) {
	if op == nil {
		return nil, fmt.Errorf("%w: operation is missing", errMalformedOperation)
	}

	wire := &raftv1.Operation{Version: operationWireVersion}
	if !op.Timestamp.IsZero() {
		wire.Timestamp = timestamppb.New(op.Timestamp)
	}

	switch op.Type {
	case OpAppend:
		wire.Command = &raftv1.Operation_Append{Append: &raftv1.AppendOperation{
			QueueName: op.QueueName,
			Envelope:  op.Message,
			DedupeKey: op.DedupeKey,
		}}
	case OpTruncate:
		wire.Command = &raftv1.Operation_Truncate{Truncate: &raftv1.TruncateOperation{
			QueueName: op.QueueName,
			MinOffset: op.MinOffset,
		}}
	case OpCreateGroup:
		group, err := encodeOperationGroup(op.GroupState)
		if err != nil {
			return nil, fmt.Errorf("%w: create group: %w", errMalformedOperation, err)
		}
		wire.Command = &raftv1.Operation_CreateGroup{CreateGroup: &raftv1.CreateGroupOperation{
			QueueName: op.QueueName,
			GroupId:   op.GroupID,
			Group:     group,
		}}
	case OpUpdateGroup:
		group, err := encodeOperationGroup(op.GroupState)
		if err != nil {
			return nil, fmt.Errorf("%w: update group: %w", errMalformedOperation, err)
		}
		wire.Command = &raftv1.Operation_UpdateGroup{UpdateGroup: &raftv1.UpdateGroupOperation{
			QueueName: op.QueueName,
			GroupId:   op.GroupID,
			Group:     group,
		}}
	case OpDeleteGroup:
		wire.Command = &raftv1.Operation_DeleteGroup{DeleteGroup: &raftv1.DeleteGroupOperation{
			QueueName: op.QueueName,
			GroupId:   op.GroupID,
		}}
	case OpUpdateCursor:
		wire.Command = &raftv1.Operation_UpdateCursor{UpdateCursor: &raftv1.UpdateCursorOperation{
			QueueName: op.QueueName,
			GroupId:   op.GroupID,
			Cursor:    op.Cursor,
		}}
	case OpUpdateCommitted:
		wire.Command = &raftv1.Operation_UpdateCommitted{UpdateCommitted: &raftv1.UpdateCommittedOperation{
			QueueName: op.QueueName,
			GroupId:   op.GroupID,
			Committed: op.Committed,
		}}
	case OpAddPending:
		if op.PendingEntry == nil {
			return nil, fmt.Errorf("%w: add pending entry is missing", errMalformedOperation)
		}
		wire.Command = &raftv1.Operation_AddPending{AddPending: &raftv1.AddPendingOperation{
			QueueName: op.QueueName,
			GroupId:   op.GroupID,
			Entry:     encodeOperationPending(op.PendingEntry),
		}}
	case OpRemovePending:
		wire.Command = &raftv1.Operation_RemovePending{RemovePending: &raftv1.RemovePendingOperation{
			QueueName:  op.QueueName,
			GroupId:    op.GroupID,
			ConsumerId: op.ConsumerID,
			Offset:     op.Offset,
		}}
	case OpTransferPending:
		wire.Command = &raftv1.Operation_TransferPending{TransferPending: &raftv1.TransferPendingOperation{
			QueueName:    op.QueueName,
			GroupId:      op.GroupID,
			Offset:       op.Offset,
			FromConsumer: op.FromConsumer,
			ToConsumer:   op.ToConsumer,
		}}
	case OpRegisterConsumer:
		if op.ConsumerInfo == nil {
			return nil, fmt.Errorf("%w: register consumer is missing", errMalformedOperation)
		}
		wire.Command = &raftv1.Operation_RegisterConsumer{RegisterConsumer: &raftv1.RegisterConsumerOperation{
			QueueName: op.QueueName,
			GroupId:   op.GroupID,
			Consumer:  encodeOperationConsumer(op.ConsumerInfo),
		}}
	case OpUnregisterConsumer:
		wire.Command = &raftv1.Operation_UnregisterConsumer{UnregisterConsumer: &raftv1.UnregisterConsumerOperation{
			QueueName:  op.QueueName,
			GroupId:    op.GroupID,
			ConsumerId: op.ConsumerID,
		}}
	case OpCreateQueue:
		if op.QueueConfig == nil {
			return nil, fmt.Errorf("%w: create queue config is missing", errMalformedOperation)
		}
		cfg, err := encodeOperationQueueConfig(op.QueueConfig)
		if err != nil {
			return nil, fmt.Errorf("%w: create queue: %w", errMalformedOperation, err)
		}
		wire.Command = &raftv1.Operation_CreateQueue{CreateQueue: &raftv1.CreateQueueOperation{
			QueueName: op.QueueName,
			Config:    cfg,
		}}
	case OpUpdateQueue:
		if op.QueueConfig == nil {
			return nil, fmt.Errorf("%w: update queue config is missing", errMalformedOperation)
		}
		cfg, err := encodeOperationQueueConfig(op.QueueConfig)
		if err != nil {
			return nil, fmt.Errorf("%w: update queue: %w", errMalformedOperation, err)
		}
		wire.Command = &raftv1.Operation_UpdateQueue{UpdateQueue: &raftv1.UpdateQueueOperation{
			QueueName: op.QueueName,
			Config:    cfg,
		}}
	case OpDeleteQueue:
		wire.Command = &raftv1.Operation_DeleteQueue{DeleteQueue: &raftv1.DeleteQueueOperation{QueueName: op.QueueName}}
	default:
		return nil, fmt.Errorf("%w: unknown operation type %d", errMalformedOperation, op.Type)
	}

	return wire, nil
}

func decodeOperation(wire *raftv1.Operation) (*Operation, error) {
	if wire == nil {
		return nil, fmt.Errorf("%w: operation is missing", errMalformedOperation)
	}
	if wire.Version != operationWireVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", errMalformedOperation, wire.Version)
	}

	op := new(Operation)
	if wire.Timestamp != nil {
		if err := wire.Timestamp.CheckValid(); err != nil {
			return nil, fmt.Errorf("%w: timestamp: %w", errMalformedOperation, err)
		}
		op.Timestamp = wire.Timestamp.AsTime()
	}

	switch payload := wire.Command.(type) {
	case *raftv1.Operation_Append:
		if payload.Append == nil {
			return nil, missingCommand("append")
		}
		op.Type = OpAppend
		op.QueueName = payload.Append.QueueName
		op.Message = payload.Append.Envelope
		op.DedupeKey = payload.Append.DedupeKey
	case *raftv1.Operation_Truncate:
		if payload.Truncate == nil {
			return nil, missingCommand("truncate")
		}
		op.Type = OpTruncate
		op.QueueName = payload.Truncate.QueueName
		op.MinOffset = payload.Truncate.MinOffset
	case *raftv1.Operation_CreateGroup:
		if payload.CreateGroup == nil || payload.CreateGroup.Group == nil {
			return nil, missingCommand("create group")
		}
		group, err := decodeOperationGroup(payload.CreateGroup.Group)
		if err != nil {
			return nil, fmt.Errorf("%w: create group: %w", errMalformedOperation, err)
		}
		op.Type, op.QueueName, op.GroupID, op.GroupState = OpCreateGroup, payload.CreateGroup.QueueName, payload.CreateGroup.GroupId, group
	case *raftv1.Operation_UpdateGroup:
		if payload.UpdateGroup == nil || payload.UpdateGroup.Group == nil {
			return nil, missingCommand("update group")
		}
		group, err := decodeOperationGroup(payload.UpdateGroup.Group)
		if err != nil {
			return nil, fmt.Errorf("%w: update group: %w", errMalformedOperation, err)
		}
		op.Type, op.QueueName, op.GroupID, op.GroupState = OpUpdateGroup, payload.UpdateGroup.QueueName, payload.UpdateGroup.GroupId, group
	case *raftv1.Operation_DeleteGroup:
		if payload.DeleteGroup == nil {
			return nil, missingCommand("delete group")
		}
		op.Type, op.QueueName, op.GroupID = OpDeleteGroup, payload.DeleteGroup.QueueName, payload.DeleteGroup.GroupId
	case *raftv1.Operation_UpdateCursor:
		if payload.UpdateCursor == nil {
			return nil, missingCommand("update cursor")
		}
		op.Type, op.QueueName, op.GroupID, op.Cursor = OpUpdateCursor, payload.UpdateCursor.QueueName, payload.UpdateCursor.GroupId, payload.UpdateCursor.Cursor
	case *raftv1.Operation_UpdateCommitted:
		if payload.UpdateCommitted == nil {
			return nil, missingCommand("update committed")
		}
		op.Type, op.QueueName, op.GroupID, op.Committed = OpUpdateCommitted, payload.UpdateCommitted.QueueName, payload.UpdateCommitted.GroupId, payload.UpdateCommitted.Committed
	case *raftv1.Operation_AddPending:
		if payload.AddPending == nil || payload.AddPending.Entry == nil {
			return nil, missingCommand("add pending")
		}
		entry, err := decodeOperationPending(payload.AddPending.Entry)
		if err != nil {
			return nil, fmt.Errorf("%w: add pending: %w", errMalformedOperation, err)
		}
		op.Type, op.QueueName, op.GroupID, op.PendingEntry = OpAddPending, payload.AddPending.QueueName, payload.AddPending.GroupId, entry
	case *raftv1.Operation_RemovePending:
		if payload.RemovePending == nil {
			return nil, missingCommand("remove pending")
		}
		op.Type, op.QueueName, op.GroupID = OpRemovePending, payload.RemovePending.QueueName, payload.RemovePending.GroupId
		op.ConsumerID, op.Offset = payload.RemovePending.ConsumerId, payload.RemovePending.Offset
	case *raftv1.Operation_TransferPending:
		if payload.TransferPending == nil {
			return nil, missingCommand("transfer pending")
		}
		op.Type, op.QueueName, op.GroupID = OpTransferPending, payload.TransferPending.QueueName, payload.TransferPending.GroupId
		op.Offset, op.FromConsumer, op.ToConsumer = payload.TransferPending.Offset, payload.TransferPending.FromConsumer, payload.TransferPending.ToConsumer
	case *raftv1.Operation_RegisterConsumer:
		if payload.RegisterConsumer == nil || payload.RegisterConsumer.Consumer == nil {
			return nil, missingCommand("register consumer")
		}
		consumer, err := decodeOperationConsumer(payload.RegisterConsumer.Consumer)
		if err != nil {
			return nil, fmt.Errorf("%w: register consumer: %w", errMalformedOperation, err)
		}
		op.Type, op.QueueName, op.GroupID, op.ConsumerInfo = OpRegisterConsumer, payload.RegisterConsumer.QueueName, payload.RegisterConsumer.GroupId, consumer
	case *raftv1.Operation_UnregisterConsumer:
		if payload.UnregisterConsumer == nil {
			return nil, missingCommand("unregister consumer")
		}
		op.Type, op.QueueName, op.GroupID = OpUnregisterConsumer, payload.UnregisterConsumer.QueueName, payload.UnregisterConsumer.GroupId
		op.ConsumerID = payload.UnregisterConsumer.ConsumerId
	case *raftv1.Operation_CreateQueue:
		if payload.CreateQueue == nil || payload.CreateQueue.Config == nil {
			return nil, missingCommand("create queue")
		}
		cfg, err := decodeOperationQueueConfig(payload.CreateQueue.Config)
		if err != nil {
			return nil, fmt.Errorf("%w: create queue: %w", errMalformedOperation, err)
		}
		op.Type, op.QueueName, op.QueueConfig = OpCreateQueue, payload.CreateQueue.QueueName, cfg
	case *raftv1.Operation_UpdateQueue:
		if payload.UpdateQueue == nil || payload.UpdateQueue.Config == nil {
			return nil, missingCommand("update queue")
		}
		cfg, err := decodeOperationQueueConfig(payload.UpdateQueue.Config)
		if err != nil {
			return nil, fmt.Errorf("%w: update queue: %w", errMalformedOperation, err)
		}
		op.Type, op.QueueName, op.QueueConfig = OpUpdateQueue, payload.UpdateQueue.QueueName, cfg
	case *raftv1.Operation_DeleteQueue:
		if payload.DeleteQueue == nil {
			return nil, missingCommand("delete queue")
		}
		op.Type, op.QueueName = OpDeleteQueue, payload.DeleteQueue.QueueName
	case nil:
		return nil, fmt.Errorf("%w: command is missing", errMalformedOperation)
	default:
		return nil, fmt.Errorf("%w: unsupported command %T", errMalformedOperation, payload)
	}

	return op, nil
}

func missingCommand(name string) error {
	return fmt.Errorf("%w: %s payload is missing", errMalformedOperation, name)
}

func encodeOperationGroup(group *types.ConsumerGroup) (*raftv1.ConsumerGroupState, error) {
	if group == nil {
		return nil, errors.New("group state is missing")
	}
	return encodeOperationGroupSnapshot(group.Snapshot())
}

// consumerGroupFromSnapshot rebuilds a detached group from a point-in-time
// copy. A snapshot has to stop referring to live group state the moment it is
// captured: the FSM keeps applying entries while the snapshot is serialized,
// and a group read through a live pointer would pick up mutations from log
// entries after the index the snapshot claims to describe.
func consumerGroupFromSnapshot(snapshot types.ConsumerGroupSnapshot) *types.ConsumerGroup {
	cursor := snapshot.Cursor
	group := &types.ConsumerGroup{
		ID:         snapshot.ID,
		QueueName:  snapshot.QueueName,
		Pattern:    snapshot.Pattern,
		Mode:       snapshot.Mode,
		AutoCommit: snapshot.AutoCommit,
		Cursor:     &cursor,
		PEL:        make(map[string][]*types.PendingEntry, len(snapshot.PEL)),
		Consumers:  make(map[string]*types.ConsumerInfo, len(snapshot.Consumers)),
		CreatedAt:  snapshot.CreatedAt,
		UpdatedAt:  snapshot.UpdatedAt,
	}
	for consumerID, entries := range snapshot.PEL {
		group.PEL[consumerID] = entries
	}
	for consumerID, consumer := range snapshot.Consumers {
		group.Consumers[consumerID] = consumer
	}
	return group
}

func encodeOperationGroupSnapshot(snapshot types.ConsumerGroupSnapshot) (*raftv1.ConsumerGroupState, error) {
	mode, err := encodeOperationGroupMode(snapshot.Mode)
	if err != nil {
		return nil, err
	}

	wire := &raftv1.ConsumerGroupState{
		Id:         snapshot.ID,
		QueueName:  snapshot.QueueName,
		Pattern:    snapshot.Pattern,
		Mode:       mode,
		AutoCommit: snapshot.AutoCommit,
		Cursor:     &raftv1.QueueCursorState{Cursor: snapshot.Cursor.Cursor, Committed: snapshot.Cursor.Committed},
		CreatedAt:  encodeOperationTime(snapshot.CreatedAt),
		UpdatedAt:  encodeOperationTime(snapshot.UpdatedAt),
	}

	consumerIDs := make([]string, 0, len(snapshot.PEL))
	for consumerID := range snapshot.PEL {
		consumerIDs = append(consumerIDs, consumerID)
	}
	slices.Sort(consumerIDs)
	for _, consumerID := range consumerIDs {
		for _, entry := range snapshot.PEL[consumerID] {
			if entry == nil {
				continue
			}
			wire.Pending = append(wire.Pending, encodeOperationPending(entry))
		}
	}

	consumerIDs = consumerIDs[:0]
	for consumerID := range snapshot.Consumers {
		consumerIDs = append(consumerIDs, consumerID)
	}
	slices.Sort(consumerIDs)
	for _, consumerID := range consumerIDs {
		if consumer := snapshot.Consumers[consumerID]; consumer != nil {
			wire.Consumers = append(wire.Consumers, encodeOperationConsumer(consumer))
		}
	}
	return wire, nil
}

func decodeOperationGroup(wire *raftv1.ConsumerGroupState) (*types.ConsumerGroup, error) {
	if wire == nil {
		return nil, errors.New("group state is missing")
	}
	mode, err := decodeOperationGroupMode(wire.Mode)
	if err != nil {
		return nil, err
	}
	if wire.Cursor == nil {
		return nil, errors.New("cursor is missing")
	}
	createdAt, err := decodeOperationTime(wire.CreatedAt, "created_at")
	if err != nil {
		return nil, err
	}
	updatedAt, err := decodeOperationTime(wire.UpdatedAt, "updated_at")
	if err != nil {
		return nil, err
	}

	pel := make(map[string][]*types.PendingEntry)
	for _, pending := range wire.Pending {
		if pending == nil {
			return nil, errors.New("pending entry is missing")
		}
		entry, decodeErr := decodeOperationPending(pending)
		if decodeErr != nil {
			return nil, decodeErr
		}
		pel[entry.ConsumerID] = append(pel[entry.ConsumerID], entry)
	}
	consumers := make(map[string]*types.ConsumerInfo, len(wire.Consumers))
	for _, item := range wire.Consumers {
		if item == nil {
			return nil, errors.New("consumer is missing")
		}
		consumer, decodeErr := decodeOperationConsumer(item)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if _, exists := consumers[consumer.ID]; exists {
			return nil, fmt.Errorf("duplicate consumer %q", consumer.ID)
		}
		consumers[consumer.ID] = consumer
	}

	return &types.ConsumerGroup{
		ID:         wire.Id,
		QueueName:  wire.QueueName,
		Pattern:    wire.Pattern,
		Mode:       mode,
		AutoCommit: wire.AutoCommit,
		Cursor:     &types.QueueCursor{Cursor: wire.Cursor.Cursor, Committed: wire.Cursor.Committed},
		PEL:        pel,
		Consumers:  consumers,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
	}, nil
}

func encodeOperationPending(entry *types.PendingEntry) *raftv1.PendingEntryState {
	return &raftv1.PendingEntryState{
		Offset:        entry.Offset,
		ConsumerId:    entry.ConsumerID,
		ClaimedAt:     encodeOperationTime(entry.ClaimedAt),
		DeliveryCount: int64(entry.DeliveryCount),
	}
}

func decodeOperationPending(wire *raftv1.PendingEntryState) (*types.PendingEntry, error) {
	claimedAt, err := decodeOperationTime(wire.ClaimedAt, "claimed_at")
	if err != nil {
		return nil, err
	}
	deliveryCount, err := operationInt(wire.DeliveryCount, "delivery_count")
	if err != nil {
		return nil, err
	}
	return &types.PendingEntry{
		Offset:        wire.Offset,
		ConsumerID:    wire.ConsumerId,
		ClaimedAt:     claimedAt,
		DeliveryCount: deliveryCount,
	}, nil
}

func encodeOperationConsumer(consumer *types.ConsumerInfo) *raftv1.ConsumerState {
	return &raftv1.ConsumerState{
		Id:            consumer.ID,
		ClientId:      consumer.ClientID,
		ProxyNodeId:   consumer.ProxyNodeID,
		RegisteredAt:  encodeOperationTime(consumer.RegisteredAt),
		LastHeartbeat: encodeOperationTime(consumer.LastHeartbeat),
	}
}

func decodeOperationConsumer(wire *raftv1.ConsumerState) (*types.ConsumerInfo, error) {
	registeredAt, err := decodeOperationTime(wire.RegisteredAt, "registered_at")
	if err != nil {
		return nil, err
	}
	lastHeartbeat, err := decodeOperationTime(wire.LastHeartbeat, "last_heartbeat")
	if err != nil {
		return nil, err
	}
	return &types.ConsumerInfo{
		ID:            wire.Id,
		ClientID:      wire.ClientId,
		ProxyNodeID:   wire.ProxyNodeId,
		RegisteredAt:  registeredAt,
		LastHeartbeat: lastHeartbeat,
	}, nil
}

func encodeOperationQueueConfig(cfg *types.QueueConfig) (*raftv1.QueueConfigState, error) {
	queueType, err := encodeOperationQueueType(cfg.Type)
	if err != nil {
		return nil, err
	}
	replicationMode, err := encodeOperationReplicationMode(cfg.Replication.Mode)
	if err != nil {
		return nil, err
	}
	return &raftv1.QueueConfigState{
		Name:                   cfg.Name,
		Topics:                 append([]string(nil), cfg.Topics...),
		Reserved:               cfg.Reserved,
		Type:                   queueType,
		PrimaryGroup:           cfg.PrimaryGroup,
		Durable:                cfg.Durable,
		AckDurability:          cfg.AckDurability,
		ExpiresAfter:           durationpb.New(cfg.ExpiresAfter),
		LastConsumerDisconnect: encodeOperationTime(cfg.LastConsumerDisconnect),
		RetryPolicy:            &raftv1.RetryPolicyState{MaxRetries: int64(cfg.RetryPolicy.MaxRetries), InitialBackoff: durationpb.New(cfg.RetryPolicy.InitialBackoff), MaxBackoff: durationpb.New(cfg.RetryPolicy.MaxBackoff), BackoffMultiplier: cfg.RetryPolicy.BackoffMultiplier, TotalTimeout: durationpb.New(cfg.RetryPolicy.TotalTimeout)},
		DeadLetter:             &raftv1.DeadLetterState{Enabled: cfg.DLQConfig.Enabled, Topic: cfg.DLQConfig.Topic, AlertWebhook: cfg.DLQConfig.AlertWebhook},
		Replication:            &raftv1.ReplicationState{Enabled: cfg.Replication.Enabled, Group: cfg.Replication.Group, ReplicationFactor: int64(cfg.Replication.ReplicationFactor), Mode: replicationMode, MinInSyncReplicas: int64(cfg.Replication.MinInSyncReplicas), AckTimeout: durationpb.New(cfg.Replication.AckTimeout), HeartbeatTimeout: durationpb.New(cfg.Replication.HeartbeatTimeout), ElectionTimeout: durationpb.New(cfg.Replication.ElectionTimeout), SnapshotInterval: durationpb.New(cfg.Replication.SnapshotInterval), SnapshotThreshold: cfg.Replication.SnapshotThreshold},
		Retention:              &raftv1.RetentionState{RetentionTime: durationpb.New(cfg.Retention.RetentionTime), TimeCheckInterval: durationpb.New(cfg.Retention.TimeCheckInterval), RetentionBytes: cfg.Retention.RetentionBytes, RetentionMessages: cfg.Retention.RetentionMessages, SizeCheckEvery: int64(cfg.Retention.SizeCheckEvery), CompactionEnabled: cfg.Retention.CompactionEnabled, CompactionKey: cfg.Retention.CompactionKey, CompactionLag: durationpb.New(cfg.Retention.CompactionLag), CompactionInterval: durationpb.New(cfg.Retention.CompactionInterval)},
		MaxMessageSize:         cfg.MaxMessageSize,
		MaxDepth:               cfg.MaxDepth,
		MessageTtl:             durationpb.New(cfg.MessageTTL),
		DeliveryTimeout:        durationpb.New(cfg.DeliveryTimeout),
		BatchSize:              int64(cfg.BatchSize),
		HeartbeatTimeout:       durationpb.New(cfg.HeartbeatTimeout),
	}, nil
}

func decodeOperationQueueConfig(wire *raftv1.QueueConfigState) (*types.QueueConfig, error) {
	if wire.RetryPolicy == nil || wire.DeadLetter == nil || wire.Replication == nil || wire.Retention == nil {
		return nil, errors.New("queue config section is missing")
	}
	queueType, err := decodeOperationQueueType(wire.Type)
	if err != nil {
		return nil, err
	}
	replicationMode, err := decodeOperationReplicationMode(wire.Replication.Mode)
	if err != nil {
		return nil, err
	}

	expiresAfter, err := decodeOperationDuration(wire.ExpiresAfter, "expires_after")
	if err != nil {
		return nil, err
	}
	lastDisconnect, err := decodeOperationTime(wire.LastConsumerDisconnect, "last_consumer_disconnect")
	if err != nil {
		return nil, err
	}
	initialBackoff, err := decodeOperationDuration(wire.RetryPolicy.InitialBackoff, "initial_backoff")
	if err != nil {
		return nil, err
	}
	maxBackoff, err := decodeOperationDuration(wire.RetryPolicy.MaxBackoff, "max_backoff")
	if err != nil {
		return nil, err
	}
	totalTimeout, err := decodeOperationDuration(wire.RetryPolicy.TotalTimeout, "total_timeout")
	if err != nil {
		return nil, err
	}
	ackTimeout, err := decodeOperationDuration(wire.Replication.AckTimeout, "ack_timeout")
	if err != nil {
		return nil, err
	}
	replicationHeartbeat, err := decodeOperationDuration(wire.Replication.HeartbeatTimeout, "replication_heartbeat_timeout")
	if err != nil {
		return nil, err
	}
	electionTimeout, err := decodeOperationDuration(wire.Replication.ElectionTimeout, "election_timeout")
	if err != nil {
		return nil, err
	}
	snapshotInterval, err := decodeOperationDuration(wire.Replication.SnapshotInterval, "snapshot_interval")
	if err != nil {
		return nil, err
	}
	retentionTime, err := decodeOperationDuration(wire.Retention.RetentionTime, "retention_time")
	if err != nil {
		return nil, err
	}
	timeCheckInterval, err := decodeOperationDuration(wire.Retention.TimeCheckInterval, "time_check_interval")
	if err != nil {
		return nil, err
	}
	compactionLag, err := decodeOperationDuration(wire.Retention.CompactionLag, "compaction_lag")
	if err != nil {
		return nil, err
	}
	compactionInterval, err := decodeOperationDuration(wire.Retention.CompactionInterval, "compaction_interval")
	if err != nil {
		return nil, err
	}
	messageTTL, err := decodeOperationDuration(wire.MessageTtl, "message_ttl")
	if err != nil {
		return nil, err
	}
	deliveryTimeout, err := decodeOperationDuration(wire.DeliveryTimeout, "delivery_timeout")
	if err != nil {
		return nil, err
	}
	heartbeatTimeout, err := decodeOperationDuration(wire.HeartbeatTimeout, "heartbeat_timeout")
	if err != nil {
		return nil, err
	}

	maxRetries, err := operationInt(wire.RetryPolicy.MaxRetries, "max_retries")
	if err != nil {
		return nil, err
	}
	replicationFactor, err := operationInt(wire.Replication.ReplicationFactor, "replication_factor")
	if err != nil {
		return nil, err
	}
	minISR, err := operationInt(wire.Replication.MinInSyncReplicas, "min_in_sync_replicas")
	if err != nil {
		return nil, err
	}
	sizeCheckEvery, err := operationInt(wire.Retention.SizeCheckEvery, "size_check_every")
	if err != nil {
		return nil, err
	}
	batchSize, err := operationInt(wire.BatchSize, "batch_size")
	if err != nil {
		return nil, err
	}

	return &types.QueueConfig{
		Name: wire.Name, Topics: append([]string(nil), wire.Topics...), Reserved: wire.Reserved, Type: queueType,
		PrimaryGroup: wire.PrimaryGroup, Durable: wire.Durable, AckDurability: wire.AckDurability,
		ExpiresAfter: expiresAfter, LastConsumerDisconnect: lastDisconnect,
		RetryPolicy:    types.RetryPolicy{MaxRetries: maxRetries, InitialBackoff: initialBackoff, MaxBackoff: maxBackoff, BackoffMultiplier: wire.RetryPolicy.BackoffMultiplier, TotalTimeout: totalTimeout},
		DLQConfig:      types.DLQConfig{Enabled: wire.DeadLetter.Enabled, Topic: wire.DeadLetter.Topic, AlertWebhook: wire.DeadLetter.AlertWebhook},
		Replication:    types.ReplicationConfig{Enabled: wire.Replication.Enabled, Group: wire.Replication.Group, ReplicationFactor: replicationFactor, Mode: replicationMode, MinInSyncReplicas: minISR, AckTimeout: ackTimeout, HeartbeatTimeout: replicationHeartbeat, ElectionTimeout: electionTimeout, SnapshotInterval: snapshotInterval, SnapshotThreshold: wire.Replication.SnapshotThreshold},
		Retention:      types.RetentionPolicy{RetentionTime: retentionTime, TimeCheckInterval: timeCheckInterval, RetentionBytes: wire.Retention.RetentionBytes, RetentionMessages: wire.Retention.RetentionMessages, SizeCheckEvery: sizeCheckEvery, CompactionEnabled: wire.Retention.CompactionEnabled, CompactionKey: wire.Retention.CompactionKey, CompactionLag: compactionLag, CompactionInterval: compactionInterval},
		MaxMessageSize: wire.MaxMessageSize, MaxDepth: wire.MaxDepth, MessageTTL: messageTTL,
		DeliveryTimeout: deliveryTimeout, BatchSize: batchSize, HeartbeatTimeout: heartbeatTimeout,
	}, nil
}

func encodeOperationGroupMode(mode types.ConsumerGroupMode) (raftv1.ConsumerGroupMode, error) {
	switch mode {
	case "":
		return raftv1.ConsumerGroupMode_CONSUMER_GROUP_MODE_UNSPECIFIED, nil
	case types.GroupModeQueue:
		return raftv1.ConsumerGroupMode_CONSUMER_GROUP_MODE_QUEUE, nil
	case types.GroupModeStream:
		return raftv1.ConsumerGroupMode_CONSUMER_GROUP_MODE_STREAM, nil
	default:
		return 0, fmt.Errorf("unknown consumer group mode %q", mode)
	}
}

func decodeOperationGroupMode(mode raftv1.ConsumerGroupMode) (types.ConsumerGroupMode, error) {
	switch mode {
	case raftv1.ConsumerGroupMode_CONSUMER_GROUP_MODE_UNSPECIFIED:
		return "", nil
	case raftv1.ConsumerGroupMode_CONSUMER_GROUP_MODE_QUEUE:
		return types.GroupModeQueue, nil
	case raftv1.ConsumerGroupMode_CONSUMER_GROUP_MODE_STREAM:
		return types.GroupModeStream, nil
	default:
		return "", fmt.Errorf("unknown consumer group mode %d", mode)
	}
}

func encodeOperationQueueType(queueType types.QueueType) (raftv1.QueueType, error) {
	switch queueType {
	case "":
		return raftv1.QueueType_QUEUE_TYPE_UNSPECIFIED, nil
	case types.QueueTypeClassic:
		return raftv1.QueueType_QUEUE_TYPE_CLASSIC, nil
	case types.QueueTypeStream:
		return raftv1.QueueType_QUEUE_TYPE_STREAM, nil
	default:
		return 0, fmt.Errorf("unknown queue type %q", queueType)
	}
}

func decodeOperationQueueType(queueType raftv1.QueueType) (types.QueueType, error) {
	switch queueType {
	case raftv1.QueueType_QUEUE_TYPE_UNSPECIFIED:
		return "", nil
	case raftv1.QueueType_QUEUE_TYPE_CLASSIC:
		return types.QueueTypeClassic, nil
	case raftv1.QueueType_QUEUE_TYPE_STREAM:
		return types.QueueTypeStream, nil
	default:
		return "", fmt.Errorf("unknown queue type %d", queueType)
	}
}

func encodeOperationReplicationMode(mode types.ReplicationMode) (raftv1.ReplicationMode, error) {
	switch mode {
	case "":
		return raftv1.ReplicationMode_REPLICATION_MODE_UNSPECIFIED, nil
	case types.ReplicationSync:
		return raftv1.ReplicationMode_REPLICATION_MODE_SYNC, nil
	case types.ReplicationAsync:
		return raftv1.ReplicationMode_REPLICATION_MODE_ASYNC, nil
	default:
		return 0, fmt.Errorf("unknown replication mode %q", mode)
	}
}

func decodeOperationReplicationMode(mode raftv1.ReplicationMode) (types.ReplicationMode, error) {
	switch mode {
	case raftv1.ReplicationMode_REPLICATION_MODE_UNSPECIFIED:
		return "", nil
	case raftv1.ReplicationMode_REPLICATION_MODE_SYNC:
		return types.ReplicationSync, nil
	case raftv1.ReplicationMode_REPLICATION_MODE_ASYNC:
		return types.ReplicationAsync, nil
	default:
		return "", fmt.Errorf("unknown replication mode %d", mode)
	}
}

func encodeOperationTime(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value)
}

func decodeOperationTime(value *timestamppb.Timestamp, field string) (time.Time, error) {
	if value == nil {
		return time.Time{}, nil
	}
	if err := value.CheckValid(); err != nil {
		return time.Time{}, fmt.Errorf("%s: %w", field, err)
	}
	return value.AsTime(), nil
}

func decodeOperationDuration(value *durationpb.Duration, field string) (time.Duration, error) {
	if value == nil {
		return 0, nil
	}
	if err := value.CheckValid(); err != nil {
		return 0, fmt.Errorf("%s: %w", field, err)
	}
	duration := value.AsDuration()
	if roundTrip := durationpb.New(duration); roundTrip.Seconds != value.Seconds || roundTrip.Nanos != value.Nanos {
		return 0, fmt.Errorf("%s exceeds Go duration range", field)
	}
	return duration, nil
}

func operationInt(value int64, field string) (int, error) {
	converted := int(value)
	if int64(converted) != value {
		return 0, fmt.Errorf("%s exceeds int range", field)
	}
	return converted, nil
}

// rejectUnknownFields walks a decoded message and refuses any field this build
// has no name for, at any depth. The walk has to be recursive because the
// fields that carry replicated state — a queue config, a group's cursor — are
// nested, and an unknown one there is exactly the one that would be applied as
// a zero value.
func rejectUnknownFields(msg protoreflect.Message) error {
	if len(msg.GetUnknown()) != 0 {
		return fmt.Errorf("unknown fields in %s", msg.Descriptor().FullName())
	}
	var nestedErr error
	msg.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			if field.MapValue().Kind() != protoreflect.MessageKind {
				return true
			}
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				nestedErr = rejectUnknownFields(item.Message())
				return nestedErr == nil
			})
		case field.IsList():
			if field.Kind() != protoreflect.MessageKind {
				return true
			}
			list := value.List()
			for i := range list.Len() {
				if nestedErr = rejectUnknownFields(list.Get(i).Message()); nestedErr != nil {
					break
				}
			}
		case field.Kind() == protoreflect.MessageKind:
			nestedErr = rejectUnknownFields(value.Message())
		}
		return nestedErr == nil
	})
	return nestedErr
}
