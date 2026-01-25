// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	"github.com/absmach/fluxmq/pkg/proto/queue/v1/queuev1connect"
	"github.com/absmach/fluxmq/queue"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Handler implements the QueueServiceHandler interface.
type Handler struct {
	queuev1connect.UnimplementedQueueServiceHandler

	manager    *queue.Manager
	logStore   storage.LogStore
	groupStore storage.ConsumerGroupStore
	logger     *slog.Logger
}

// NewHandler creates a new queue service handler.
func NewHandler(manager *queue.Manager, logStore storage.LogStore, groupStore storage.ConsumerGroupStore, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		manager:    manager,
		logStore:   logStore,
		groupStore: groupStore,
		logger:     logger,
	}
}

// --- Queue Management ---

func (h *Handler) CreateQueue(ctx context.Context, req *connect.Request[queuev1.CreateQueueRequest]) (*connect.Response[queuev1.Queue], error) {
	msg := req.Msg

	config := types.QueueConfig{
		Name:       msg.Name,
		Partitions: int(msg.Partitions),
		MessageTTL: 7 * 24 * time.Hour,
	}

	if config.Partitions == 0 {
		config.Partitions = 10
	}

	if msg.Config != nil && msg.Config.Retention != nil && msg.Config.Retention.MaxAge != nil {
		config.MessageTTL = msg.Config.Retention.MaxAge.AsDuration()
	}

	if err := h.manager.CreateQueue(ctx, config); err != nil {
		return nil, connect.NewError(connect.CodeAlreadyExists, err)
	}

	return connect.NewResponse(h.queueToProto(&config)), nil
}

func (h *Handler) GetQueue(ctx context.Context, req *connect.Request[queuev1.GetQueueRequest]) (*connect.Response[queuev1.Queue], error) {
	config, err := h.logStore.GetQueue(ctx, req.Msg.Name)
	if err != nil {
		if err == storage.ErrQueueNotFound {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(h.queueToProto(config)), nil
}

func (h *Handler) ListQueues(ctx context.Context, req *connect.Request[queuev1.ListQueuesRequest]) (*connect.Response[queuev1.ListQueuesResponse], error) {
	configs, err := h.logStore.ListQueues(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	queues := make([]*queuev1.Queue, len(configs))
	for i := range configs {
		queues[i] = h.queueToProto(&configs[i])
	}

	return connect.NewResponse(&queuev1.ListQueuesResponse{
		Queues: queues,
	}), nil
}

func (h *Handler) DeleteQueue(ctx context.Context, req *connect.Request[queuev1.DeleteQueueRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := h.manager.DeleteQueue(ctx, req.Msg.Name); err != nil {
		if err == storage.ErrQueueNotFound {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (h *Handler) UpdateQueue(ctx context.Context, req *connect.Request[queuev1.UpdateQueueRequest]) (*connect.Response[queuev1.Queue], error) {
	config, err := h.logStore.GetQueue(ctx, req.Msg.Name)
	if err != nil {
		if err == storage.ErrQueueNotFound {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(h.queueToProto(config)), nil
}

// --- Append Operations ---

func (h *Handler) Append(ctx context.Context, req *connect.Request[queuev1.AppendRequest]) (*connect.Response[queuev1.AppendResponse], error) {
	msg := req.Msg

	properties := make(map[string]string)
	if len(msg.Headers) > 0 {
		for k, v := range msg.Headers {
			properties[k] = string(v)
		}
	}
	if len(msg.PartitionKey) > 0 {
		properties["partition-key"] = string(msg.PartitionKey)
	}

	if err := h.manager.Enqueue(ctx, msg.QueueName, msg.Value, properties); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	config, err := h.logStore.GetQueue(ctx, msg.QueueName)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	partitionID := h.getPartitionID(string(msg.PartitionKey), config.Partitions)
	if msg.PartitionId != nil {
		partitionID = int(*msg.PartitionId)
	}
	tail, _ := h.logStore.Tail(ctx, msg.QueueName, partitionID)

	return connect.NewResponse(&queuev1.AppendResponse{
		Offset:      tail - 1,
		PartitionId: uint32(partitionID),
		Timestamp:   timestamppb.Now(),
	}), nil
}

func (h *Handler) AppendBatch(ctx context.Context, req *connect.Request[queuev1.AppendBatchRequest]) (*connect.Response[queuev1.AppendBatchResponse], error) {
	msg := req.Msg

	config, err := h.logStore.GetQueue(ctx, msg.QueueName)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	partitionID := h.getPartitionID(string(msg.PartitionKey), config.Partitions)
	if msg.PartitionId != nil {
		partitionID = int(*msg.PartitionId)
	}

	var firstOffset, lastOffset uint64
	var count uint32
	for i, entry := range msg.Messages {
		properties := make(map[string]string)
		if len(entry.Headers) > 0 {
			for k, v := range entry.Headers {
				properties[k] = string(v)
			}
		}

		if err := h.manager.Enqueue(ctx, msg.QueueName, entry.Value, properties); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		tail, _ := h.logStore.Tail(ctx, msg.QueueName, partitionID)
		offset := tail - 1
		if i == 0 {
			firstOffset = offset
		}
		lastOffset = offset
		count++
	}

	return connect.NewResponse(&queuev1.AppendBatchResponse{
		FirstOffset: firstOffset,
		LastOffset:  lastOffset,
		PartitionId: uint32(partitionID),
		Count:       count,
		Timestamp:   timestamppb.Now(),
	}), nil
}

func (h *Handler) AppendStream(ctx context.Context, stream *connect.ClientStream[queuev1.AppendRequest]) (*connect.Response[queuev1.AppendBatchResponse], error) {
	var firstOffset, lastOffset uint64
	var count uint32
	var lastPartitionID uint32

	first := true
	for stream.Receive() {
		msg := stream.Msg()

		properties := make(map[string]string)
		if len(msg.Headers) > 0 {
			for k, v := range msg.Headers {
				properties[k] = string(v)
			}
		}
		if len(msg.PartitionKey) > 0 {
			properties["partition-key"] = string(msg.PartitionKey)
		}

		if err := h.manager.Enqueue(ctx, msg.QueueName, msg.Value, properties); err != nil {
			continue
		}

		config, err := h.logStore.GetQueue(ctx, msg.QueueName)
		if err != nil {
			continue
		}

		partitionID := h.getPartitionID(string(msg.PartitionKey), config.Partitions)
		if msg.PartitionId != nil {
			partitionID = int(*msg.PartitionId)
		}
		tail, _ := h.logStore.Tail(ctx, msg.QueueName, partitionID)
		offset := tail - 1

		if first {
			firstOffset = offset
			first = false
		}
		lastOffset = offset
		lastPartitionID = uint32(partitionID)
		count++
	}

	if err := stream.Err(); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&queuev1.AppendBatchResponse{
		FirstOffset: firstOffset,
		LastOffset:  lastOffset,
		PartitionId: lastPartitionID,
		Count:       count,
		Timestamp:   timestamppb.Now(),
	}), nil
}

// --- Read Operations ---

func (h *Handler) Read(ctx context.Context, req *connect.Request[queuev1.ReadRequest]) (*connect.Response[queuev1.Message], error) {
	msg := req.Msg

	message, err := h.logStore.Read(ctx, msg.QueueName, int(msg.PartitionId), msg.Offset)
	if err != nil {
		if err == storage.ErrOffsetOutOfRange {
			return nil, connect.NewError(connect.CodeOutOfRange, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(h.messageToProto(message)), nil
}

func (h *Handler) ReadBatch(ctx context.Context, req *connect.Request[queuev1.ReadBatchRequest]) (*connect.Response[queuev1.ReadBatchResponse], error) {
	msg := req.Msg

	limit := int(msg.Limit)
	if limit == 0 {
		limit = 100
	}

	messages, err := h.logStore.ReadBatch(ctx, msg.QueueName, int(msg.PartitionId), msg.StartOffset, limit)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	protoMsgs := make([]*queuev1.Message, len(messages))
	for i, m := range messages {
		protoMsgs[i] = h.messageToProto(m)
	}

	return connect.NewResponse(&queuev1.ReadBatchResponse{
		Messages: protoMsgs,
	}), nil
}

func (h *Handler) Tail(ctx context.Context, req *connect.Request[queuev1.TailRequest], stream *connect.ServerStream[queuev1.Message]) error {
	msg := req.Msg
	offset := msg.StartOffset

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		messages, err := h.logStore.ReadBatch(ctx, msg.QueueName, int(msg.PartitionId), offset, 10)
		if err != nil {
			if err == storage.ErrOffsetOutOfRange {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return connect.NewError(connect.CodeInternal, err)
		}

		for _, m := range messages {
			if err := stream.Send(h.messageToProto(m)); err != nil {
				return err
			}
			offset = m.Sequence + 1
		}

		if len(messages) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// --- Seek Operations ---

func (h *Handler) SeekToOffset(ctx context.Context, req *connect.Request[queuev1.SeekToOffsetRequest]) (*connect.Response[queuev1.SeekResponse], error) {
	msg := req.Msg

	head, err := h.logStore.Head(ctx, msg.QueueName, int(msg.PartitionId))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tail, err := h.logStore.Tail(ctx, msg.QueueName, int(msg.PartitionId))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	offset := msg.Offset
	if offset < head {
		offset = head
	}
	if offset > tail {
		offset = tail
	}

	return connect.NewResponse(&queuev1.SeekResponse{
		Offset:      offset,
		PartitionId: msg.PartitionId,
	}), nil
}

func (h *Handler) SeekToTimestamp(ctx context.Context, req *connect.Request[queuev1.SeekToTimestampRequest]) (*connect.Response[queuev1.SeekResponse], error) {
	msg := req.Msg

	head, err := h.logStore.Head(ctx, msg.QueueName, int(msg.PartitionId))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&queuev1.SeekResponse{
		Offset:      head,
		PartitionId: msg.PartitionId,
	}), nil
}

// --- Consumer Group Operations ---

func (h *Handler) CreateConsumerGroup(ctx context.Context, req *connect.Request[queuev1.CreateConsumerGroupRequest]) (*connect.Response[queuev1.ConsumerGroup], error) {
	msg := req.Msg

	config, err := h.logStore.GetQueue(ctx, msg.QueueName)
	if err != nil {
		if err == storage.ErrQueueNotFound {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	group := &types.ConsumerGroupState{
		ID:        msg.GroupId,
		QueueName: msg.QueueName,
		Pattern:   "",
		Cursors:   make(map[int]*types.PartitionCursor),
		Consumers: make(map[string]*types.ConsumerInfo),
		PEL:       make(map[string][]*types.PendingEntry),
		CreatedAt: time.Now(),
	}

	for i := 0; i < config.Partitions; i++ {
		head, _ := h.logStore.Head(ctx, msg.QueueName, i)
		group.Cursors[i] = &types.PartitionCursor{
			PartitionID: i,
			Cursor:      head,
			Committed:   head,
		}
	}

	if err := h.groupStore.CreateConsumerGroup(ctx, group); err != nil {
		if err == storage.ErrConsumerGroupExists {
			return nil, connect.NewError(connect.CodeAlreadyExists, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(h.groupToProto(group)), nil
}

func (h *Handler) GetConsumerGroup(ctx context.Context, req *connect.Request[queuev1.GetConsumerGroupRequest]) (*connect.Response[queuev1.ConsumerGroup], error) {
	msg := req.Msg

	group, err := h.groupStore.GetConsumerGroup(ctx, msg.QueueName, msg.GroupId)
	if err != nil {
		if err == storage.ErrConsumerNotFound {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(h.groupToProto(group)), nil
}

func (h *Handler) ListConsumerGroups(ctx context.Context, req *connect.Request[queuev1.ListConsumerGroupsRequest]) (*connect.Response[queuev1.ListConsumerGroupsResponse], error) {
	msg := req.Msg

	groups, err := h.groupStore.ListConsumerGroups(ctx, msg.QueueName)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	protoGroups := make([]*queuev1.ConsumerGroup, len(groups))
	for i, g := range groups {
		protoGroups[i] = h.groupToProto(g)
	}

	return connect.NewResponse(&queuev1.ListConsumerGroupsResponse{
		Groups: protoGroups,
	}), nil
}

func (h *Handler) DeleteConsumerGroup(ctx context.Context, req *connect.Request[queuev1.DeleteConsumerGroupRequest]) (*connect.Response[emptypb.Empty], error) {
	msg := req.Msg

	if err := h.groupStore.DeleteConsumerGroup(ctx, msg.QueueName, msg.GroupId); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (h *Handler) JoinGroup(ctx context.Context, req *connect.Request[queuev1.JoinGroupRequest]) (*connect.Response[queuev1.JoinGroupResponse], error) {
	msg := req.Msg

	consumer := &types.ConsumerInfo{
		ID:            msg.ConsumerId,
		ClientID:      msg.ConsumerId,
		LastHeartbeat: time.Now(),
		RegisteredAt:  time.Now(),
	}

	if err := h.groupStore.RegisterConsumer(ctx, msg.QueueName, msg.GroupId, consumer); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	group, err := h.groupStore.GetConsumerGroup(ctx, msg.QueueName, msg.GroupId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	assignedPartitions := make([]uint32, 0)
	for partitionID := range group.Cursors {
		assignedPartitions = append(assignedPartitions, uint32(partitionID))
	}

	return connect.NewResponse(&queuev1.JoinGroupResponse{
		GenerationId:       1,
		AssignedPartitions: assignedPartitions,
	}), nil
}

func (h *Handler) LeaveGroup(ctx context.Context, req *connect.Request[queuev1.LeaveGroupRequest]) (*connect.Response[emptypb.Empty], error) {
	msg := req.Msg

	if err := h.groupStore.UnregisterConsumer(ctx, msg.QueueName, msg.GroupId, msg.ConsumerId); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (h *Handler) Heartbeat(ctx context.Context, req *connect.Request[queuev1.HeartbeatRequest]) (*connect.Response[queuev1.HeartbeatResponse], error) {
	msg := req.Msg

	group, err := h.groupStore.GetConsumerGroup(ctx, msg.QueueName, msg.GroupId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	consumer, exists := group.Consumers[msg.ConsumerId]
	if !exists {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("consumer not found"))
	}

	consumer.LastHeartbeat = time.Now()

	return connect.NewResponse(&queuev1.HeartbeatResponse{
		ShouldRejoin: false,
	}), nil
}

// --- Consume Operations ---

func (h *Handler) Consume(ctx context.Context, req *connect.Request[queuev1.ConsumeRequest]) (*connect.Response[queuev1.ConsumeResponse], error) {
	msg := req.Msg

	group, err := h.groupStore.GetConsumerGroup(ctx, msg.QueueName, msg.GroupId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	limit := int(msg.MaxMessages)
	if limit == 0 {
		limit = 10
	}

	var messages []*queuev1.Message
	for partitionID, cursor := range group.Cursors {
		msgs, err := h.logStore.ReadBatch(ctx, msg.QueueName, partitionID, cursor.Cursor, limit-len(messages))
		if err != nil {
			continue
		}

		for _, m := range msgs {
			entry := &types.PendingEntry{
				PartitionID: partitionID,
				Offset:      m.Sequence,
				ConsumerID:  msg.ConsumerId,
				ClaimedAt:   time.Now(),
			}
			h.groupStore.AddPendingEntry(ctx, msg.QueueName, msg.GroupId, entry)

			messages = append(messages, h.messageToProto(m))
		}

		if len(messages) >= limit {
			break
		}
	}

	return connect.NewResponse(&queuev1.ConsumeResponse{
		Messages: messages,
	}), nil
}

func (h *Handler) ConsumeStream(ctx context.Context, req *connect.Request[queuev1.ConsumeStreamRequest], stream *connect.ServerStream[queuev1.Message]) error {
	msg := req.Msg

	group, err := h.groupStore.GetConsumerGroup(ctx, msg.QueueName, msg.GroupId)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	cursors := make(map[int]uint64)
	for partitionID, cursor := range group.Cursors {
		cursors[partitionID] = cursor.Cursor
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		sent := false
		for partitionID, offset := range cursors {
			messages, err := h.logStore.ReadBatch(ctx, msg.QueueName, partitionID, offset, 10)
			if err != nil || len(messages) == 0 {
				continue
			}

			for _, m := range messages {
				entry := &types.PendingEntry{
					PartitionID: partitionID,
					Offset:      m.Sequence,
					ConsumerID:  msg.ConsumerId,
					ClaimedAt:   time.Now(),
				}
				h.groupStore.AddPendingEntry(ctx, msg.QueueName, msg.GroupId, entry)

				if err := stream.Send(h.messageToProto(m)); err != nil {
					return err
				}
				cursors[partitionID] = m.Sequence + 1
				sent = true
			}
		}

		if !sent {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (h *Handler) Ack(ctx context.Context, req *connect.Request[queuev1.AckRequest]) (*connect.Response[queuev1.AckResponse], error) {
	msg := req.Msg

	var success int32
	for _, partitionOffsets := range msg.Offsets {
		for _, offset := range partitionOffsets.Offsets {
			err := h.groupStore.RemovePendingEntry(ctx, msg.QueueName, msg.GroupId, msg.ConsumerId, int(partitionOffsets.PartitionId), offset)
			if err == nil {
				success++
			}
		}

		if len(partitionOffsets.Offsets) > 0 {
			maxOffset := partitionOffsets.Offsets[len(partitionOffsets.Offsets)-1]
			h.groupStore.UpdateCommitted(ctx, msg.QueueName, msg.GroupId, int(partitionOffsets.PartitionId), maxOffset+1)
		}
	}

	return connect.NewResponse(&queuev1.AckResponse{
		AckedCount: uint32(success),
	}), nil
}

func (h *Handler) Nack(ctx context.Context, req *connect.Request[queuev1.NackRequest]) (*connect.Response[emptypb.Empty], error) {
	msg := req.Msg

	for _, partitionOffsets := range msg.Offsets {
		for _, offset := range partitionOffsets.Offsets {
			h.groupStore.RemovePendingEntry(ctx, msg.QueueName, msg.GroupId, msg.ConsumerId, int(partitionOffsets.PartitionId), offset)
		}
	}

	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (h *Handler) Claim(ctx context.Context, req *connect.Request[queuev1.ClaimRequest]) (*connect.Response[queuev1.ClaimResponse], error) {
	msg := req.Msg

	limit := int(msg.Limit)
	if limit == 0 {
		limit = 10
	}

	var claimed []*queuev1.Message
	group, err := h.groupStore.GetConsumerGroup(ctx, msg.QueueName, msg.GroupId)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	minIdleTime := time.Duration(0)
	if msg.MinIdleTime != nil {
		minIdleTime = msg.MinIdleTime.AsDuration()
	}

	for _, pel := range group.PEL {
		for _, entry := range pel {
			if entry.ConsumerID == msg.ConsumerId {
				continue
			}
			if time.Since(entry.ClaimedAt) < minIdleTime {
				continue
			}

			if msg.PartitionId != nil && entry.PartitionID != int(*msg.PartitionId) {
				continue
			}

			m, err := h.logStore.Read(ctx, msg.QueueName, entry.PartitionID, entry.Offset)
			if err != nil {
				continue
			}

			entry.ConsumerID = msg.ConsumerId
			entry.ClaimedAt = time.Now()
			entry.DeliveryCount++

			claimed = append(claimed, h.messageToProto(m))
			if len(claimed) >= limit {
				break
			}
		}
		if len(claimed) >= limit {
			break
		}
	}

	return connect.NewResponse(&queuev1.ClaimResponse{
		Messages: claimed,
	}), nil
}

func (h *Handler) GetPending(ctx context.Context, req *connect.Request[queuev1.GetPendingRequest]) (*connect.Response[queuev1.GetPendingResponse], error) {
	msg := req.Msg

	var entries []*types.PendingEntry
	var err error

	if msg.ConsumerId != "" {
		entries, err = h.groupStore.GetPendingEntries(ctx, msg.QueueName, msg.GroupId, msg.ConsumerId)
	} else {
		entries, err = h.groupStore.GetAllPendingEntries(ctx, msg.QueueName, msg.GroupId)
	}

	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	protoEntries := make([]*queuev1.PendingEntry, len(entries))
	for i, e := range entries {
		protoEntries[i] = &queuev1.PendingEntry{
			PartitionId:   uint32(e.PartitionID),
			Offset:        e.Offset,
			ConsumerId:    e.ConsumerID,
			DeliveredAt:   timestamppb.New(e.ClaimedAt),
			DeliveryCount: uint32(e.DeliveryCount),
		}
	}

	return connect.NewResponse(&queuev1.GetPendingResponse{
		Entries: protoEntries,
	}), nil
}

// --- Partition Info ---

func (h *Handler) GetPartitionInfo(ctx context.Context, req *connect.Request[queuev1.GetPartitionInfoRequest]) (*connect.Response[queuev1.PartitionInfo], error) {
	msg := req.Msg

	head, err := h.logStore.Head(ctx, msg.QueueName, int(msg.PartitionId))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tail, err := h.logStore.Tail(ctx, msg.QueueName, int(msg.PartitionId))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	count, _ := h.logStore.Count(ctx, msg.QueueName, int(msg.PartitionId))

	return connect.NewResponse(&queuev1.PartitionInfo{
		PartitionId:  msg.PartitionId,
		HeadOffset:   head,
		TailOffset:   tail,
		MessageCount: count,
	}), nil
}

func (h *Handler) ListPartitions(ctx context.Context, req *connect.Request[queuev1.ListPartitionsRequest]) (*connect.Response[queuev1.ListPartitionsResponse], error) {
	msg := req.Msg

	config, err := h.logStore.GetQueue(ctx, msg.QueueName)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	partitions := make([]*queuev1.PartitionInfo, config.Partitions)
	for i := 0; i < config.Partitions; i++ {
		head, _ := h.logStore.Head(ctx, msg.QueueName, i)
		tail, _ := h.logStore.Tail(ctx, msg.QueueName, i)
		count, _ := h.logStore.Count(ctx, msg.QueueName, i)

		partitions[i] = &queuev1.PartitionInfo{
			PartitionId:  uint32(i),
			HeadOffset:   head,
			TailOffset:   tail,
			MessageCount: count,
		}
	}

	return connect.NewResponse(&queuev1.ListPartitionsResponse{
		Partitions: partitions,
	}), nil
}

// --- Stats ---

func (h *Handler) GetStats(ctx context.Context, req *connect.Request[queuev1.GetStatsRequest]) (*connect.Response[queuev1.QueueStats], error) {
	msg := req.Msg

	config, err := h.logStore.GetQueue(ctx, msg.QueueName)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	totalCount, _ := h.logStore.TotalCount(ctx, msg.QueueName)

	partitionStats := make([]*queuev1.PartitionStats, config.Partitions)
	for i := 0; i < config.Partitions; i++ {
		head, _ := h.logStore.Head(ctx, msg.QueueName, i)
		tail, _ := h.logStore.Tail(ctx, msg.QueueName, i)
		count, _ := h.logStore.Count(ctx, msg.QueueName, i)

		partitionStats[i] = &queuev1.PartitionStats{
			PartitionId:  uint32(i),
			HeadOffset:   head,
			TailOffset:   tail,
			MessageCount: count,
		}
	}

	return connect.NewResponse(&queuev1.QueueStats{
		QueueName:     msg.QueueName,
		Partitions:    partitionStats,
		TotalMessages: totalCount,
	}), nil
}

// --- Admin Operations ---

func (h *Handler) Purge(ctx context.Context, req *connect.Request[queuev1.PurgeRequest]) (*connect.Response[queuev1.PurgeResponse], error) {
	msg := req.Msg

	config, err := h.logStore.GetQueue(ctx, msg.QueueName)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var purged uint64
	if msg.PartitionId != nil {
		count, _ := h.logStore.Count(ctx, msg.QueueName, int(*msg.PartitionId))
		tail, _ := h.logStore.Tail(ctx, msg.QueueName, int(*msg.PartitionId))
		h.logStore.Truncate(ctx, msg.QueueName, int(*msg.PartitionId), tail)
		purged = count
	} else {
		for i := 0; i < config.Partitions; i++ {
			count, _ := h.logStore.Count(ctx, msg.QueueName, i)
			tail, _ := h.logStore.Tail(ctx, msg.QueueName, i)
			h.logStore.Truncate(ctx, msg.QueueName, i, tail)
			purged += count
		}
	}

	return connect.NewResponse(&queuev1.PurgeResponse{
		MessagesDeleted: purged,
	}), nil
}

func (h *Handler) Truncate(ctx context.Context, req *connect.Request[queuev1.TruncateRequest]) (*connect.Response[emptypb.Empty], error) {
	msg := req.Msg

	if err := h.logStore.Truncate(ctx, msg.QueueName, int(msg.PartitionId), msg.MinOffset); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&emptypb.Empty{}), nil
}

// --- Helper Functions ---

func (h *Handler) queueToProto(config *types.QueueConfig) *queuev1.Queue {
	return &queuev1.Queue{
		Name:       config.Name,
		Partitions: uint32(config.Partitions),
		Config: &queuev1.QueueConfig{
			Retention: &queuev1.RetentionConfig{
				MaxAge: durationpb.New(config.MessageTTL),
			},
		},
	}
}

func (h *Handler) messageToProto(msg *types.Message) *queuev1.Message {
	protoMsg := &queuev1.Message{
		Offset:      msg.Sequence,
		PartitionId: uint32(msg.PartitionID),
		Timestamp:   timestamppb.New(msg.CreatedAt),
		Value:       msg.GetPayload(),
		Key:         []byte(msg.PartitionKey),
	}

	if len(msg.Properties) > 0 {
		protoMsg.Headers = make(map[string][]byte)
		for k, v := range msg.Properties {
			protoMsg.Headers[k] = []byte(v)
		}
	}

	return protoMsg
}

func (h *Handler) groupToProto(group *types.ConsumerGroupState) *queuev1.ConsumerGroup {
	consumers := make([]*queuev1.ConsumerInfo, 0, len(group.Consumers))
	for _, c := range group.Consumers {
		consumers = append(consumers, &queuev1.ConsumerInfo{
			ConsumerId:    c.ID,
			LastHeartbeat: timestamppb.New(c.LastHeartbeat),
		})
	}

	cursors := make([]*queuev1.PartitionCursor, 0, len(group.Cursors))
	for partitionID, cursor := range group.Cursors {
		cursors = append(cursors, &queuev1.PartitionCursor{
			PartitionId: uint32(partitionID),
			Cursor:      cursor.Cursor,
			Committed:   cursor.Committed,
		})
	}

	var pendingCount uint64
	for _, entries := range group.PEL {
		pendingCount += uint64(len(entries))
	}

	return &queuev1.ConsumerGroup{
		GroupId:      group.ID,
		QueueName:    group.QueueName,
		Consumers:    consumers,
		Cursors:      cursors,
		PendingCount: pendingCount,
		CreatedAt:    timestamppb.New(group.CreatedAt),
	}
}

func (h *Handler) getPartitionID(key string, partitions int) int {
	if partitions <= 0 {
		return 0
	}
	if key == "" {
		return 0
	}

	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}

	return int(hash % uint32(partitions))
}
