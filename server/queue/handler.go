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
	queueStore storage.QueueStore
	groupStore storage.ConsumerGroupStore
	logger     *slog.Logger
}

// NewHandler creates a new queue service handler.
func NewHandler(manager *queue.Manager, queueStore storage.QueueStore, groupStore storage.ConsumerGroupStore, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		manager:    manager,
		queueStore: queueStore,
		groupStore: groupStore,
		logger:     logger,
	}
}

// --- Queue Management ---
func (h *Handler) CreateQueue(ctx context.Context, req *connect.Request[queuev1.CreateQueueRequest]) (*connect.Response[queuev1.Queue], error) {
	msg := req.Msg

	config := types.QueueConfig{
		Name:       msg.Name,
		Topics:     []string{msg.Name}, // Default subject matches queue name
		MessageTTL: 7 * 24 * time.Hour,
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
	config, err := h.queueStore.GetQueue(ctx, req.Msg.Name)
	if err != nil {
		if err == storage.ErrQueueNotFound {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(h.queueToProto(config)), nil
}

func (h *Handler) ListQueues(ctx context.Context, req *connect.Request[queuev1.ListQueuesRequest]) (*connect.Response[queuev1.ListQueuesResponse], error) {
	configs, err := h.queueStore.ListQueues(ctx)
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
	config, err := h.queueStore.GetQueue(ctx, req.Msg.Name)
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

	if err := h.manager.Publish(ctx, msg.QueueName, msg.Value, properties); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tail, _ := h.queueStore.Tail(ctx, msg.QueueName)

	return connect.NewResponse(&queuev1.AppendResponse{
		Offset:    tail - 1,
		Timestamp: timestamppb.Now(),
	}), nil
}

func (h *Handler) AppendBatch(ctx context.Context, req *connect.Request[queuev1.AppendBatchRequest]) (*connect.Response[queuev1.AppendBatchResponse], error) {
	msg := req.Msg

	var firstOffset, lastOffset uint64
	var count uint32

	for i, entry := range msg.Messages {
		properties := make(map[string]string)
		if len(entry.Headers) > 0 {
			for k, v := range entry.Headers {
				properties[k] = string(v)
			}
		}

		if err := h.manager.Publish(ctx, msg.QueueName, entry.Value, properties); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		tail, _ := h.queueStore.Tail(ctx, msg.QueueName)
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
		Count:       count,
		Timestamp:   timestamppb.Now(),
	}), nil
}

func (h *Handler) AppendStream(ctx context.Context, stream *connect.ClientStream[queuev1.AppendRequest]) (*connect.Response[queuev1.AppendBatchResponse], error) {
	var firstOffset, lastOffset uint64
	var count uint32

	first := true
	for stream.Receive() {
		msg := stream.Msg()

		properties := make(map[string]string)
		if len(msg.Headers) > 0 {
			for k, v := range msg.Headers {
				properties[k] = string(v)
			}
		}

		if err := h.manager.Publish(ctx, msg.QueueName, msg.Value, properties); err != nil {
			continue
		}

		tail, _ := h.queueStore.Tail(ctx, msg.QueueName)
		offset := tail - 1

		if first {
			firstOffset = offset
			first = false
		}
		lastOffset = offset
		count++
	}

	if err := stream.Err(); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&queuev1.AppendBatchResponse{
		FirstOffset: firstOffset,
		LastOffset:  lastOffset,
		Count:       count,
		Timestamp:   timestamppb.Now(),
	}), nil
}

// --- Read Operations ---

func (h *Handler) Read(ctx context.Context, req *connect.Request[queuev1.ReadRequest]) (*connect.Response[queuev1.Message], error) {
	msg := req.Msg

	message, err := h.queueStore.Read(ctx, msg.QueueName, msg.Offset)
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

	messages, err := h.queueStore.ReadBatch(ctx, msg.QueueName, msg.StartOffset, limit)
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

		messages, err := h.queueStore.ReadBatch(ctx, msg.QueueName, offset, 10)
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

	head, err := h.queueStore.Head(ctx, msg.QueueName)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tail, err := h.queueStore.Tail(ctx, msg.QueueName)
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
		Offset: offset,
	}), nil
}

func (h *Handler) SeekToTimestamp(ctx context.Context, req *connect.Request[queuev1.SeekToTimestampRequest]) (*connect.Response[queuev1.SeekResponse], error) {
	msg := req.Msg

	head, err := h.queueStore.Head(ctx, msg.QueueName)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&queuev1.SeekResponse{
		Offset: head,
	}), nil
}

// --- Consumer Group Operations ---

func (h *Handler) CreateConsumerGroup(ctx context.Context, req *connect.Request[queuev1.CreateConsumerGroupRequest]) (*connect.Response[queuev1.ConsumerGroup], error) {
	msg := req.Msg

	_, err := h.queueStore.GetQueue(ctx, msg.QueueName)
	if err != nil {
		if err == storage.ErrQueueNotFound {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	head, _ := h.queueStore.Head(ctx, msg.QueueName)

	group := types.NewConsumerGroupState(msg.QueueName, msg.GroupId, "")
	group.Cursor.Cursor = head
	group.Cursor.Committed = head

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

	return connect.NewResponse(&queuev1.JoinGroupResponse{
		GenerationId: 1,
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

	consumer := group.GetConsumer(msg.ConsumerId)
	if consumer == nil {
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

	cursor := group.GetCursor()
	messages, err := h.queueStore.ReadBatch(ctx, msg.QueueName, cursor.Cursor, limit)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var protoMsgs []*queuev1.Message
	for _, m := range messages {
		entry := &types.PendingEntry{
			Offset:     m.Sequence,
			ConsumerID: msg.ConsumerId,
			ClaimedAt:  time.Now(),
		}
		h.groupStore.AddPendingEntry(ctx, msg.QueueName, msg.GroupId, entry)

		protoMsgs = append(protoMsgs, h.messageToProto(m))
	}

	return connect.NewResponse(&queuev1.ConsumeResponse{
		Messages: protoMsgs,
	}), nil
}

func (h *Handler) ConsumeStream(ctx context.Context, req *connect.Request[queuev1.ConsumeQueueRequest], stream *connect.ServerStream[queuev1.Message]) error {
	msg := req.Msg

	group, err := h.groupStore.GetConsumerGroup(ctx, msg.QueueName, msg.GroupId)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	cursor := group.GetCursor()
	offset := cursor.Cursor

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		messages, err := h.queueStore.ReadBatch(ctx, msg.QueueName, offset, 10)
		if err != nil || len(messages) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, m := range messages {
			entry := &types.PendingEntry{
				Offset:     m.Sequence,
				ConsumerID: msg.ConsumerId,
				ClaimedAt:  time.Now(),
			}
			h.groupStore.AddPendingEntry(ctx, msg.QueueName, msg.GroupId, entry)

			if err := stream.Send(h.messageToProto(m)); err != nil {
				return err
			}
			offset = m.Sequence + 1
		}
	}
}

func (h *Handler) Ack(ctx context.Context, req *connect.Request[queuev1.AckRequest]) (*connect.Response[queuev1.AckResponse], error) {
	msg := req.Msg

	var success int32
	var maxOffset uint64
	for _, offset := range msg.Offsets {
		err := h.groupStore.RemovePendingEntry(ctx, msg.QueueName, msg.GroupId, msg.ConsumerId, offset)
		if err == nil {
			success++
		}
		if offset > maxOffset {
			maxOffset = offset
		}
	}

	if len(msg.Offsets) > 0 {
		h.groupStore.UpdateCommitted(ctx, msg.QueueName, msg.GroupId, maxOffset+1)
	}

	return connect.NewResponse(&queuev1.AckResponse{
		AckedCount: uint32(success),
	}), nil
}

func (h *Handler) Nack(ctx context.Context, req *connect.Request[queuev1.NackRequest]) (*connect.Response[emptypb.Empty], error) {
	msg := req.Msg

	for _, offset := range msg.Offsets {
		h.groupStore.RemovePendingEntry(ctx, msg.QueueName, msg.GroupId, msg.ConsumerId, offset)
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

			m, err := h.queueStore.Read(ctx, msg.QueueName, entry.Offset)
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

// --- Queue Info ---

func (h *Handler) GetQueueInfo(ctx context.Context, req *connect.Request[queuev1.GetQueueInfoRequest]) (*connect.Response[queuev1.QueueInfo], error) {
	msg := req.Msg

	head, err := h.queueStore.Head(ctx, msg.QueueName)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tail, err := h.queueStore.Tail(ctx, msg.QueueName)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	count, _ := h.queueStore.Count(ctx, msg.QueueName)

	return connect.NewResponse(&queuev1.QueueInfo{
		QueueName:    msg.QueueName,
		HeadOffset:   head,
		TailOffset:   tail,
		MessageCount: count,
	}), nil
}

// --- Stats ---

func (h *Handler) GetStats(ctx context.Context, req *connect.Request[queuev1.GetStatsRequest]) (*connect.Response[queuev1.QueueStats], error) {
	msg := req.Msg

	head, _ := h.queueStore.Head(ctx, msg.QueueName)
	tail, _ := h.queueStore.Tail(ctx, msg.QueueName)
	count, _ := h.queueStore.Count(ctx, msg.QueueName)

	return connect.NewResponse(&queuev1.QueueStats{
		QueueName:    msg.QueueName,
		MessageCount: count,
		HeadOffset:   head,
		TailOffset:   tail,
	}), nil
}

// --- Admin Operations ---

func (h *Handler) Purge(ctx context.Context, req *connect.Request[queuev1.PurgeRequest]) (*connect.Response[queuev1.PurgeResponse], error) {
	msg := req.Msg

	count, _ := h.queueStore.Count(ctx, msg.QueueName)
	tail, _ := h.queueStore.Tail(ctx, msg.QueueName)
	h.queueStore.Truncate(ctx, msg.QueueName, tail)

	return connect.NewResponse(&queuev1.PurgeResponse{
		MessagesDeleted: count,
	}), nil
}

func (h *Handler) Truncate(ctx context.Context, req *connect.Request[queuev1.TruncateRequest]) (*connect.Response[emptypb.Empty], error) {
	msg := req.Msg

	if err := h.queueStore.Truncate(ctx, msg.QueueName, msg.MinOffset); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&emptypb.Empty{}), nil
}

// --- Helper Functions ---

func (h *Handler) queueToProto(config *types.QueueConfig) *queuev1.Queue {
	return &queuev1.Queue{
		Name:   config.Name,
		Topics: config.Topics,
		Config: &queuev1.QueueConfig{
			Retention: &queuev1.RetentionConfig{
				MaxAge: durationpb.New(config.MessageTTL),
			},
		},
	}
}

func (h *Handler) messageToProto(msg *types.Message) *queuev1.Message {
	protoMsg := &queuev1.Message{
		Offset:    msg.Sequence,
		Timestamp: timestamppb.New(msg.CreatedAt),
		Value:     msg.GetPayload(),
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

	cursor := group.GetCursor()
	queueCursor := &queuev1.QueueCursor{
		Cursor:    cursor.Cursor,
		Committed: cursor.Committed,
	}

	var pendingCount uint64
	for _, entries := range group.PEL {
		pendingCount += uint64(len(entries))
	}

	return &queuev1.ConsumerGroup{
		GroupId:      group.ID,
		QueueName:    group.QueueName,
		Consumers:    consumers,
		Cursor:       queueCursor,
		PendingCount: pendingCount,
		CreatedAt:    timestamppb.New(group.CreatedAt),
	}
}
