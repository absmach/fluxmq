// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/absmach/fluxmq/message"
	coremessage "github.com/absmach/fluxmq/message"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	"github.com/absmach/fluxmq/pkg/proto/queue/v1/queuev1connect"
	"github.com/absmach/fluxmq/queue"
	"github.com/absmach/fluxmq/queue/consumer"
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
	if manager != nil {
		if queueStore == nil {
			queueStore = manager.QueueStore()
		}
		if groupStore == nil {
			groupStore = manager.GroupStore()
		}
	}

	return &Handler{
		manager:    manager,
		queueStore: queueStore,
		groupStore: groupStore,
		logger:     logger,
	}
}

// --- Queue Management ---.
func (h *Handler) CreateQueue(ctx context.Context, req *connect.Request[queuev1.CreateQueueRequest]) (*connect.Response[queuev1.Queue], error) {
	msg := req.Msg

	topics := msg.Topics
	if len(topics) == 0 {
		topics = []string{msg.Name}
	}

	config := types.DefaultQueueConfig(msg.Name, topics...)
	if msg.Config != nil {
		applyQueueConfigUpdateFromProto(&config, msg.Config)
	}

	if err := h.manager.CreateQueue(ctx, config); err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(h.queueToProto(&config)), nil
}

func (h *Handler) GetQueue(ctx context.Context, req *connect.Request[queuev1.GetQueueRequest]) (*connect.Response[queuev1.Queue], error) {
	config, err := h.queueStore.GetQueue(ctx, req.Msg.Name)
	if err != nil {
		if errors.Is(err, storage.ErrQueueNotFound) {
			return nil, newConnectError(queue.ErrorCodeNotFound, err)
		}
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(h.queueToProto(config)), nil
}

func (h *Handler) ListQueues(ctx context.Context, req *connect.Request[queuev1.ListQueuesRequest]) (*connect.Response[queuev1.ListQueuesResponse], error) {
	configs, err := h.queueStore.ListQueues(ctx)
	if err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	filtered := make([]types.QueueConfig, 0, len(configs))
	prefix := req.Msg.Prefix
	for _, cfg := range configs {
		if prefix != "" && !strings.HasPrefix(cfg.Name, prefix) {
			continue
		}
		filtered = append(filtered, cfg)
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Name < filtered[j].Name
	})

	start := 0
	pageToken := req.Msg.PageToken
	if pageToken != "" {
		for i, cfg := range filtered {
			if cfg.Name > pageToken {
				start = i
				break
			}
			start = len(filtered)
		}
	}

	end := len(filtered)
	limit := int(req.Msg.Limit)
	if limit > 0 && start+limit < end {
		end = start + limit
	}

	page := filtered[start:end]
	queues := make([]*queuev1.Queue, len(page))
	for i := range page {
		queues[i] = h.queueToProto(&page[i])
	}

	nextPageToken := ""
	if end < len(filtered) && len(page) > 0 {
		nextPageToken = page[len(page)-1].Name
	}

	return connect.NewResponse(&queuev1.ListQueuesResponse{
		Queues:        queues,
		NextPageToken: nextPageToken,
	}), nil
}

func (h *Handler) DeleteQueue(ctx context.Context, req *connect.Request[queuev1.DeleteQueueRequest]) (*connect.Response[emptypb.Empty], error) {
	if err := h.manager.DeleteQueue(ctx, req.Msg.Name); err != nil {
		if errors.Is(err, queue.ErrProtectedQueueMutation) {
			return nil, newConnectError(queue.ErrorCodeFailedPrecondition, err)
		}
		if errors.Is(err, storage.ErrQueueNotFound) {
			return nil, newConnectError(queue.ErrorCodeNotFound, err)
		}
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (h *Handler) UpdateQueue(ctx context.Context, req *connect.Request[queuev1.UpdateQueueRequest]) (*connect.Response[queuev1.Queue], error) {
	config, err := h.queueStore.GetQueue(ctx, req.Msg.Name)
	if err != nil {
		if errors.Is(err, storage.ErrQueueNotFound) {
			return nil, newConnectError(queue.ErrorCodeNotFound, err)
		}
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	updated := *config
	if req.Msg.Config != nil {
		applyQueueConfigUpdateFromProto(&updated, req.Msg.Config)
	}

	if h.manager != nil {
		if err := h.manager.UpdateQueue(ctx, updated); err != nil {
			return nil, newConnectError(queue.ErrorCodeInternal, err)
		}
		current, err := h.queueStore.GetQueue(ctx, updated.Name)
		if err != nil {
			if errors.Is(err, storage.ErrQueueNotFound) {
				return nil, newConnectError(queue.ErrorCodeNotFound, err)
			}
			return nil, newConnectError(queue.ErrorCodeInternal, err)
		}
		return connect.NewResponse(h.queueToProto(current)), nil
	}

	if err := h.queueStore.UpdateQueue(ctx, updated); err != nil {
		if errors.Is(err, storage.ErrQueueNotFound) {
			return nil, newConnectError(queue.ErrorCodeNotFound, err)
		}
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(h.queueToProto(&updated)), nil
}

// --- Append Operations ---

func (h *Handler) Append(ctx context.Context, req *connect.Request[queuev1.AppendRequest]) (*connect.Response[queuev1.AppendResponse], error) {
	msg := req.Msg

	envelope := appendEnvelope(msg.QueueName, msg.Value, msg.Key, msg.Headers)
	defer message.Release(envelope)

	outcome, err := h.manager.StateMachine().Append(ctx, queue.AppendCommand{
		QueueName: msg.QueueName,
		Envelopes: []*message.Envelope{envelope},
	})
	if err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(&queuev1.AppendResponse{
		Offset:    outcome.FirstOffset,
		Timestamp: timestamppb.New(outcome.Timestamp),
	}), nil
}

func (h *Handler) AppendBatch(ctx context.Context, req *connect.Request[queuev1.AppendBatchRequest]) (*connect.Response[queuev1.AppendBatchResponse], error) {
	msg := req.Msg

	// Offset 0 is a valid offset, so an empty batch cannot be answered with a
	// zero-valued success: the client could not tell it from a real append.
	if len(msg.Messages) == 0 {
		return nil, newConnectError(queue.ErrorCodeInvalidArgument, errEmptyBatch)
	}

	envelopes := make([]*message.Envelope, len(msg.Messages))
	for i, entry := range msg.Messages {
		envelopes[i] = appendEnvelope(msg.QueueName, entry.Value, entry.Key, entry.Headers)
	}
	defer func() {
		for _, envelope := range envelopes {
			message.Release(envelope)
		}
	}()

	outcome, err := h.manager.StateMachine().Append(ctx, queue.AppendCommand{
		QueueName:   msg.QueueName,
		Envelopes:   envelopes,
		AtomicBatch: true,
	})
	if err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(&queuev1.AppendBatchResponse{
		FirstOffset: outcome.FirstOffset,
		LastOffset:  outcome.LastOffset,
		Count:       outcome.Count,
		Timestamp:   timestamppb.New(outcome.Timestamp),
	}), nil
}

// AppendQueue appends a stream of messages to one queue. The stream is pinned
// to the first message's queue: a mid-stream change would make the single
// returned offset range meaningless.
//
// The stream commits a prefix and stops at the first failed append. The error
// carries that prefix as AppendProgress so the client can resume rather
// than re-send and duplicate it.
func (h *Handler) AppendQueue(ctx context.Context, stream *connect.ClientStream[queuev1.AppendRequest]) (*connect.Response[queuev1.AppendBatchResponse], error) {
	var (
		queueName     string
		firstOffset   uint64
		lastOffset    uint64
		lastTimestamp time.Time
		count         uint32
	)

	for stream.Receive() {
		msg := stream.Msg()

		if count == 0 {
			queueName = msg.QueueName
		} else if msg.QueueName != queueName {
			return nil, newConnectErrorWithProgress(
				queue.ErrorCodeInvalidArgument,
				fmt.Errorf("%w: stream is pinned to queue %q, got %q", errStreamQueueChanged, queueName, msg.QueueName),
				appendProgress(count, firstOffset, lastOffset),
			)
		}

		envelope := appendEnvelope(msg.QueueName, msg.Value, msg.Key, msg.Headers)
		outcome, err := h.manager.StateMachine().Append(ctx, queue.AppendCommand{
			QueueName: msg.QueueName,
			Envelopes: []*message.Envelope{envelope},
		})
		message.Release(envelope)
		if err != nil {
			return nil, newConnectErrorWithProgress(
				queue.ErrorCodeInternal, err, appendProgress(count, firstOffset, lastOffset),
			)
		}

		if count == 0 {
			firstOffset = outcome.FirstOffset
		}
		lastOffset = outcome.FirstOffset
		lastTimestamp = outcome.Timestamp
		count++
	}

	if err := stream.Err(); err != nil {
		return nil, newConnectErrorWithProgress(
			queue.ErrorCodeInternal, err, appendProgress(count, firstOffset, lastOffset),
		)
	}

	// Same reasoning as AppendBatch: a zero-valued success is indistinguishable
	// from a real append at offset 0.
	if count == 0 {
		return nil, newConnectError(queue.ErrorCodeInvalidArgument, errEmptyStream)
	}

	return connect.NewResponse(&queuev1.AppendBatchResponse{
		FirstOffset: firstOffset,
		LastOffset:  lastOffset,
		Count:       count,
		Timestamp:   timestamppb.New(lastTimestamp),
	}), nil
}

// --- Read Operations ---

func (h *Handler) Read(ctx context.Context, req *connect.Request[queuev1.ReadRequest]) (*connect.Response[queuev1.Message], error) {
	msg := req.Msg

	message, err := h.queueStore.Read(ctx, msg.QueueName, msg.Offset)
	if err != nil {
		if errors.Is(err, storage.ErrOffsetOutOfRange) {
			return nil, newConnectError(queue.ErrorCodeOutOfRange, err)
		}
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	protoMessage := h.messageToProto(message)
	coremessage.Release(message)
	return connect.NewResponse(protoMessage), nil
}

func (h *Handler) ReadBatch(ctx context.Context, req *connect.Request[queuev1.ReadBatchRequest]) (*connect.Response[queuev1.ReadBatchResponse], error) {
	msg := req.Msg

	limit := int(msg.Limit)
	if limit == 0 {
		limit = 100
	}

	messages, err := h.queueStore.ReadBatch(ctx, msg.QueueName, msg.StartOffset, limit)
	if err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	protoMsgs := h.messagesToProto(messages)

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
			if errors.Is(err, storage.ErrOffsetOutOfRange) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return newConnectError(queue.ErrorCodeInternal, err)
		}

		protoMessages := h.messagesToProto(messages)
		for _, m := range protoMessages {
			if err := stream.Send(m); err != nil {
				return err
			}
			offset = m.Offset + 1
		}

		if len(messages) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// --- Seek Operations ---

func (h *Handler) SeekToOffset(ctx context.Context, req *connect.Request[queuev1.SeekToOffsetRequest]) (*connect.Response[queuev1.SeekResponse], error) {
	msg := req.Msg
	if h.manager != nil {
		outcome, err := h.manager.StateMachine().Seek(ctx, queue.SeekCommand{
			QueueName: msg.QueueName,
			Kind:      queue.SeekOffset,
			Offset:    msg.Offset,
		})
		if err != nil {
			return nil, newConnectError(queue.ErrorCodeInternal, err)
		}
		return connect.NewResponse(&queuev1.SeekResponse{Offset: outcome.Offset}), nil
	}

	head, err := h.queueStore.Head(ctx, msg.QueueName)
	if err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	tail, err := h.queueStore.Tail(ctx, msg.QueueName)
	if err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
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
	if msg.Timestamp == nil {
		return nil, newConnectError(queue.ErrorCodeInvalidArgument, fmt.Errorf("timestamp is required"))
	}
	if h.manager != nil {
		outcome, err := h.manager.StateMachine().Seek(ctx, queue.SeekCommand{
			QueueName: msg.QueueName,
			Kind:      queue.SeekTimestamp,
			Timestamp: msg.Timestamp.AsTime(),
		})
		if err != nil {
			return nil, newConnectError(queue.ErrorCodeInternal, err)
		}
		return connect.NewResponse(&queuev1.SeekResponse{
			Offset:     outcome.Offset,
			Timestamp:  timestamppb.New(outcome.Timestamp),
			ExactMatch: outcome.ExactMatch,
		}), nil
	}

	head, err := h.queueStore.Head(ctx, msg.QueueName)
	if err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	tail, err := h.queueStore.Tail(ctx, msg.QueueName)
	if err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	target := msg.Timestamp.AsTime()
	offset := head
	for offset < tail {
		batch, err := h.queueStore.ReadBatch(ctx, msg.QueueName, offset, 128)
		if err != nil {
			if errors.Is(err, storage.ErrOffsetOutOfRange) {
				break
			}
			return nil, newConnectError(queue.ErrorCodeInternal, err)
		}
		if len(batch) == 0 {
			break
		}

		for _, m := range batch {
			if !m.BrokerMeta.Queue.CreatedAt.Before(target) {
				response := &queuev1.SeekResponse{
					Offset:     m.BrokerMeta.Queue.Offset,
					Timestamp:  timestamppb.New(m.BrokerMeta.Queue.CreatedAt),
					ExactMatch: m.BrokerMeta.Queue.CreatedAt.Equal(target),
				}
				releaseMessages(batch)
				return connect.NewResponse(response), nil
			}
		}

		offset = batch[len(batch)-1].BrokerMeta.Queue.Offset + 1
		releaseMessages(batch)
	}

	return connect.NewResponse(&queuev1.SeekResponse{
		Offset:    tail,
		Timestamp: timestamppb.New(target),
	}), nil
}

// --- Consumer Group Operations ---

func (h *Handler) CreateConsumerGroup(ctx context.Context, req *connect.Request[queuev1.CreateConsumerGroupRequest]) (*connect.Response[queuev1.ConsumerGroup], error) {
	msg := req.Msg

	_, err := h.queueStore.GetQueue(ctx, msg.QueueName)
	if err != nil {
		if errors.Is(err, storage.ErrQueueNotFound) {
			return nil, newConnectError(queue.ErrorCodeNotFound, err)
		}
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	head, _ := h.queueStore.Head(ctx, msg.QueueName)

	group := types.NewConsumerGroupState(msg.QueueName, msg.GroupId, "")
	group.Cursor.Cursor = head
	group.Cursor.Committed = head

	if err := h.groupStore.CreateConsumerGroup(ctx, group); err != nil {
		if errors.Is(err, storage.ErrConsumerGroupExists) {
			return nil, newConnectError(queue.ErrorCodeAlreadyExists, err)
		}
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(h.groupToProto(group)), nil
}

func (h *Handler) GetConsumerGroup(ctx context.Context, req *connect.Request[queuev1.GetConsumerGroupRequest]) (*connect.Response[queuev1.ConsumerGroup], error) {
	msg := req.Msg

	group, err := h.groupStore.GetConsumerGroup(ctx, msg.QueueName, msg.GroupId)
	if err != nil {
		if errors.Is(err, storage.ErrConsumerNotFound) {
			return nil, newConnectError(queue.ErrorCodeNotFound, err)
		}
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(h.groupToProto(group)), nil
}

func (h *Handler) ListConsumerGroups(ctx context.Context, req *connect.Request[queuev1.ListConsumerGroupsRequest]) (*connect.Response[queuev1.ListConsumerGroupsResponse], error) {
	msg := req.Msg

	groups, err := h.groupStore.ListConsumerGroups(ctx, msg.QueueName)
	if err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
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
		return nil, newConnectError(queue.ErrorCodeInternal, err)
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
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(&queuev1.JoinGroupResponse{
		GenerationId: 1,
	}), nil
}

func (h *Handler) LeaveGroup(ctx context.Context, req *connect.Request[queuev1.LeaveGroupRequest]) (*connect.Response[emptypb.Empty], error) {
	msg := req.Msg

	if err := h.groupStore.UnregisterConsumer(ctx, msg.QueueName, msg.GroupId, msg.ConsumerId); err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (h *Handler) Heartbeat(ctx context.Context, req *connect.Request[queuev1.HeartbeatRequest]) (*connect.Response[queuev1.HeartbeatResponse], error) {
	msg := req.Msg
	if h.manager != nil {
		if err := h.manager.UpdateConsumerHeartbeat(ctx, msg.QueueName, msg.GroupId, msg.ConsumerId); err != nil {
			if errors.Is(err, storage.ErrConsumerNotFound) || errors.Is(err, consumer.ErrConsumerNotFound) {
				return nil, newConnectError(queue.ErrorCodeNotFound, err)
			}
			return nil, newConnectError(queue.ErrorCodeInternal, err)
		}
		return connect.NewResponse(&queuev1.HeartbeatResponse{
			ShouldRejoin: false,
		}), nil
	}

	group, err := h.groupStore.GetConsumerGroup(ctx, msg.QueueName, msg.GroupId)
	if err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	// Through the group's lock rather than a pointer it handed out, which is
	// what let this write race the encoder that persists the group.
	c, registered := group.TouchConsumer(msg.ConsumerId, time.Now())
	if !registered {
		return nil, newConnectError(queue.ErrorCodeNotFound, fmt.Errorf("consumer not found"))
	}

	if err := h.groupStore.RegisterConsumer(ctx, msg.QueueName, msg.GroupId, &c); err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(&queuev1.HeartbeatResponse{
		ShouldRejoin: false,
	}), nil
}

// --- Consume Operations ---

// streamIdlePoll is how long ConsumeQueue waits before re-checking an empty
// queue. The streaming RPC carries no client-supplied wait: back-pressure there
// belongs in max_in_flight.
const streamIdlePoll = 100 * time.Millisecond

// consumePollInterval is how often a waiting unary Consume re-checks the queue.
const consumePollInterval = 50 * time.Millisecond

// sleepCtx waits for d, reporting false if the context ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// consumeWaiting polls until the queue yields messages or the wait elapses.
//
// A client that sets wait_time is asking not to be answered with an immediate
// empty response; returning one anyway turns a long poll into a busy loop. The
// wait is bounded by the request deadline as well, so a caller cannot hold a
// handler past the context it supplied.
func (h *Handler) consumeWaiting(ctx context.Context, command queue.ConsumeCommand, wait time.Duration) (queue.ConsumeOutcome, error) {
	outcome, err := h.manager.StateMachine().Consume(ctx, command)
	if !errors.Is(err, consumer.ErrNoMessages) || wait <= 0 {
		return outcome, err
	}

	deadline := time.Now().Add(wait)
	if requestDeadline, ok := ctx.Deadline(); ok && requestDeadline.Before(deadline) {
		deadline = requestDeadline
	}

	for time.Now().Before(deadline) {
		if !sleepCtx(ctx, min(consumePollInterval, time.Until(deadline))) {
			return queue.ConsumeOutcome{}, consumer.ErrNoMessages
		}
		outcome, err = h.manager.StateMachine().Consume(ctx, command)
		if !errors.Is(err, consumer.ErrNoMessages) {
			return outcome, err
		}
	}

	return queue.ConsumeOutcome{}, consumer.ErrNoMessages
}

func (h *Handler) Consume(ctx context.Context, req *connect.Request[queuev1.ConsumeRequest]) (*connect.Response[queuev1.ConsumeResponse], error) {
	msg := req.Msg
	if h.manager == nil {
		return nil, newConnectError(queue.ErrorCodeFailedPrecondition, fmt.Errorf("queue state machine is unavailable"))
	}
	limit := int(msg.MaxMessages)
	if limit == 0 {
		limit = 10
	}
	command := queue.ConsumeCommand{
		QueueName:  msg.QueueName,
		GroupID:    msg.GroupId,
		ConsumerID: msg.ConsumerId,
		Limit:      limit,
	}

	outcome, err := h.consumeWaiting(ctx, command, msg.WaitTime.AsDuration())
	if errors.Is(err, consumer.ErrNoMessages) {
		return connect.NewResponse(&queuev1.ConsumeResponse{}), nil
	}
	if err != nil {
		releaseMessages(outcome.Messages)
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}
	protoMsgs := h.messagesToProto(outcome.Messages)
	if outcome.CommitRequired {
		if err := h.manager.StateMachine().CommitConsume(ctx, queue.CommitConsumeCommand{
			QueueName: msg.QueueName,
			GroupID:   msg.GroupId,
			Offset:    outcome.NextOffset,
		}); err != nil {
			return nil, newConnectError(queue.ErrorCodeInternal, err)
		}
	}
	return connect.NewResponse(&queuev1.ConsumeResponse{
		Messages: protoMsgs,
	}), nil
}

func (h *Handler) ConsumeQueue(ctx context.Context, req *connect.Request[queuev1.ConsumeQueueRequest], stream *connect.ServerStream[queuev1.Message]) error {
	msg := req.Msg
	if h.manager == nil {
		return newConnectError(queue.ErrorCodeFailedPrecondition, fmt.Errorf("queue state machine is unavailable"))
	}
	limit := int(msg.MaxInFlight)
	if limit == 0 {
		limit = 10
	}

	for {
		// Checked before each round rather than only when idle: a stream with a
		// steady supply of messages never reaches the idle branch, so a client
		// that went away was noticed only when a Send eventually failed.
		if err := ctx.Err(); err != nil {
			return nil
		}

		outcome, err := h.manager.StateMachine().Consume(ctx, queue.ConsumeCommand{
			QueueName:  msg.QueueName,
			GroupID:    msg.GroupId,
			ConsumerID: msg.ConsumerId,
			Limit:      limit,
		})
		if errors.Is(err, consumer.ErrNoMessages) {
			if !sleepCtx(ctx, streamIdlePoll) {
				return nil
			}
			continue
		}
		if err != nil {
			releaseMessages(outcome.Messages)
			return newConnectError(queue.ErrorCodeInternal, err)
		}
		protoMessages := h.messagesToProto(outcome.Messages)
		for _, message := range protoMessages {
			if err := stream.Send(message); err != nil {
				return err
			}
		}
		if outcome.CommitRequired {
			if err := h.manager.StateMachine().CommitConsume(ctx, queue.CommitConsumeCommand{
				QueueName: msg.QueueName,
				GroupID:   msg.GroupId,
				Offset:    outcome.NextOffset,
			}); err != nil {
				return newConnectError(queue.ErrorCodeInternal, err)
			}
		}
	}
}

func (h *Handler) Ack(ctx context.Context, req *connect.Request[queuev1.AckRequest]) (*connect.Response[queuev1.AckResponse], error) {
	msg := req.Msg
	if h.manager == nil {
		return nil, newConnectError(queue.ErrorCodeFailedPrecondition, fmt.Errorf("queue state machine is unavailable"))
	}
	if msg.GroupId == "" {
		return nil, newConnectError(queue.ErrorCodeInvalidArgument, errSettlementGroupRequired)
	}
	outcome, err := h.manager.StateMachine().Ack(ctx, queue.AckCommand{
		QueueName:  msg.QueueName,
		GroupID:    msg.GroupId,
		ConsumerID: msg.ConsumerId,
		Offsets:    msg.Offsets,
	})
	if err != nil {
		// A multi-offset settlement stops at its first failure. Report the prefix
		// it did settle so the client re-acknowledges only what remains.
		return nil, newConnectErrorWithProgress(queue.ErrorCodeInternal, err,
			settlementProgress(outcome, failedOffset(msg.Offsets, len(outcome.Offsets))))
	}
	return connect.NewResponse(&queuev1.AckResponse{
		AckedCount: uint32(len(outcome.Offsets)),
		Committed: &queuev1.QueueCursor{
			Cursor:    outcome.Cursor,
			Committed: outcome.Committed,
		},
	}), nil
}

func (h *Handler) Nack(ctx context.Context, req *connect.Request[queuev1.NackRequest]) (*connect.Response[emptypb.Empty], error) {
	msg := req.Msg
	if h.manager == nil {
		return nil, newConnectError(queue.ErrorCodeFailedPrecondition, fmt.Errorf("queue state machine is unavailable"))
	}
	if msg.GroupId == "" {
		return nil, newConnectError(queue.ErrorCodeInvalidArgument, errSettlementGroupRequired)
	}
	delay := time.Duration(0)
	if msg.Delay != nil {
		delay = msg.Delay.AsDuration()
	}
	outcome, err := h.manager.StateMachine().Nack(ctx, queue.NackCommand{
		QueueName:  msg.QueueName,
		GroupID:    msg.GroupId,
		ConsumerID: msg.ConsumerId,
		Offsets:    msg.Offsets,
		Delay:      delay,
	})
	if err != nil {
		return nil, newConnectErrorWithProgress(queue.ErrorCodeInternal, err,
			settlementProgress(outcome, failedOffset(msg.Offsets, len(outcome.Offsets))))
	}
	return connect.NewResponse(&emptypb.Empty{}), nil
}

func (h *Handler) Claim(ctx context.Context, req *connect.Request[queuev1.ClaimRequest]) (*connect.Response[queuev1.ClaimResponse], error) {
	msg := req.Msg
	if h.manager == nil {
		return nil, newConnectError(queue.ErrorCodeFailedPrecondition, fmt.Errorf("queue state machine is unavailable"))
	}
	limit := int(msg.Limit)
	if limit == 0 {
		limit = 10
	}
	minIdleTime := time.Duration(0)
	if msg.MinIdleTime != nil {
		minIdleTime = msg.MinIdleTime.AsDuration()
	}
	outcome, err := h.manager.StateMachine().Claim(ctx, queue.ClaimCommand{
		QueueName:  msg.QueueName,
		GroupID:    msg.GroupId,
		ConsumerID: msg.ConsumerId,
		MinIdle:    minIdleTime,
		Limit:      limit,
	})
	if errors.Is(err, consumer.ErrNoMessages) {
		return connect.NewResponse(&queuev1.ClaimResponse{}), nil
	}
	if err != nil {
		releaseMessages(outcome.Messages)
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}
	claimed := h.messagesToProto(outcome.Messages)
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
		return nil, newConnectError(queue.ErrorCodeInternal, err)
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
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	tail, err := h.queueStore.Tail(ctx, msg.QueueName)
	if err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
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
	if err := h.queueStore.Truncate(ctx, msg.QueueName, tail); err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(&queuev1.PurgeResponse{
		MessagesDeleted: count,
	}), nil
}

func (h *Handler) Truncate(ctx context.Context, req *connect.Request[queuev1.TruncateRequest]) (*connect.Response[emptypb.Empty], error) {
	msg := req.Msg

	if err := h.queueStore.Truncate(ctx, msg.QueueName, msg.MinOffset); err != nil {
		return nil, newConnectError(queue.ErrorCodeInternal, err)
	}

	return connect.NewResponse(&emptypb.Empty{}), nil
}

// --- Helper Functions ---

func (h *Handler) queueToProto(config *types.QueueConfig) *queuev1.Queue {
	retentionMaxAge := config.Retention.RetentionTime
	if retentionMaxAge == 0 {
		retentionMaxAge = config.MessageTTL
	}

	replication := &queuev1.ReplicationConfig{
		Enabled:           config.Replication.Enabled,
		ReplicationFactor: clampIntToUint32(config.Replication.ReplicationFactor),
		Mode:              replicationModeToProto(config.Replication.Mode),
		MinInSyncReplicas: clampIntToUint32(config.Replication.MinInSyncReplicas),
		AckTimeout:        durationpb.New(config.Replication.AckTimeout),
		Group:             config.Replication.Group,
	}
	if config.Replication.HeartbeatTimeout > 0 {
		replication.HeartbeatTimeout = durationpb.New(config.Replication.HeartbeatTimeout)
	}
	if config.Replication.ElectionTimeout > 0 {
		replication.ElectionTimeout = durationpb.New(config.Replication.ElectionTimeout)
	}
	if config.Replication.SnapshotInterval > 0 {
		replication.SnapshotInterval = durationpb.New(config.Replication.SnapshotInterval)
	}
	replication.SnapshotThreshold = config.Replication.SnapshotThreshold

	return &queuev1.Queue{
		Name:   config.Name,
		Topics: config.Topics,
		Config: &queuev1.QueueConfig{
			Retention: &queuev1.RetentionConfig{
				MaxAge:      durationpb.New(retentionMaxAge),
				MaxBytes:    clampInt64ToUint64(config.Retention.RetentionBytes),
				MinMessages: clampInt64ToUint64(config.Retention.RetentionMessages),
			},
			MaxMessageSize: clampInt64ToUint32(config.MaxMessageSize),
			Replication:    replication,
		},
	}
}

func applyQueueConfigUpdateFromProto(config *types.QueueConfig, cfg *queuev1.QueueConfig) {
	if config == nil || cfg == nil {
		return
	}

	if cfg.Retention != nil {
		if cfg.Retention.MaxAge != nil {
			maxAge := cfg.Retention.MaxAge.AsDuration()
			config.MessageTTL = maxAge
			config.Retention.RetentionTime = maxAge
		}
		if cfg.Retention.MaxBytes > 0 {
			config.Retention.RetentionBytes = int64(cfg.Retention.MaxBytes)
		}
		if cfg.Retention.MinMessages > 0 {
			config.Retention.RetentionMessages = int64(cfg.Retention.MinMessages)
		}
	}

	if cfg.MaxMessageSize > 0 {
		config.MaxMessageSize = int64(cfg.MaxMessageSize)
	}

	if cfg.Replication != nil {
		replication := config.Replication
		replication.Enabled = cfg.Replication.Enabled

		if cfg.Replication.ReplicationFactor > 0 {
			replication.ReplicationFactor = int(cfg.Replication.ReplicationFactor)
		}
		if cfg.Replication.MinInSyncReplicas > 0 {
			replication.MinInSyncReplicas = int(cfg.Replication.MinInSyncReplicas)
		}
		if cfg.Replication.AckTimeout != nil {
			replication.AckTimeout = cfg.Replication.AckTimeout.AsDuration()
		}

		switch cfg.Replication.Mode {
		case queuev1.ReplicationMode_REPLICATION_MODE_ASYNC:
			replication.Mode = types.ReplicationAsync
		case queuev1.ReplicationMode_REPLICATION_MODE_SYNC:
			replication.Mode = types.ReplicationSync
		}

		if cfg.Replication.HeartbeatTimeout != nil {
			replication.HeartbeatTimeout = cfg.Replication.HeartbeatTimeout.AsDuration()
		}
		if cfg.Replication.ElectionTimeout != nil {
			replication.ElectionTimeout = cfg.Replication.ElectionTimeout.AsDuration()
		}
		if cfg.Replication.SnapshotInterval != nil {
			replication.SnapshotInterval = cfg.Replication.SnapshotInterval.AsDuration()
		}
		if cfg.Replication.SnapshotThreshold > 0 {
			replication.SnapshotThreshold = cfg.Replication.SnapshotThreshold
		}
		replication.Group = strings.TrimSpace(cfg.Replication.Group)

		config.Replication = replication
	}
}

func clampInt64ToUint64(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}

func clampInt64ToUint32(value int64) uint32 {
	if value <= 0 {
		return 0
	}
	if value > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(value)
}

func clampIntToUint32(value int) uint32 {
	if value <= 0 {
		return 0
	}
	if value > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(value)
}

func replicationModeToProto(mode types.ReplicationMode) queuev1.ReplicationMode {
	switch mode {
	case types.ReplicationAsync:
		return queuev1.ReplicationMode_REPLICATION_MODE_ASYNC
	case types.ReplicationSync:
		fallthrough
	default:
		return queuev1.ReplicationMode_REPLICATION_MODE_SYNC
	}
}

func (h *Handler) messageToProto(msg *coremessage.Envelope) *queuev1.Message {
	protoMsg := &queuev1.Message{
		Offset:    msg.BrokerMeta.Queue.Offset,
		Timestamp: timestamppb.New(msg.BrokerMeta.Queue.CreatedAt),
		Key:       bytes.Clone(msg.PublisherMeta.Key),
		Value:     msg.StablePayload(),
	}

	if len(msg.PublisherMeta.Headers) > 0 || len(msg.PublisherMeta.Properties) > 0 {
		protoMsg.Headers = make(map[string][]byte, len(msg.PublisherMeta.Headers)+len(msg.PublisherMeta.Properties))
		for k, v := range msg.PublisherMeta.Properties {
			protoMsg.Headers[k] = []byte(v)
		}
		for k, v := range msg.PublisherMeta.Headers {
			protoMsg.Headers[k] = bytes.Clone(v)
		}
	}

	return protoMsg
}

func (h *Handler) messagesToProto(messages []*coremessage.Envelope) []*queuev1.Message {
	converted := make([]*queuev1.Message, len(messages))
	for i, msg := range messages {
		converted[i] = h.messageToProto(msg)
		coremessage.Release(msg)
	}
	return converted
}

func releaseMessages(messages []*coremessage.Envelope) {
	for _, msg := range messages {
		coremessage.Release(msg)
	}
}

func (h *Handler) groupToProto(group *types.ConsumerGroup) *queuev1.ConsumerGroup {
	consumers := make([]*queuev1.ConsumerInfo, 0, len(group.Consumers))
	for _, c := range group.Consumers {
		consumers = append(consumers, &queuev1.ConsumerInfo{
			ConsumerId:    c.ID,
			LastHeartbeat: timestamppb.New(c.LastHeartbeat),
		})
	}

	cursor := group.CursorView()
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

// appendEnvelope builds the envelope one Connect append request names. The
// command borrows it, so the caller releases it once the append returns.
func appendEnvelope(queueName string, value, key []byte, headers map[string][]byte) *message.Envelope {
	envelope := message.New(queueName, value)
	envelope.PublisherMeta.Key = key
	envelope.PublisherMeta.Headers = headers
	return envelope
}
