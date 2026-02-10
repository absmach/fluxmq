// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
)

// RemoteRouter is the subset of cluster.Cluster needed by the delivery engine
// for cross-node message routing.
type RemoteRouter interface {
	ListQueueConsumers(ctx context.Context, queueName string) ([]*cluster.QueueConsumerInfo, error)
	RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName string, msg *cluster.QueueMessage) error
}

// DeliveryEngine claims messages from queues and routes them to local or
// remote consumers. It owns the scheduling loop and delivery state; the
// Manager delegates all delivery work here.
type DeliveryEngine struct {
	queueStore      storage.QueueStore
	groupStore      storage.ConsumerGroupStore
	consumerManager *consumer.Manager
	local           Deliverer
	remote          RemoteRouter // nil for single-node
	localNodeID     string
	distributionMode DistributionMode
	batchSize       int
	logger          *slog.Logger

	mu       sync.Mutex
	enqueued map[string]struct{}
	queue    chan string

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewDeliveryEngine creates a delivery engine. remote may be nil for
// single-node deployments.
func NewDeliveryEngine(
	queueStore storage.QueueStore,
	groupStore storage.ConsumerGroupStore,
	consumerMgr *consumer.Manager,
	local Deliverer,
	remote RemoteRouter,
	localNodeID string,
	distributionMode DistributionMode,
	batchSize int,
	logger *slog.Logger,
) *DeliveryEngine {
	return &DeliveryEngine{
		queueStore:       queueStore,
		groupStore:       groupStore,
		consumerManager:  consumerMgr,
		local:            local,
		remote:           remote,
		localNodeID:      localNodeID,
		distributionMode: distributionMode,
		batchSize:        batchSize,
		logger:           logger,
		enqueued:         make(map[string]struct{}),
		queue:            make(chan string, 4096),
		stopCh:           make(chan struct{}),
	}
}

// Start launches the delivery loop goroutine.
func (e *DeliveryEngine) Start() {
	e.wg.Add(1)
	go e.run()
}

// Stop signals the delivery loop to exit and waits for it to finish.
func (e *DeliveryEngine) Stop() {
	close(e.stopCh)
	e.wg.Wait()
}

// Schedule enqueues a queue name for delivery. Duplicate schedules for the
// same queue are coalesced until the queue is delivered.
func (e *DeliveryEngine) Schedule(queueName string) {
	if queueName == "" {
		return
	}

	e.mu.Lock()
	if _, exists := e.enqueued[queueName]; exists {
		e.mu.Unlock()
		return
	}
	e.enqueued[queueName] = struct{}{}
	e.mu.Unlock()

	select {
	case e.queue <- queueName:
	default:
		e.logger.Warn("delivery channel full, dropping trigger (will retry on next sweep)",
			slog.String("queue", queueName))
		e.markDelivered(queueName)
	}
}

// ScheduleAll lists all queues and schedules each for delivery.
func (e *DeliveryEngine) ScheduleAll(ctx context.Context) {
	queues, err := e.queueStore.ListQueues(ctx)
	if err != nil {
		return
	}
	for _, queueConfig := range queues {
		e.Schedule(queueConfig.Name)
	}
}

// Unschedule removes a queue from the dedup set. Called when a queue is deleted.
func (e *DeliveryEngine) Unschedule(queueName string) {
	e.markDelivered(queueName)
}

// DeliverAll delivers messages for every queue (full sweep). Intended for
// tests and benchmarks that need synchronous delivery without the loop.
func (e *DeliveryEngine) DeliverAll(ctx context.Context) {
	queues, err := e.queueStore.ListQueues(ctx)
	if err != nil {
		return
	}
	for i := range queues {
		e.deliverQueueConfig(ctx, &queues[i])
	}
}

// DeliverQueue delivers messages for a single queue by name. Returns true if
// any messages were delivered.
func (e *DeliveryEngine) DeliverQueue(ctx context.Context, queueName string) bool {
	if queueName == "" {
		return false
	}
	queueConfig, err := e.queueStore.GetQueue(ctx, queueName)
	if err != nil {
		return false
	}
	return e.deliverQueueConfig(ctx, queueConfig)
}

func (e *DeliveryEngine) markDelivered(queueName string) {
	e.mu.Lock()
	delete(e.enqueued, queueName)
	e.mu.Unlock()
}

func (e *DeliveryEngine) run() {
	defer e.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case queueName := <-e.queue:
			e.markDelivered(queueName)
			if e.DeliverQueue(context.Background(), queueName) {
				e.Schedule(queueName)
			}
		case <-ticker.C:
			e.ScheduleAll(context.Background())
		}
	}
}

func (e *DeliveryEngine) deliverQueueConfig(ctx context.Context, queueConfig *types.QueueConfig) bool {
	if queueConfig == nil {
		return false
	}

	delivered := false
	primaryGroup := strings.TrimSpace(queueConfig.PrimaryGroup)
	primaryCommitted := make(map[string]uint64)
	getPrimaryCommitted := func(pattern string) (uint64, bool) {
		if primaryGroup == "" {
			return 0, false
		}

		patternGroupID := primaryGroup
		if pattern != "" {
			patternGroupID = fmt.Sprintf("%s@%s", primaryGroup, pattern)
		}

		if val, ok := primaryCommitted[patternGroupID]; ok {
			return val, true
		}

		committed, err := e.consumerManager.GetCommittedOffset(ctx, queueConfig.Name, patternGroupID)
		if err != nil {
			return 0, false
		}

		primaryCommitted[patternGroupID] = committed
		return committed, true
	}

	groups, err := e.groupStore.ListConsumerGroups(ctx, queueConfig.Name)
	if err == nil {
		for _, group := range groups {
			if e.deliverToGroup(ctx, queueConfig, group, getPrimaryCommitted) {
				delivered = true
			}
		}
	}

	if e.remote != nil && e.distributionMode == DistributionForward {
		if e.deliverToRemoteConsumers(ctx, queueConfig) {
			delivered = true
		}
	}

	return delivered
}

func (e *DeliveryEngine) deliverToGroup(ctx context.Context, config *types.QueueConfig, group *types.ConsumerGroup, primaryCommitted func(pattern string) (uint64, bool)) bool {
	if group.ConsumerCount() == 0 {
		return false
	}

	var filter *consumer.Filter
	if group.Pattern != "" {
		filter = consumer.NewFilter(group.Pattern)
	}

	consumers := group.ConsumerIDs()
	if len(consumers) == 0 {
		return false
	}

	delivered := false
	for _, consumerID := range consumers {
		var msgs []*types.Message
		var err error
		if group.Mode == types.GroupModeStream {
			msgs, err = e.consumerManager.ClaimBatchStream(ctx, config.Name, group.ID, consumerID, filter, e.batchSize)
		} else {
			msgs, err = e.consumerManager.ClaimBatch(ctx, config.Name, group.ID, consumerID, filter, e.batchSize)
		}
		if err != nil {
			continue
		}
		if len(msgs) > 0 {
			delivered = true
		}

		freshGroup, err := e.groupStore.GetConsumerGroup(ctx, config.Name, group.ID)
		if err != nil {
			continue
		}

		consumerInfo := freshGroup.GetConsumer(consumerID)
		if consumerInfo == nil {
			continue
		}

		if len(msgs) > 0 {
			e.consumerManager.UpdateHeartbeat(ctx, config.Name, group.ID, consumerID)
		}

		var workCommitted uint64
		var hasWorkCommitted bool
		if group.Mode == types.GroupModeStream && primaryCommitted != nil {
			workCommitted, hasWorkCommitted = primaryCommitted(group.Pattern)
		}

		for _, msg := range msgs {
			if e.remote != nil && consumerInfo.ProxyNodeID != "" && consumerInfo.ProxyNodeID != e.localNodeID {
				routeMsg := createRoutedQueueMessage(
					msg,
					group.ID,
					config.Name,
					group.Mode == types.GroupModeStream,
					workCommitted,
					hasWorkCommitted,
					config.PrimaryGroup,
				)
				err := e.remote.RouteQueueMessage(
					ctx,
					consumerInfo.ProxyNodeID,
					consumerInfo.ClientID,
					config.Name,
					routeMsg,
				)
				if err != nil {
					e.logger.Warn("queue message remote routing failed",
						slog.String("client", consumerInfo.ClientID),
						slog.String("node", consumerInfo.ProxyNodeID),
						slog.String("topic", msg.Topic),
						slog.String("error", err.Error()))
				}
			} else if e.local != nil {
				deliveryMsg := createDeliveryMessage(msg, group.ID, config.Name)
				if group.Mode == types.GroupModeStream {
					decorateStreamDelivery(deliveryMsg, msg, workCommitted, hasWorkCommitted, config.PrimaryGroup)
				}

				if err := e.local.Deliver(ctx, consumerInfo.ClientID, deliveryMsg); err != nil {
					e.logger.Warn("queue message delivery failed",
						slog.String("client", consumerInfo.ClientID),
						slog.String("topic", msg.Topic),
						slog.String("error", err.Error()))
				}
			}
		}
	}

	return delivered
}

func (e *DeliveryEngine) deliverToRemoteConsumers(ctx context.Context, config *types.QueueConfig) bool {
	consumers, err := e.remote.ListQueueConsumers(ctx, config.Name)
	if err != nil {
		e.logger.Debug("failed to list cluster consumers",
			slog.String("queue", config.Name),
			slog.String("error", err.Error()))
		return false
	}

	consumersByGroup := make(map[string][]*cluster.QueueConsumerInfo)
	for _, c := range consumers {
		if c.ProxyNodeID == e.localNodeID {
			continue
		}
		consumersByGroup[c.GroupID] = append(consumersByGroup[c.GroupID], c)
	}

	delivered := false
	for groupID, groupConsumers := range consumersByGroup {
		mode := types.GroupModeQueue
		if groupConsumers[0].Mode != "" {
			mode = types.ConsumerGroupMode(groupConsumers[0].Mode)
		}
		group, err := e.consumerManager.GetOrCreateGroup(ctx, config.Name, groupID, groupConsumers[0].Pattern, mode, true)
		if err != nil {
			continue
		}

		var filter *consumer.Filter
		if group.Pattern != "" {
			filter = consumer.NewFilter(group.Pattern)
		}

		var workCommitted uint64
		var hasWorkCommitted bool
		if group.Mode == types.GroupModeStream && config.PrimaryGroup != "" {
			patternGroupID := config.PrimaryGroup
			if group.Pattern != "" {
				patternGroupID = fmt.Sprintf("%s@%s", config.PrimaryGroup, group.Pattern)
			}
			if committed, err := e.consumerManager.GetCommittedOffset(ctx, config.Name, patternGroupID); err == nil {
				workCommitted = committed
				hasWorkCommitted = true
			}
		}

		for _, consumerInfo := range groupConsumers {
			var msgs []*types.Message
			var err error
			if group.Mode == types.GroupModeStream {
				msgs, err = e.consumerManager.ClaimBatchStream(ctx, config.Name, groupID, consumerInfo.ConsumerID, filter, e.batchSize)
			} else {
				msgs, err = e.consumerManager.ClaimBatch(ctx, config.Name, groupID, consumerInfo.ConsumerID, filter, e.batchSize)
			}
			if err != nil {
				continue
			}
			if len(msgs) > 0 {
				delivered = true
			}

			for _, msg := range msgs {
				routeMsg := createRoutedQueueMessage(
					msg,
					groupID,
					config.Name,
					group.Mode == types.GroupModeStream,
					workCommitted,
					hasWorkCommitted,
					config.PrimaryGroup,
				)

				err := e.remote.RouteQueueMessage(
					ctx,
					consumerInfo.ProxyNodeID,
					consumerInfo.ClientID,
					config.Name,
					routeMsg,
				)
				if err != nil {
					e.logger.Warn("remote queue message delivery failed",
						slog.String("client", consumerInfo.ClientID),
						slog.String("node", consumerInfo.ProxyNodeID),
						slog.String("queue", config.Name),
						slog.String("error", err.Error()))
				} else {
					e.logger.Debug("routed queue message to remote consumer",
						slog.String("client", consumerInfo.ClientID),
						slog.String("node", consumerInfo.ProxyNodeID),
						slog.String("queue", config.Name),
						slog.Uint64("offset", msg.Sequence))
				}
			}
		}
	}

	return delivered
}

// --- Message building helpers (stateless) ---

func createDeliveryMessage(msg *types.Message, groupID string, queueName string) *brokerstorage.Message {
	props := createRouteProperties(msg, groupID, queueName)

	deliveryMsg := &brokerstorage.Message{
		Topic:      msg.Topic,
		QoS:        1,
		Properties: props,
	}
	deliveryMsg.SetPayloadFromBytes(msg.GetPayload())

	return deliveryMsg
}

func decorateStreamDelivery(delivery *brokerstorage.Message, msg *types.Message, workCommitted uint64, hasWorkCommitted bool, primaryGroup string) {
	if delivery == nil || msg == nil {
		return
	}
	if delivery.Properties == nil {
		delivery.Properties = make(map[string]string)
	}
	decorateStreamProperties(delivery.Properties, msg, workCommitted, hasWorkCommitted, primaryGroup)
}

func createRouteProperties(msg *types.Message, groupID, queueName string) map[string]string {
	props := make(map[string]string, len(msg.Properties)+4)
	for k, v := range msg.Properties {
		props[k] = v
	}
	props[types.PropMessageID] = fmt.Sprintf("%s:%d", queueName, msg.Sequence)
	props[types.PropGroupID] = groupID
	props[types.PropQueueName] = queueName
	props[types.PropOffset] = fmt.Sprintf("%d", msg.Sequence)

	return props
}

func createRoutedQueueMessage(msg *types.Message, groupID, queueName string, stream bool, workCommitted uint64, hasWorkCommitted bool, primaryGroup string) *cluster.QueueMessage {
	userProps := make(map[string]string, len(msg.Properties))
	for k, v := range msg.Properties {
		userProps[k] = v
	}

	routeMsg := &cluster.QueueMessage{
		MessageID:      fmt.Sprintf("%s:%d", queueName, msg.Sequence),
		QueueName:      queueName,
		GroupID:        groupID,
		Payload:        msg.GetPayload(),
		Sequence:       int64(msg.Sequence),
		UserProperties: userProps,
		Stream:         stream,
	}

	if stream {
		routeMsg.StreamOffset = int64(msg.Sequence)
		if !msg.CreatedAt.IsZero() {
			routeMsg.StreamTimestamp = msg.CreatedAt.UnixMilli()
		}
		if hasWorkCommitted {
			routeMsg.HasWorkCommitted = true
			routeMsg.WorkCommittedOffset = int64(workCommitted)
			routeMsg.WorkAcked = msg.Sequence < workCommitted
			routeMsg.WorkGroup = primaryGroup
		}
	}

	return routeMsg
}

func decorateStreamProperties(properties map[string]string, msg *types.Message, workCommitted uint64, hasWorkCommitted bool, primaryGroup string) {
	if properties == nil || msg == nil {
		return
	}

	properties[types.PropStreamOffset] = fmt.Sprintf("%d", msg.Sequence)
	if !msg.CreatedAt.IsZero() {
		properties[types.PropStreamTimestamp] = fmt.Sprintf("%d", msg.CreatedAt.UnixMilli())
	}

	if hasWorkCommitted {
		properties[types.PropWorkCommittedOffset] = fmt.Sprintf("%d", workCommitted)
		properties[types.PropWorkAcked] = strconv.FormatBool(msg.Sequence < workCommitted)
		if primaryGroup != "" {
			properties[types.PropWorkGroup] = primaryGroup
		}
	}
}
