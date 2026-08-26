// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// RemoteRouter is the subset of cluster.Cluster needed by the delivery engine
// for cross-node message routing.
type RemoteRouter interface {
	ListQueueConsumers(ctx context.Context, queueName string) ([]*cluster.QueueConsumerInfo, error)
	RouteQueueMessage(ctx context.Context, nodeID, clientID string, msg *message.Envelope) error
	UnregisterQueueConsumer(ctx context.Context, queueName, groupID, consumerID string) error
}

type RemoteBatchRouter interface {
	RouteQueueBatch(ctx context.Context, nodeID string, deliveries []cluster.QueueDelivery) error
}

// DeliveryEngine claims messages from queues and routes them to local or
// remote consumers. It owns the scheduling loop and delivery state; the
// Manager delegates all delivery work here.
type DeliveryEngine struct {
	queueStore        storage.QueueStore
	groupStore        storage.ConsumerGroupStore
	consumerManager   *consumer.Manager
	stateMachine      *stateMachine
	local             Deliverer
	remote            RemoteRouter // nil for single-node
	localNodeID       string
	distributionMode  DistributionMode
	batchSize         int
	logger            *slog.Logger
	onConsumerRemoved func(context.Context, string, string, []string)

	schedule *deliveryQueue

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewDeliveryEngine creates a delivery engine. remote may be nil for
// single-node deployments.
func NewDeliveryEngine(
	machine *stateMachine,
	schedule *deliveryQueue,
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
		stateMachine:     machine,
		local:            local,
		remote:           remote,
		localNodeID:      localNodeID,
		distributionMode: distributionMode,
		batchSize:        batchSize,
		logger:           logger,
		schedule:         schedule,
		stopCh:           make(chan struct{}),
	}
}

func (e *DeliveryEngine) setConsumerRemovedCallback(callback func(context.Context, string, string, []string)) {
	e.onConsumerRemoved = callback
}

// Start launches the delivery loop goroutine.
func (e *DeliveryEngine) Start(ctx context.Context) {
	e.wg.Add(1)
	go e.run(ctx) //nolint:contextcheck // goroutine manages its own delivery lifecycle; ctx is stored for cancellation propagation
}

// Stop signals the delivery loop to exit and waits for it to finish.
func (e *DeliveryEngine) Stop() {
	close(e.stopCh)
	e.wg.Wait()
}

// Schedule enqueues a queue name for delivery. Duplicate schedules for the
// same queue are coalesced until the queue is delivered.
func (e *DeliveryEngine) Schedule(queueName string) {
	e.schedule.Schedule(queueName)
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
	e.schedule.markDelivered(queueName)
}

func (e *DeliveryEngine) run(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case queueName := <-e.schedule.pending():
			e.markDelivered(queueName)
			if e.DeliverQueue(ctx, queueName) {
				e.Schedule(queueName)
			}
		case <-ticker.C:
			e.ScheduleAll(ctx)
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

		patternGroupID := corebroker.EffectiveConsumerGroupID(primaryGroup, pattern)

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

	consumers := group.ConsumerIDs()
	if len(consumers) == 0 {
		return false
	}

	delivered := false
	for _, consumerID := range consumers {
		freshGroup, err := e.groupStore.GetConsumerGroup(ctx, config.Name, group.ID)
		if err != nil {
			continue
		}

		consumerInfo, registered := freshGroup.GetConsumer(consumerID)
		if !registered {
			continue
		}

		remoteTarget := e.isRemoteConsumer(&consumerInfo)
		if !remoteTarget {
			if e.local == nil {
				continue
			}
			if e.localDeliveryTargetMissing(consumerInfo.ClientID) {
				e.unregisterConsumer(ctx, config.Name, group.ID, consumerID,
					corebroker.ErrClientNotConnected)
				continue
			}
		}

		outcome, err := e.stateMachine.Consume(ctx, ConsumeCommand{
			QueueName:  config.Name,
			GroupID:    group.ID,
			ConsumerID: consumerID,
			Filter:     group.Pattern,
			Limit:      e.batchSize,
		})
		if errors.Is(err, consumer.ErrNoMessages) {
			releaseDeliverySources(outcome.Messages)
			e.touchConsumerHeartbeat(ctx, config.Name, group.ID, consumerID)
			continue
		}
		if err != nil {
			releaseDeliverySources(outcome.Messages)
			continue
		}
		msgs := outcome.Messages
		nextCursor := outcome.NextOffset
		if len(msgs) == 0 {
			e.touchConsumerHeartbeat(ctx, config.Name, group.ID, consumerID)
			continue
		}

		var workCommitted uint64
		var hasWorkCommitted bool
		if group.Mode == types.GroupModeStream && primaryCommitted != nil {
			workCommitted, hasWorkCommitted = primaryCommitted(group.Pattern)
		}

		if remoteTarget {
			deliveries := make([]cluster.QueueDelivery, 0, len(msgs))
			for _, msg := range msgs {
				deliveries = append(deliveries, cluster.QueueDelivery{
					ClientID: consumerInfo.ClientID,
					Message: createRoutedQueueMessage(
						msg,
						group.ID,
						config.Name,
						group.Mode == types.GroupModeStream,
						workCommitted,
						hasWorkCommitted,
						config.PrimaryGroup,
					),
				})
			}
			releaseDeliverySources(msgs)
			if err := e.routeRemoteBatch(ctx, consumerInfo.ProxyNodeID, deliveries); err != nil {
				e.logger.Warn("queue message remote routing failed",
					slog.String("client", consumerInfo.ClientID),
					slog.String("node", consumerInfo.ProxyNodeID),
					slog.String("queue", config.Name),
					slog.Int("batch_size", len(deliveries)),
					slog.String("error", err.Error()))
				if corebroker.IsErrClientNotConnected(err) {
					e.unregisterConsumer(ctx, config.Name, group.ID, consumerID, err)
				}
				continue
			}
			if group.Mode == types.GroupModeStream {
				e.commitStreamCursor(ctx, config.Name, group.ID, nextCursor)
			}
			e.touchConsumerHeartbeat(ctx, config.Name, group.ID, consumerID)
			delivered = true
			continue
		}

		var (
			committedCursor uint64
			deliveredAny    bool
			allDelivered    = true
		)
		for _, msg := range msgs {
			if e.local != nil {
				deliveryMsg := createDeliveryMessage(msg, group.ID, config.Name)
				if group.Mode == types.GroupModeStream {
					decorateStreamDelivery(deliveryMsg, msg, workCommitted, hasWorkCommitted, config.PrimaryGroup)
				}

				if err := e.local.Deliver(ctx, consumerInfo.ClientID, deliveryMsg); err != nil {
					allDelivered = false
					e.logger.Warn("queue message delivery failed",
						slog.String("client", consumerInfo.ClientID),
						slog.String("topic", msg.Topic),
						slog.String("error", err.Error()))
					if corebroker.IsErrClientNotConnected(err) {
						e.unregisterConsumer(ctx, config.Name, group.ID, consumerID, err)
						break
					}
					if group.Mode == types.GroupModeStream {
						break
					}
					continue
				}
				deliveredAny = true
				delivered = true
				if group.Mode == types.GroupModeStream {
					committedCursor = msg.BrokerMeta.Queue.Offset + 1
				}
			}
		}
		releaseDeliverySources(msgs)

		if group.Mode == types.GroupModeStream && deliveredAny {
			if allDelivered {
				committedCursor = nextCursor
			}
			e.commitStreamCursor(ctx, config.Name, group.ID, committedCursor)
		}
		if deliveredAny {
			e.touchConsumerHeartbeat(ctx, config.Name, group.ID, consumerID)
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

		var workCommitted uint64
		var hasWorkCommitted bool
		if group.Mode == types.GroupModeStream && config.PrimaryGroup != "" {
			patternGroupID := corebroker.EffectiveConsumerGroupID(config.PrimaryGroup, group.Pattern)
			if committed, err := e.consumerManager.GetCommittedOffset(ctx, config.Name, patternGroupID); err == nil {
				workCommitted = committed
				hasWorkCommitted = true
			}
		}

		for _, consumerInfo := range groupConsumers {
			outcome, err := e.stateMachine.Consume(ctx, ConsumeCommand{
				QueueName:  config.Name,
				GroupID:    groupID,
				ConsumerID: consumerInfo.ConsumerID,
				Filter:     group.Pattern,
				Limit:      e.batchSize,
			})
			if err != nil {
				releaseDeliverySources(outcome.Messages)
				continue
			}
			msgs := outcome.Messages
			nextCursor := outcome.NextOffset

			if len(msgs) == 0 {
				continue
			}

			deliveries := make([]cluster.QueueDelivery, 0, len(msgs))
			for _, msg := range msgs {
				deliveries = append(deliveries, cluster.QueueDelivery{
					ClientID: consumerInfo.ClientID,
					Message: createRoutedQueueMessage(
						msg,
						groupID,
						config.Name,
						group.Mode == types.GroupModeStream,
						workCommitted,
						hasWorkCommitted,
						config.PrimaryGroup,
					),
				})
			}
			lastOffset := msgs[len(msgs)-1].BrokerMeta.Queue.Offset
			releaseDeliverySources(msgs)

			if err := e.routeRemoteBatch(ctx, consumerInfo.ProxyNodeID, deliveries); err != nil {
				e.logger.Warn("remote queue message delivery failed",
					slog.String("client", consumerInfo.ClientID),
					slog.String("node", consumerInfo.ProxyNodeID),
					slog.String("queue", config.Name),
					slog.String("error", err.Error()))
				if corebroker.IsErrClientNotConnected(err) {
					e.unregisterConsumer(ctx, config.Name, groupID, consumerInfo.ConsumerID, err)
				}
				continue
			}

			if group.Mode == types.GroupModeStream {
				e.commitStreamCursor(ctx, config.Name, groupID, nextCursor)
			}
			delivered = true
			e.logger.Debug("routed queue message batch to remote consumer",
				slog.String("client", consumerInfo.ClientID),
				slog.String("node", consumerInfo.ProxyNodeID),
				slog.String("queue", config.Name),
				slog.Int("batch_size", len(deliveries)),
				slog.Uint64("last_offset", lastOffset))
		}
	}

	return delivered
}

func (e *DeliveryEngine) isRemoteConsumer(consumerInfo *types.ConsumerInfo) bool {
	return e.remote != nil && consumerInfo.ProxyNodeID != "" && consumerInfo.ProxyNodeID != e.localNodeID
}

func (e *DeliveryEngine) localDeliveryTargetMissing(clientID string) bool {
	targetChecker, ok := e.local.(ClientDeliveryTargetChecker)
	if ok {
		return !targetChecker.HasDeliveryTarget(clientID)
	}
	checker, ok := e.local.(ClientConnectionChecker)
	return ok && !checker.IsClientConnected(clientID)
}

func (e *DeliveryEngine) unregisterConsumer(ctx context.Context, queueName, groupID, consumerID string, reason error) {
	attrs := []slog.Attr{
		slog.String("queue", queueName),
		slog.String("group", groupID),
		slog.String("consumer", consumerID),
	}
	if reason != nil {
		attrs = append(attrs, slog.String("reason", reason.Error()))
	}
	e.logger.LogAttrs(ctx, slog.LevelWarn, "removing stale queue consumer", attrs...)

	if err := e.consumerManager.UnregisterConsumer(ctx, queueName, groupID, consumerID); err != nil {
		if !errors.Is(err, consumer.ErrConsumerNotFound) &&
			!errors.Is(err, storage.ErrConsumerNotFound) &&
			!errors.Is(err, storage.ErrQueueNotFound) {
			e.logger.Warn("failed to unregister stale queue consumer",
				slog.String("queue", queueName),
				slog.String("group", groupID),
				slog.String("consumer", consumerID),
				slog.String("error", err.Error()))
		}
	} else if e.onConsumerRemoved != nil {
		e.onConsumerRemoved(ctx, queueName, groupID, []string{consumerID})
	}
	if e.remote == nil {
		return
	}
	if err := e.remote.UnregisterQueueConsumer(ctx, queueName, groupID, consumerID); err != nil {
		e.logger.Warn("failed to unregister stale queue consumer from cluster",
			slog.String("queue", queueName),
			slog.String("group", groupID),
			slog.String("consumer", consumerID),
			slog.String("error", err.Error()))
	}
}

func (e *DeliveryEngine) touchConsumerHeartbeat(ctx context.Context, queueName, groupID, consumerID string) {
	if err := e.consumerManager.UpdateHeartbeat(ctx, queueName, groupID, consumerID); err != nil {
		e.logger.Warn("failed to update consumer heartbeat",
			slog.String("queue", queueName),
			slog.String("group", groupID),
			slog.String("consumer", consumerID),
			slog.String("error", err.Error()))
	}
}

func (e *DeliveryEngine) commitStreamCursor(ctx context.Context, queueName, groupID string, cursor uint64) {
	if err := e.stateMachine.CommitConsume(ctx, CommitConsumeCommand{
		QueueName: queueName,
		GroupID:   groupID,
		Offset:    cursor,
	}); err != nil {
		e.logger.Warn("failed to commit stream cursor after delivery",
			slog.String("queue", queueName),
			slog.String("group", groupID),
			slog.Uint64("cursor", cursor),
			slog.String("error", err.Error()))
	}
}

func (e *DeliveryEngine) routeRemoteBatch(ctx context.Context, nodeID string, deliveries []cluster.QueueDelivery) error {
	if len(deliveries) == 0 {
		return nil
	}
	defer func() {
		for _, delivery := range deliveries {
			message.Release(delivery.Message)
		}
	}()
	if batchRouter, ok := e.remote.(RemoteBatchRouter); ok {
		err := batchRouter.RouteQueueBatch(ctx, nodeID, deliveries)
		if err == nil {
			return nil
		}
		// Batch router errors may be shared across coalesced requests. Fall back
		// so any stale-client error is tied to this exact delivery.
	}

	for _, delivery := range deliveries {
		if delivery.Message == nil {
			continue
		}
		if err := e.remote.RouteQueueMessage(ctx, nodeID, delivery.ClientID, delivery.Message); err != nil {
			return err
		}
	}
	return nil
}

// --- Message building helpers (stateless) ---

func createDeliveryMessage(msg *message.Envelope, groupID string, queueName string) *message.Envelope {
	delivery := msg.Clone()
	delivery.Topic = queueDeliveryTopic(queueName, msg.Topic)
	delivery.BrokerMeta.Source.Topic = msg.Topic
	delivery.BrokerMeta.Delivery = message.DeliveryMetadata{
		PublishedAt: msg.BrokerMeta.Delivery.PublishedAt,
		ExpiresAt:   msg.BrokerMeta.Delivery.ExpiresAt,
		QoS:         1,
	}
	// No delivery handle is stored: the queue and offset are the identity, and
	// the string a consumer sees is rendered from them at the protocol boundary.
	delivery.BrokerMeta.Queue = message.QueueMetadata{
		Name:    queueName,
		GroupID: groupID,
		Offset:  msg.BrokerMeta.Queue.Offset,
	}
	return delivery
}

func releaseDeliverySources(envelopes []*message.Envelope) {
	for _, envelope := range envelopes {
		message.Release(envelope)
	}
}

func decorateStreamDelivery(delivery *message.Envelope, msg *message.Envelope, workCommitted uint64, hasWorkCommitted bool, primaryGroup string) {
	if delivery == nil || msg == nil {
		return
	}
	delivery.BrokerMeta.Queue.Stream = &message.StreamMetadata{
		Offset:    msg.BrokerMeta.Queue.Offset,
		Timestamp: msg.BrokerMeta.Queue.CreatedAt.UnixMilli(),
	}
	if hasWorkCommitted {
		delivery.BrokerMeta.Queue.Stream.HasCommittedOffset = true
		delivery.BrokerMeta.Queue.Stream.CommittedOffset = workCommitted
		delivery.BrokerMeta.Queue.Stream.WorkAcknowledged = msg.BrokerMeta.Queue.Offset < workCommitted
		delivery.BrokerMeta.Queue.Stream.WorkGroup = primaryGroup
	}
}

func createRoutedQueueMessage(msg *message.Envelope, groupID, queueName string, stream bool, workCommitted uint64, hasWorkCommitted bool, primaryGroup string) *message.Envelope {
	routed := createDeliveryMessage(msg, groupID, queueName)
	if stream {
		decorateStreamDelivery(routed, msg, workCommitted, hasWorkCommitted, primaryGroup)
	}
	return routed
}

// queueDeliveryTopic converts a queue's stored source topic into the canonical
// queue address expected by protocol consumers. Explicit queue publishes are
// already canonical. Ordinary pub/sub captures retain their original path
// after the queue root, so a capture of m/domain/... in queue m is delivered as
// $queue/m/domain/....
//
// The address identifies the queue and nothing more. It is deliberately not
// injective: a capture of m/acme/temp into queue m, a capture of acme/temp into
// queue m, and an explicit publish to $queue/m/acme/temp all deliver as
// $queue/m/acme/temp, because the leading level is absorbed when it already
// equals the queue name. Consumers must not parse a source topic back out of
// it; the v1 contract carries the origin in typed SourceMetadata, which the
// broker stamps and a publisher cannot forge.
//
// That contract reaches every protocol that can encode message properties —
// MQTT 5.0, AMQP 0.9.1 and AMQP 1.0. MQTT 3.1.1 has no property field, so a
// 3.1.1 consumer of a captured message receives the queue identity and nothing
// about where the message came from. Explicit queue publishes are unaffected on
// every protocol, since their address is already canonical.
func queueDeliveryTopic(queueName, topic string) string {
	queueName = strings.Trim(strings.TrimSpace(queueName), "/")
	topic = strings.TrimPrefix(strings.TrimSpace(topic), "/")
	if queueName == "" {
		return topic
	}

	root := "$queue/" + queueName
	switch {
	case topic == "", topic == queueName:
		return root
	case topic == root, strings.HasPrefix(topic, root+"/"):
		return topic
	case strings.HasPrefix(topic, queueName+"/"):
		return "$queue/" + topic
	default:
		return root + "/" + topic
	}
}
