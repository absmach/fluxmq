// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/broker/events"
	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
)

// Publish publishes a message, handling retained storage and distribution to subscribers.
// Publish takes ownership of msg on every path; callers must not use it after
// the call.
func (b *Broker) Publish(ctx context.Context, msg *message.Envelope) error {
	defer message.Release(msg)
	if b.telemetry.logger.Enabled(ctx, slog.LevelDebug) {
		b.logOp("publish", slog.String("topic", msg.Topic), slog.Int("qos", int(msg.BrokerMeta.Delivery.QoS)), slog.Bool("retain", msg.BrokerMeta.Delivery.Retain))
	}
	b.telemetry.stats.IncrementPublishReceived()

	payloadLen := len(msg.PayloadBytes())
	b.telemetry.stats.AddBytesReceived(uint64(payloadLen))

	// Record metrics
	if b.telemetry.metrics != nil {
		b.telemetry.metrics.RecordMessageReceived(msg.BrokerMeta.Delivery.QoS, int64(payloadLen)) //nolint:contextcheck // metrics recording uses background context internally
	}

	route := b.routeResolver.Resolve(msg.Topic)

	// Handle retained messages before routing — ensures queue topics also
	// store retained state so new subscribers receive the last known value.
	if msg.BrokerMeta.Delivery.Retain {
		if err := b.handleRetained(ctx, msg, payloadLen); err != nil {
			if route.Kind == broker.RouteQueue {
				b.logError("retained_store_failed", err, slog.String("topic", msg.Topic))
			} else {
				return err
			}
		}
		// For queue topics, retained messages are stored only in the retained
		// store (for last-known-value delivery on subscribe). They are NOT
		// enqueued — the queue handles the ordered stream of non-retained
		// messages separately, avoiding duplicates on subscribe.
		if route.Kind == broker.RouteQueue {
			return nil
		}
	}

	// Route queue topics and ack topics to queue manager
	if b.queueManager != nil {
		switch route.Kind {
		case broker.RouteQueueMalformed:
			b.logError("queue_control_verb_misplaced",
				fmt.Errorf("%q must be the last level of a queue address", route.ControlVerb),
				slog.String("topic", msg.Topic))
			return fmt.Errorf("queue address %q: %s must be the last level", msg.Topic, route.ControlVerb)
		case broker.RouteQueueAck:
			return b.handleQueueAck(ctx, msg, route)
		case broker.RouteQueue:
			// The queue borrows the envelope and derives its own record from it
			// before this one is released.
			return b.queueManager.Publish(ctx, msg)
		}
	}

	// A configured stream may bind an ordinary topic pattern (for example
	// m/#). Capture it only on the ingress node; cluster-forwarded pub/sub
	// deliveries use ForwardPublish and therefore cannot append duplicates.
	//
	// A capture failure never fails the publish: see broker.TopicQueuePublisher.
	if publisher, ok := b.queueManager.(broker.TopicQueuePublisher); ok {
		if err := publisher.PublishToMatchingQueues(ctx, msg); err != nil {
			b.logError("queue_topic_capture", err, slog.String("topic", msg.Topic))
		}
	}

	// Webhook: message published
	if b.telemetry.webhooks != nil {
		payload := ""
		// Note: Payload encoding should be done by caller if needed
		// ClientID not available at broker level, will be set by handler
		b.telemetry.webhooks.Notify(ctx, events.MessagePublished{ //nolint:errcheck // fire-and-forget webhook notification
			ClientID:     "", // Set by handler
			MessageTopic: msg.Topic,
			QoS:          msg.BrokerMeta.Delivery.QoS,
			Retained:     msg.BrokerMeta.Delivery.Retain,
			PayloadSize:  payloadLen,
			Payload:      payload, // Will be set if includePayload is true
		})
	}

	// Event hook: message published
	if b.eventHook != nil {
		if err := b.eventHook.OnPublish(ctx, msg.BrokerMeta.Source.ClientID, msg.Topic, msg.BrokerMeta.Delivery.QoS, msg.PayloadBytes()); err != nil {
			b.logError("event_hook_publish", err, slog.String("topic", msg.Topic))
		}
	}

	// Distribute message to subscribers (this will retain the buffer as needed)
	err := b.distribute(ctx, msg)

	return err
}

// PublishWill publishes a session's will message if it exists.
func (b *Broker) PublishWill(ctx context.Context, clientID string) error {
	if b.stores.wills == nil {
		return nil
	}

	will, err := b.stores.wills.Get(ctx, clientID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		return err
	}

	if err := b.publishWillMessage(ctx, will); err != nil {
		return err
	}

	return b.stores.wills.Delete(ctx, clientID)
}

// publishWillMessage distributes a Will message. Used both for stored Wills
// (PublishWill) and for the Will of a connection displaced by a takeover.
func (b *Broker) publishWillMessage(ctx context.Context, will *storage.WillMessage) error {
	// Persisted Will payloads are byte slices; ingress copies them into an
	// immutable broker buffer.
	msg := message.New(will.Topic, will.Payload)
	msg.BrokerMeta.Source.ClientID = will.ClientID
	msg.BrokerMeta.Delivery.QoS = will.QoS
	msg.BrokerMeta.Delivery.Retain = will.Retain
	msg.PublisherMeta.Properties = message.NewPropertyMap(will.Properties)

	err := b.distribute(ctx, msg)
	message.Release(msg)
	return err
}

// handleRetained stores or clears a retained message.
func (b *Broker) handleRetained(ctx context.Context, msg *message.Envelope, payloadLen int) error {
	if payloadLen == 0 {
		if err := b.stores.retained.Delete(ctx, msg.Topic); err != nil {
			return err
		}
		if b.cluster != nil {
			if err := b.cluster.Retained().Delete(ctx, msg.Topic); err != nil {
				b.logError("cluster_delete_retained", err, slog.String("topic", msg.Topic))
			}
		}
		if b.telemetry.webhooks != nil {
			b.telemetry.webhooks.Notify(ctx, events.RetainedMessageSet{ //nolint:errcheck // fire-and-forget webhook notification
				MessageTopic: msg.Topic,
				PayloadSize:  0,
				Cleared:      true,
			})
		}
		return nil
	}

	retainedMsg := msg.Clone()
	defer message.Release(retainedMsg)
	retainedMsg.BrokerMeta.Delivery.Retain = true
	if err := b.stores.retained.Set(ctx, msg.Topic, retainedMsg); err != nil {
		return err
	}
	if b.cluster != nil {
		if err := b.cluster.Retained().Set(ctx, msg.Topic, retainedMsg); err != nil {
			b.logError("cluster_set_retained", err, slog.String("topic", msg.Topic))
		}
	}
	if b.telemetry.webhooks != nil {
		b.telemetry.webhooks.Notify(ctx, events.RetainedMessageSet{ //nolint:errcheck // fire-and-forget webhook notification
			MessageTopic: msg.Topic,
			PayloadSize:  payloadLen,
			Cleared:      false,
		})
	}
	return nil
}

// Distribute distributes a message to all matching subscribers (implements Service interface).
func (b *Broker) Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
	msg := message.New(topic, payload)
	msg.BrokerMeta.Delivery.QoS = qos
	msg.BrokerMeta.Delivery.Retain = retain
	msg.PublisherMeta.Properties = message.NewPropertyMap(props)

	err := b.distribute(context.Background(), msg)

	message.Release(msg)

	return err
}

// distribute distributes a message to all matching subscribers (local and remote).
func (b *Broker) distribute(ctx context.Context, msg *message.Envelope) error {
	if _, err := b.distributeLocal(ctx, msg, ingressScope); err != nil {
		return err
	}

	// Route to remote subscribers in cluster
	if b.cluster != nil {
		timeout := b.cfg.routePublishTimeout
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// A cross-node hop always carries retain=0: the receiving node re-stamps
		// the flag per local subscription. Clear it on a borrowed shallow copy
		// rather than on msg, which local fan-out may still be reading.
		// RoutePublish only reads the copy, and never releases it.
		forward := *msg
		forward.BrokerMeta.Delivery.Retain = false
		if err := b.cluster.RoutePublish(ctx, &forward); err != nil {
			b.logError("cluster_route_publish", err, slog.String("topic", msg.Topic))
			if msg.BrokerMeta.Delivery.QoS > 0 {
				return fmt.Errorf("cluster route publish: %w", err)
			}
		}
	}

	return nil
}

// distributeScope says which deliveries one local distribution is responsible
// for. An ingress publish owns all of them. A publish forwarded from another
// node owns neither cross-protocol delivery nor share groups: the ingress node
// ran the first, and chose a share group's member across the whole cluster,
// sending to it directly. A forwarded message reaches this node because of some
// ordinary subscription, and letting it pick from a share group here would hand
// the group a second copy.
type distributeScope struct {
	crossProtocol bool
	shareGroups   bool
}

var (
	ingressScope   = distributeScope{crossProtocol: true, shareGroups: true}
	forwardedScope = distributeScope{}
)

// distributeLocal delivers a message to local subscribers and returns the
// number of matched subscriptions.
func (b *Broker) distributeLocal(ctx context.Context, msg *message.Envelope, scope distributeScope) (int, error) {
	matched := router.AcquireSubscriptionSlice()
	defer router.ReleaseSubscriptionSlice(matched)

	if err := b.router.MatchInto(msg.Topic, matched); err != nil {
		return 0, err
	}

	// Members of matching share groups that live on other nodes. The local
	// router only knows the groups something here subscribed to, so without
	// this a group whose members are all elsewhere would match nothing and
	// receive nothing.
	remote := acquireShareMembers()
	defer releaseShareMembers(remote)

	if scope.shareGroups && b.cluster != nil {
		members, err := b.cluster.ShareGroupMembers(ctx, msg.Topic, (*remote)[:0])
		if err != nil {
			b.logError("cluster_share_group_members", err, slog.String("topic", msg.Topic))
		}
		*remote = members
	}

	// Track which share groups have already received the message (lazy init)
	var deliveredGroups map[string]bool

	for _, sub := range *matched {
		clientID := sub.ClientID

		// Check if this is a shared subscription
		if strings.HasPrefix(clientID, "$share/") {
			if !scope.shareGroups {
				continue
			}

			// Extract group key from the special client ID
			groupKey := clientID[7:] // Remove "$share/" prefix

			// Lazy init the map only when we have shared subscriptions
			if deliveredGroups == nil {
				deliveredGroups = make(map[string]bool)
			}

			// Skip if we already delivered to this group
			if deliveredGroups[groupKey] {
				continue
			}
			deliveredGroups[groupKey] = true

			// groupKey here typically looks like "groupName/topicFilter"
			b.deliverToShareGroup(ctx, groupKey, sub.QoS, msg, *remote)
		} else {
			if sub.Options.NoLocal && clientID == msg.BrokerMeta.Source.ClientID {
				continue
			}
			if broker.IsAMQP091Client(clientID) || broker.IsAMQP1Client(clientID) {
				if scope.crossProtocol && b.crossDeliver != nil {
					b.crossDeliver(ctx, clientID, msg.Topic, msg.PayloadBytes(), sub.QoS, message.ProjectProperties(msg, message.TrustedServiceProjection))
				}
				continue
			}
			// Normal subscription
			s := b.sessionsMap.Get(clientID)
			if s == nil {
				continue
			}

			deliverQoS := msg.BrokerMeta.Delivery.QoS
			if sub.QoS < deliverQoS {
				deliverQoS = sub.QoS
			}
			retain := msg.BrokerMeta.Delivery.Retain && sub.Options.RetainAsPublished
			if deliverQoS == 0 {
				_ = b.deliverSharedQoS0(ctx, s, msg, retain)
				continue
			}

			deliverMsg := msg.Clone()
			deliverMsg.BrokerMeta.Delivery.QoS = deliverQoS
			deliverMsg.BrokerMeta.Delivery.Retain = retain

			// DeliverToSession takes full ownership of the message
			if _, err := b.DeliverToSession(ctx, s, deliverMsg); err != nil {
				if deliverQoS > 0 {
					b.telemetry.logger.Warn("failed to deliver QoS message",
						slog.String("client_id", clientID),
						slog.String("topic", msg.Topic),
						slog.Uint64("qos", uint64(deliverQoS)),
						slog.String("error", err.Error()))
				}
				continue
			}
		}
	}

	// A group with no member on this node never appears in the local match, so
	// its turn has to be taken from the remote members directly.
	for i := range *remote {
		member := (*remote)[i]
		groupKey := member.ShareName + "/" + member.Filter
		if deliveredGroups[groupKey] {
			continue
		}
		if deliveredGroups == nil {
			deliveredGroups = make(map[string]bool)
		}
		deliveredGroups[groupKey] = true

		b.deliverToShareGroup(ctx, groupKey, member.QoS, msg, *remote)
	}

	return len(*matched), nil
}

// shareMemberPool holds the per-publish slice of remote share group members.
// A publish that matches no share group never fills one, so the pool costs a
// get and a put on the hot path rather than an allocation.
var shareMemberPool = sync.Pool{
	New: func() any {
		members := make([]cluster.ShareMember, 0, 8)
		return &members
	},
}

func acquireShareMembers() *[]cluster.ShareMember {
	return shareMemberPool.Get().(*[]cluster.ShareMember)
}

func releaseShareMembers(members *[]cluster.ShareMember) {
	const maxPooledShareMembers = 256
	if cap(*members) > maxPooledShareMembers {
		return
	}
	*members = (*members)[:0]
	shareMemberPool.Put(members)
}

// shareMemberCount reports how many of members belong to groupKey.
func shareMemberCount(members []cluster.ShareMember, groupKey string) int {
	count := 0
	for i := range members {
		if shareMemberInGroup(members[i], groupKey) {
			count++
		}
	}

	return count
}

// shareMemberAt returns the i-th member of groupKey. Members of one group are
// not contiguous in the slice — it holds every group the topic matched — so the
// index is counted rather than sliced. A group is small, and counting keeps the
// publish path free of a per-group slice.
func shareMemberAt(members []cluster.ShareMember, groupKey string, i int) (cluster.ShareMember, bool) {
	for j := range members {
		if !shareMemberInGroup(members[j], groupKey) {
			continue
		}
		if i == 0 {
			return members[j], true
		}
		i--
	}

	return cluster.ShareMember{}, false
}

func shareMemberInGroup(member cluster.ShareMember, groupKey string) bool {
	name, filter, _ := strings.Cut(groupKey, "/")
	return member.ShareName == name && member.Filter == filter
}

// deliverToShareGroup hands msg to one member of a share group and advances the
// group's round-robin cursor by one. The group spans the cluster: its members
// here and its members on other nodes take turns in one rotation, so exactly
// one of them receives the message wherever it is connected.
//
// A member that cannot take the message — its session is gone, its node is
// unreachable — is skipped and the next is tried, so the group loses a message
// only when no member can take it. subQoS caps delivery for local members;
// remote members carry their own cap.
//
// It borrows msg. Reports whether a member took the message.
func (b *Broker) deliverToShareGroup(ctx context.Context, groupKey string, subQoS byte, msg *message.Envelope, remote []cluster.ShareMember) bool {
	local := b.sharedSubs.LocalMemberCount(groupKey)
	remoteCount := shareMemberCount(remote, groupKey)

	total := local + remoteCount
	if total == 0 {
		return false
	}

	rotation := b.sharedSubs.NextRotation(groupKey, total)

	// The rotation names the member whose turn it is; the rest of the pass are
	// its fallbacks, in group order, and one pass tries every member once.
	for offset := range total {
		index := (rotation + offset) % total

		if index < local {
			clientID, ok := b.sharedSubs.LocalMemberAt(groupKey, index)
			if !ok {
				continue
			}
			if b.deliverShareLocal(ctx, clientID, subQoS, msg) {
				return true
			}
			continue
		}

		member, ok := shareMemberAt(remote, groupKey, index-local)
		if !ok {
			continue
		}
		if b.deliverShareRemote(ctx, member, msg) {
			return true
		}
	}

	return false
}

// deliverShareLocal delivers to one share group member connected to this node.
func (b *Broker) deliverShareLocal(ctx context.Context, clientID string, subQoS byte, msg *message.Envelope) bool {
	s := b.sessionsMap.Get(clientID)
	if s == nil {
		return false
	}

	deliverQoS := msg.BrokerMeta.Delivery.QoS
	if subQoS < deliverQoS {
		deliverQoS = subQoS
	}

	if deliverQoS == 0 {
		return b.deliverSharedQoS0(ctx, s, msg, false) == nil
	}

	deliverMsg := msg.Clone()
	deliverMsg.BrokerMeta.Delivery.QoS = deliverQoS
	deliverMsg.BrokerMeta.Delivery.Retain = false // MQTT spec: shared subscriptions don't receive retained flag

	// DeliverToSession takes full ownership of the message
	if _, err := b.DeliverToSession(ctx, s, deliverMsg); err != nil {
		b.telemetry.logger.Warn("failed to deliver QoS message to share group member",
			slog.String("client_id", clientID),
			slog.String("topic", msg.Topic),
			slog.Uint64("qos", uint64(deliverQoS)),
			slog.String("error", err.Error()))
		return false
	}

	return true
}

// deliverShareRemote sends the message to the one group member that was chosen,
// on the node holding its session. It is addressed to that client rather than
// broadcast to the node: the receiving node delivers it as told and runs no
// matching of its own, which is what keeps a group to one copy per message.
func (b *Broker) deliverShareRemote(ctx context.Context, member cluster.ShareMember, msg *message.Envelope) bool {
	if b.cluster == nil {
		return false
	}

	// The cross-node hop reads a borrowed shallow copy: local fan-out may still
	// be reading msg, and the QoS cap and cleared retain belong to this
	// delivery alone.
	forward := *msg
	if member.QoS < forward.BrokerMeta.Delivery.QoS {
		forward.BrokerMeta.Delivery.QoS = member.QoS
	}
	forward.BrokerMeta.Delivery.Retain = false

	if err := b.cluster.RoutePublishToClient(ctx, member.NodeID, member.ClientID, &forward); err != nil {
		b.telemetry.logger.Warn("failed to deliver to remote share group member",
			slog.String("client_id", member.ClientID),
			slog.String("node_id", member.NodeID),
			slog.String("topic", msg.Topic),
			slog.String("error", err.Error()))
		return false
	}

	return true
}

// ForwardPublish handles a forwarded publish from a remote cluster node.
// It converts the cluster message to a storage message and delivers locally.
func (b *Broker) ForwardPublish(ctx context.Context, msg *message.Envelope) error {
	storeMsg := msg.Clone()

	matched, err := b.distributeLocal(ctx, storeMsg, forwardedScope)
	message.Release(storeMsg)
	if err == nil && matched == 0 {
		// The sending node believed a subscriber lives here (stale owner
		// route); without this the message vanishes with no trace.
		b.warnUnroutableForward(msg.Topic)
	}
	return err
}

// warnUnroutableForward logs (at most once per 10s) that a forwarded publish
// matched no local subscription.
func (b *Broker) warnUnroutableForward(topic string) {
	const throttle = int64(10 * time.Second)
	now := time.Now().UnixNano()
	last := b.lastUnroutableWarn.Load()
	if now-last < throttle || !b.lastUnroutableWarn.CompareAndSwap(last, now) {
		return
	}
	b.telemetry.logger.Warn("forwarded publish matched no local subscription",
		slog.String("topic", topic))
}

// handleQueueAck handles queue acknowledgment messages ($ack, $nack, $reject).
func (b *Broker) handleQueueAck(ctx context.Context, msg *message.Envelope, route broker.RouteResult) error {
	queueName := route.QueueName

	if queueName == "" {
		b.logError("queue_ack_invalid_queue_topic", fmt.Errorf("invalid queue topic %q", route.PublishTopic),
			slog.String("topic", msg.Topic))
		return fmt.Errorf("invalid queue topic: %s", route.PublishTopic)
	}

	// The delivery being settled is named by the client, in the inbound command
	// namespace. It cannot be read from Broker.Queue: those are broker-owned
	// outbound fields, and the protocol boundary strips their reserved property
	// names from client input, so nothing a consumer sends ever reaches them.
	settlement, err := types.SettlementFromProperties(msg.PublisherMeta.Properties.Map())
	if err != nil {
		b.logError("queue_ack_invalid_settlement", err, slog.String("topic", msg.Topic))
		return err
	}
	groupID := settlement.GroupID
	offset := settlement.Offset

	switch route.AckKind {
	case broker.AckAccept:
		b.logOp("queue_ack", slog.String("queue", queueName), slog.Uint64("offset", offset), slog.String("group_id", groupID))
		return b.queueManager.Ack(ctx, queueName, groupID, offset)
	case broker.AckNack:
		b.logOp("queue_nack", slog.String("queue", queueName), slog.Uint64("offset", offset), slog.String("group_id", groupID))
		return b.queueManager.Nack(ctx, queueName, groupID, offset)
	case broker.AckReject:
		reason := "rejected by consumer"
		if rejectReason, ok := msg.PublisherMeta.Properties.Get(types.PropRejectReason); ok && rejectReason != "" {
			reason = rejectReason
		}
		b.logOp("queue_reject", slog.String("queue", queueName), slog.Uint64("offset", offset), slog.String("group_id", groupID), slog.String("reason", reason))
		return b.queueManager.Reject(ctx, queueName, groupID, offset, reason)
	default:
		return fmt.Errorf("invalid queue ack topic: %s", msg.Topic)
	}
}

// GetRetainedMatching returns all retained messages matching a topic filter.
// In clustered mode, queries the cluster; otherwise uses local storage.
func (b *Broker) GetRetainedMatching(filter string) ([]*message.Envelope, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if b.cluster != nil {
		return b.cluster.Retained().Match(ctx, filter)
	}
	return b.stores.retained.Match(ctx, filter)
}

// triggerWills processes pending will messages.
func (b *Broker) triggerWills() {
	if b.stores.wills == nil {
		return
	}

	ctx := context.Background()
	now := time.Now()
	pending, err := b.stores.wills.GetPending(ctx, now)
	if err != nil {
		return
	}

	for _, candidate := range pending {
		will := b.claimPendingWill(ctx, candidate.ClientID, now)
		if will == nil {
			continue
		}

		// Create a broker-owned envelope from the persisted Will payload.
		msg := message.New(will.Topic, will.Payload)
		msg.BrokerMeta.Source.ClientID = will.ClientID
		msg.BrokerMeta.Delivery.QoS = will.QoS
		msg.BrokerMeta.Delivery.Retain = will.Retain
		msg.PublisherMeta.Properties = message.NewPropertyMap(will.Properties)

		b.distribute(ctx, msg) //nolint:errcheck // fire-and-forget will message distribution

		message.Release(msg)
	}
}

// claimPendingWill revalidates a delayed-Will snapshot under the same client-ID
// lock used by disconnect, reconnect, and Clean Start. GetPending carries the
// store's disconnect timestamp, so re-querying here distinguishes an older
// pending generation from a newer identical Will whose delay has not elapsed.
// The record is deleted before unlocking; a reconnect that follows cannot
// cancel a Will whose deadline already won the lock, while a reconnect that
// wins first removes the record and this claim returns nil.
func (b *Broker) claimPendingWill(ctx context.Context, clientID string, now time.Time) *storage.WillMessage {
	sessionLock := b.sessionLocks.Key(clientID)
	sessionLock.Lock()
	defer sessionLock.Unlock()

	current, err := pendingWillForClient(ctx, b.stores.wills, clientID, now)
	if err != nil {
		return nil
	}

	s := b.sessionsMap.Get(clientID)
	if s != nil && s.IsConnected() {
		b.stores.wills.Delete(ctx, clientID) //nolint:errcheck // best-effort delayed-Will cancellation for connected client
		return nil
	}

	if err := b.stores.wills.Delete(ctx, clientID); err != nil {
		return nil
	}
	return current
}

type clientPendingWillStore interface {
	GetPendingForClient(ctx context.Context, clientID string, before time.Time) (*storage.WillMessage, error)
}

// pendingWillForClient uses the built-in stores' keyed fast path while keeping
// the public WillStore interface source-compatible for custom implementations.
func pendingWillForClient(ctx context.Context, store storage.WillStore, clientID string, before time.Time) (*storage.WillMessage, error) {
	if keyed, ok := store.(clientPendingWillStore); ok {
		return keyed.GetPendingForClient(ctx, clientID, before)
	}
	pending, err := store.GetPending(ctx, before)
	if err != nil {
		return nil, err
	}
	for _, will := range pending {
		if will.ClientID == clientID {
			return will, nil
		}
	}
	return nil, storage.ErrNotFound
}

// GetRetainedMessage implements cluster.MessageHandler.GetRetainedMessage.
// Fetches a retained message from the local storage for remote node requests.
func (b *Broker) GetRetainedMessage(ctx context.Context, topic string) (*message.Envelope, error) {
	if b.stores.retained == nil {
		return nil, fmt.Errorf("retained store not configured")
	}
	return b.stores.retained.Get(ctx, topic)
}

// GetWillMessage implements cluster.MessageHandler.GetWillMessage.
// Fetches a will message from the local storage for remote node requests.
func (b *Broker) GetWillMessage(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	if b.stores.wills == nil {
		return nil, fmt.Errorf("will store not configured")
	}
	return b.stores.wills.Get(ctx, clientID)
}
