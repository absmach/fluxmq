// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
)

// Common handler errors.
var (
	ErrProtocolViolation   = errors.New("protocol violation")
	ErrNotAuthorized       = errors.New("not authorized")
	ErrBadUserOrPassword   = errors.New("bad username or password")
	ErrClientIDRejected    = errors.New("client identifier rejected")
	ErrClientIDRequired    = errors.New("client identifier required")
	ErrServerUnavailable   = errors.New("server unavailable")
	ErrTopicInvalid        = errors.New("topic name invalid")
	ErrPacketTooLarge      = errors.New("packet too large")
	ErrQuotaExceeded       = errors.New("quota exceeded")
	ErrMaxSessionsExceeded = errors.New("maximum sessions exceeded")
	ErrInvalidPacketType   = errors.New("invalid packet type")
	ErrSessionNotFound     = errors.New("session not found")
)

const (
	queueHeartbeatUpdateMinInterval = time.Second
	queueHeartbeatUpdateTimeout     = 2 * time.Second
)

func maybeUpdateQueueHeartbeat(b *Broker, s *session.Session) {
	if b.queueManager == nil {
		return
	}
	if !s.ShouldUpdateHeartbeat(time.Now(), queueHeartbeatUpdateMinInterval) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), queueHeartbeatUpdateTimeout)
	defer cancel()
	_ = b.queueManager.UpdateHeartbeat(ctx, s.ID)
}

// Handler is the interface for packet handlers (V3, V5, etc).
//
// Post-CONNECT handlers receive a *connCtx rather than a *session.Session: it
// binds response writes and teardown to the specific connection generation that
// read the packet, so a superseded connection's leftover goroutine cannot write
// to or disconnect the connection that replaced it.
type Handler interface {
	// HandleConnect handles CONNECT packets.
	HandleConnect(conn core.Connection, pkt packets.ControlPacket) error

	// HandlePublish handles PUBLISH packets.
	HandlePublish(s *connCtx, pkt packets.ControlPacket) error

	// HandlePubAck handles PUBACK packets.
	HandlePubAck(s *connCtx, pkt packets.ControlPacket) error

	// HandlePubRec handles PUBREC packets.
	HandlePubRec(s *connCtx, pkt packets.ControlPacket) error

	// HandlePubRel handles PUBREL packets.
	HandlePubRel(s *connCtx, pkt packets.ControlPacket) error

	// HandlePubComp handles PUBCOMP packets.
	HandlePubComp(s *connCtx, pkt packets.ControlPacket) error

	// HandleSubscribe handles SUBSCRIBE packets.
	HandleSubscribe(s *connCtx, pkt packets.ControlPacket) error

	// HandleUnsubscribe handles UNSUBSCRIBE packets.
	HandleUnsubscribe(s *connCtx, pkt packets.ControlPacket) error

	// HandlePingReq handles PINGREQ packets.
	HandlePingReq(s *connCtx) error

	// HandleDisconnect handles DISCONNECT packets.
	HandleDisconnect(s *connCtx, pkt packets.ControlPacket) error

	// HandleAuth handles AUTH packets.
	HandleAuth(s *connCtx, pkt packets.ControlPacket) error
}

// Router is the message routing interface.
type Router interface {
	Subscribe(clientID string, filter string, qos byte, opts storage.SubscribeOptions) error
	Unsubscribe(clientID string, filter string) error
	Match(topic string) ([]*storage.Subscription, error)
	MatchInto(topic string, matched *[]*storage.Subscription) error
}

// Service defines the broker service interface for middleware wrapping.
type Service interface {
	HandleConnection(conn core.Connection)
	Subscribe(clientID string, filter string, qos byte, opts storage.SubscribeOptions) error
	Unsubscribe(clientID string, filter string) error
	Match(topic string) ([]*storage.Subscription, error)
	Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error
	Close() error
}
