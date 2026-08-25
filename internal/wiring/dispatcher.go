// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package wiring

import (
	"context"
	"errors"

	amqpbroker "github.com/absmach/fluxmq/amqp/broker"
	amqp1broker "github.com/absmach/fluxmq/amqp1/broker"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/message"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
)

type mqttClusterHandler interface {
	cluster.MessageHandler
	cluster.ForwardPublishHandler
}

type amqp1ForwardHandler interface {
	DeliverToClusterMessage(ctx context.Context, clientID string, msg *message.Envelope) error
	ForwardPublish(ctx context.Context, msg *message.Envelope) error
}

type amqp091ForwardHandler interface {
	DeliverToClusterMessage(ctx context.Context, clientID string, msg *message.Envelope) error
	ForwardPublish(ctx context.Context, msg *message.Envelope) error
}

// MessageDispatcher routes cluster-delivered messages to the appropriate protocol broker.
type MessageDispatcher struct {
	mqtt    mqttClusterHandler
	amqp    amqp1ForwardHandler
	amqp091 amqp091ForwardHandler
}

var (
	_ cluster.MessageHandler        = (*MessageDispatcher)(nil)
	_ cluster.ForwardPublishHandler = (*MessageDispatcher)(nil)
)

// NewMessageDispatcher builds a protocol-aware cluster dispatcher.
func NewMessageDispatcher(mqtt mqttClusterHandler, amqp amqp1ForwardHandler, amqp091 amqp091ForwardHandler) *MessageDispatcher {
	return &MessageDispatcher{
		mqtt:    mqtt,
		amqp:    amqp,
		amqp091: amqp091,
	}
}

func (d *MessageDispatcher) DeliverToClient(ctx context.Context, clientID string, msg *message.Envelope) error {
	defer message.Release(msg)
	if amqp1broker.IsAMQPClient(clientID) {
		return d.amqp.DeliverToClusterMessage(ctx, clientID, msg)
	}
	if amqpbroker.IsAMQP091Client(clientID) {
		return d.amqp091.DeliverToClusterMessage(ctx, clientID, msg)
	}
	return d.mqtt.DeliverToClient(ctx, clientID, msg)
}

func (d *MessageDispatcher) ForwardPublish(ctx context.Context, msg *message.Envelope) error {
	defer message.Release(msg)
	var errs []error
	if err := d.mqtt.ForwardPublish(ctx, msg); err != nil {
		errs = append(errs, err)
	}
	if err := d.amqp.ForwardPublish(ctx, msg); err != nil {
		errs = append(errs, err)
	}
	if err := d.amqp091.ForwardPublish(ctx, msg); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (d *MessageDispatcher) GetSessionStateAndClose(ctx context.Context, clientID string) (*clusterv1.SessionState, error) {
	return d.mqtt.GetSessionStateAndClose(ctx, clientID)
}

func (d *MessageDispatcher) HandleSessionLeaseLost(ctx context.Context, clientIDs []string) {
	d.mqtt.HandleSessionLeaseLost(ctx, clientIDs)
}

func (d *MessageDispatcher) GetRetainedMessage(ctx context.Context, topic string) (*message.Envelope, error) {
	return d.mqtt.GetRetainedMessage(ctx, topic)
}

func (d *MessageDispatcher) GetWillMessage(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	return d.mqtt.GetWillMessage(ctx, clientID)
}
