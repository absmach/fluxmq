// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package wiring

import (
	"context"
	"errors"
	"testing"

	"github.com/absmach/fluxmq/cluster"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
)

type fakeMQTTClusterHandler struct {
	deliverCalls []string
	forwardErr   error
}

func (f *fakeMQTTClusterHandler) DeliverToClient(ctx context.Context, clientID string, msg *cluster.Message) error {
	f.deliverCalls = append(f.deliverCalls, clientID)
	return nil
}

func (f *fakeMQTTClusterHandler) ForwardPublish(ctx context.Context, msg *cluster.Message) error {
	return f.forwardErr
}

func (f *fakeMQTTClusterHandler) GetSessionStateAndClose(ctx context.Context, clientID string) (*clusterv1.SessionState, error) {
	return nil, nil
}

func (f *fakeMQTTClusterHandler) GetRetainedMessage(ctx context.Context, topic string) (*storage.Message, error) {
	return nil, nil
}

func (f *fakeMQTTClusterHandler) GetWillMessage(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	return nil, nil
}

type fakeAMQPForwardHandler struct {
	deliverCalls []string
	forwardErr   error
}

func (f *fakeAMQPForwardHandler) DeliverToClusterMessage(ctx context.Context, clientID string, msg *cluster.Message) error {
	f.deliverCalls = append(f.deliverCalls, clientID)
	return nil
}

func (f *fakeAMQPForwardHandler) ForwardPublish(ctx context.Context, msg *cluster.Message) error {
	return f.forwardErr
}

func TestMessageDispatcherDeliverToClientRoutesByProtocol(t *testing.T) {
	mqtt := &fakeMQTTClusterHandler{}
	amqp1 := &fakeAMQPForwardHandler{}
	amqp091 := &fakeAMQPForwardHandler{}
	d := NewMessageDispatcher(mqtt, amqp1, amqp091)

	msg := &cluster.Message{Topic: "test/topic", Payload: []byte("x")}
	if err := d.DeliverToClient(context.Background(), "amqp:container-1", msg); err != nil {
		t.Fatalf("amqp1 delivery failed: %v", err)
	}
	if err := d.DeliverToClient(context.Background(), "amqp091-conn-1", msg); err != nil {
		t.Fatalf("amqp091 delivery failed: %v", err)
	}
	if err := d.DeliverToClient(context.Background(), "mqtt-client-1", msg); err != nil {
		t.Fatalf("mqtt delivery failed: %v", err)
	}

	if len(amqp1.deliverCalls) != 1 || amqp1.deliverCalls[0] != "amqp:container-1" {
		t.Fatalf("expected amqp1 route, got calls=%v", amqp1.deliverCalls)
	}
	if len(amqp091.deliverCalls) != 1 || amqp091.deliverCalls[0] != "amqp091-conn-1" {
		t.Fatalf("expected amqp091 route, got calls=%v", amqp091.deliverCalls)
	}
	if len(mqtt.deliverCalls) != 1 || mqtt.deliverCalls[0] != "mqtt-client-1" {
		t.Fatalf("expected mqtt route, got calls=%v", mqtt.deliverCalls)
	}
}

func TestMessageDispatcherForwardPublishJoinsErrors(t *testing.T) {
	errMQTT := errors.New("mqtt forward failed")
	errAMQP1 := errors.New("amqp1 forward failed")

	d := NewMessageDispatcher(
		&fakeMQTTClusterHandler{forwardErr: errMQTT},
		&fakeAMQPForwardHandler{forwardErr: errAMQP1},
		&fakeAMQPForwardHandler{},
	)

	err := d.ForwardPublish(context.Background(), &cluster.Message{Topic: "test/topic"})
	if err == nil {
		t.Fatal("expected joined error, got nil")
	}
	if !errors.Is(err, errMQTT) {
		t.Fatalf("expected mqtt error in chain, got %v", err)
	}
	if !errors.Is(err, errAMQP1) {
		t.Fatalf("expected amqp1 error in chain, got %v", err)
	}
}
