// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/absmach/fluxmq/amqp/codec"
	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/cluster"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
)

func readBrokerFramesFrom(t *testing.T, buf *bytes.Buffer, start int) []*codec.Frame {
	t.Helper()

	data := buf.Bytes()
	if start > len(data) {
		t.Fatalf("start offset beyond buffer length")
	}

	r := bytes.NewReader(data[start:])
	var frames []*codec.Frame
	for r.Len() > 0 {
		frame, err := codec.ReadFrame(r)
		if err != nil {
			t.Fatalf("ReadFrame failed: %v", err)
		}
		frames = append(frames, frame)
	}
	return frames
}

func TestDeliverToClusterMessage(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	buf := &bytes.Buffer{}

	c := &Connection{
		broker:   b,
		writer:   bufio.NewWriter(buf),
		frameMax: defaultFrameMax,
		logger:   logger,
		connID:   "conn-1",
		channels: make(map[uint16]*Channel),
	}
	ch := newChannel(c, 1)
	ch.consumers["ctag"] = &consumer{
		tag:        "ctag",
		queue:      "telemetry/#",
		mqttFilter: "telemetry/#",
		noAck:      true,
	}
	c.channels[1] = ch
	b.connections.Store(c.connID, c)

	msg := &cluster.Message{
		Topic:      "telemetry/room1",
		Payload:    []byte("hello"),
		Properties: map[string]string{qtypes.PropMessageID: "m1"},
	}
	if err := b.DeliverToClusterMessage(context.Background(), PrefixedClientID(c.connID), msg); err != nil {
		t.Fatalf("DeliverToClusterMessage failed: %v", err)
	}

	frames := readBrokerFramesFrom(t, buf, 0)
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}

	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	deliver, ok := decoded.(*codec.BasicDeliver)
	if !ok {
		t.Fatalf("expected BasicDeliver, got %T", decoded)
	}
	if deliver.ConsumerTag != "ctag" {
		t.Fatalf("expected consumer tag ctag, got %q", deliver.ConsumerTag)
	}
	if deliver.Exchange != "" {
		t.Fatalf("expected default exchange, got %q", deliver.Exchange)
	}
	if deliver.RoutingKey != "telemetry.room1" {
		t.Fatalf("expected AMQP routing key telemetry.room1, got %q", deliver.RoutingKey)
	}

	if frames[1].Type != codec.FrameHeader {
		t.Fatalf("expected header frame, got %d", frames[1].Type)
	}
	if frames[2].Type != codec.FrameBody {
		t.Fatalf("expected body frame, got %d", frames[2].Type)
	}
	if string(frames[2].Payload) != "hello" {
		t.Fatalf("expected payload hello, got %q", string(frames[2].Payload))
	}
}

func TestDeliverToClusterMessageClientNotFound(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)

	err := b.DeliverToClusterMessage(context.Background(), PrefixedClientID("missing"), &cluster.Message{
		Topic:   "telemetry/room1",
		Payload: []byte("hello"),
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "client not found") {
		t.Fatalf("expected client not found error, got %v", err)
	}
}

func TestPublishDispatchesToLocalAndCross(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	buf := &bytes.Buffer{}

	c := &Connection{
		broker:   b,
		writer:   bufio.NewWriter(buf),
		frameMax: defaultFrameMax,
		logger:   logger,
		connID:   "conn-1",
		channels: make(map[uint16]*Channel),
	}
	ch := newChannel(c, 1)
	ch.consumers["ctag"] = &consumer{
		tag:        "ctag",
		queue:      "telemetry/#",
		mqttFilter: "telemetry/#",
		noAck:      true,
	}
	c.channels[1] = ch
	b.connections.Store(c.connID, c)

	if err := b.router.Subscribe(PrefixedClientID(c.connID), "telemetry/#", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("local subscribe failed: %v", err)
	}
	if err := b.router.Subscribe("mqtt-client", "telemetry/#", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("cross subscribe failed: %v", err)
	}

	calls := 0
	var gotClientID string
	b.SetCrossDeliver(func(clientID string, topic string, payload []byte, qos byte, props map[string]string) {
		calls++
		gotClientID = clientID
	})

	if err := b.Publish("telemetry/room1", []byte("hello"), map[string]string{qtypes.PropMessageID: "m1"}); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if calls != 1 {
		t.Fatalf("expected 1 cross-deliver call, got %d", calls)
	}
	if gotClientID != "mqtt-client" {
		t.Fatalf("expected cross-deliver to mqtt-client, got %q", gotClientID)
	}

	frames := readBrokerFramesFrom(t, buf, 0)
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
}

func TestForwardPublishSkipsCrossProtocolDispatch(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	buf := &bytes.Buffer{}

	c := &Connection{
		broker:   b,
		writer:   bufio.NewWriter(buf),
		frameMax: defaultFrameMax,
		logger:   logger,
		connID:   "conn-1",
		channels: make(map[uint16]*Channel),
	}
	ch := newChannel(c, 1)
	ch.consumers["ctag"] = &consumer{
		tag:        "ctag",
		queue:      "telemetry/#",
		mqttFilter: "telemetry/#",
		noAck:      true,
	}
	c.channels[1] = ch
	b.connections.Store(c.connID, c)

	if err := b.router.Subscribe(PrefixedClientID(c.connID), "telemetry/#", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("local subscribe failed: %v", err)
	}
	if err := b.router.Subscribe("mqtt-client", "telemetry/#", 1, storage.SubscribeOptions{}); err != nil {
		t.Fatalf("cross subscribe failed: %v", err)
	}

	calls := 0
	b.SetCrossDeliver(func(clientID string, topic string, payload []byte, qos byte, props map[string]string) {
		calls++
	})

	msg := &cluster.Message{
		Topic:      "telemetry/room1",
		Payload:    []byte("hello"),
		QoS:        1,
		Properties: map[string]string{qtypes.PropMessageID: "m1"},
	}
	if err := b.ForwardPublish(context.Background(), msg); err != nil {
		t.Fatalf("ForwardPublish failed: %v", err)
	}

	if calls != 0 {
		t.Fatalf("expected 0 cross-deliver calls on ForwardPublish, got %d", calls)
	}

	frames := readBrokerFramesFrom(t, buf, 0)
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
}

func TestPrefixedClientIDUsesCorePrefix(t *testing.T) {
	if got := PrefixedClientID("c1"); got != corebroker.PrefixedAMQP091ClientID("c1") {
		t.Fatalf("PrefixedClientID mismatch: got %q", got)
	}
}
