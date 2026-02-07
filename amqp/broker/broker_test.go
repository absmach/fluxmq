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
	"github.com/absmach/fluxmq/cluster"
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
		tag:   "ctag",
		queue: "telemetry/#",
		noAck: true,
	}
	c.channels[1] = ch
	b.connections.Store(c.connID, c)

	msg := &cluster.Message{
		Topic:      "telemetry/room1",
		Payload:    []byte("hello"),
		Properties: map[string]string{"message-id": "m1"},
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
