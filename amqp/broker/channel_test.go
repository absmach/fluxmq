// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bufio"
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/absmach/fluxmq/amqp/codec"
	qtypes "github.com/absmach/fluxmq/queue/types"
)

func newTestChannel(t *testing.T) (*Channel, *bytes.Buffer) {
	t.Helper()

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(nil, logger)
	c := &Connection{
		broker:   b,
		writer:   bufio.NewWriter(buf),
		frameMax: defaultFrameMax,
		logger:   logger,
		connID:   "test-conn",
		channels: make(map[uint16]*Channel),
	}
	ch := newChannel(c, 1)
	return ch, buf
}

func readFramesFrom(t *testing.T, buf *bytes.Buffer, start int) []*codec.Frame {
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

func TestPublishStateMachine_HeaderWithoutPublish(t *testing.T) {
	ch, buf := newTestChannel(t)

	header := &codec.ContentHeader{
		ClassID:  codec.ClassBasic,
		Weight:   0,
		BodySize: 0,
	}
	var payload bytes.Buffer
	if err := header.WriteContentHeader(&payload); err != nil {
		t.Fatalf("WriteContentHeader failed: %v", err)
	}

	ch.handleHeaderFrame(&codec.Frame{
		Type:    codec.FrameHeader,
		Channel: 1,
		Payload: payload.Bytes(),
	})

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	closeMsg, ok := decoded.(*codec.ChannelClose)
	if !ok {
		t.Fatalf("expected ChannelClose, got %T", decoded)
	}
	if closeMsg.ReplyCode != codec.UnexpectedFrame {
		t.Fatalf("expected UnexpectedFrame, got %d", closeMsg.ReplyCode)
	}
}

func TestPublishStateMachine_BodyWithoutHeader(t *testing.T) {
	ch, buf := newTestChannel(t)

	ch.handleBodyFrame(&codec.Frame{
		Type:    codec.FrameBody,
		Channel: 1,
		Payload: []byte("payload"),
	})

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	closeMsg, ok := decoded.(*codec.ChannelClose)
	if !ok {
		t.Fatalf("expected ChannelClose, got %T", decoded)
	}
	if closeMsg.ReplyCode != codec.UnexpectedFrame {
		t.Fatalf("expected UnexpectedFrame, got %d", closeMsg.ReplyCode)
	}
}

func TestMandatoryPublishReturn(t *testing.T) {
	ch, buf := newTestChannel(t)
	ch.confirmMode = true

	if err := ch.handleMethod(&codec.BasicPublish{
		Exchange:   "",
		RoutingKey: "no.route",
		Mandatory:  true,
	}); err != nil {
		t.Fatalf("handleMethod failed: %v", err)
	}

	payload := []byte("hello")
	header := &codec.ContentHeader{
		ClassID:  codec.ClassBasic,
		Weight:   0,
		BodySize: uint64(len(payload)),
		Properties: codec.BasicProperties{
			MessageID: "mid-1",
		},
	}
	var headerBuf bytes.Buffer
	if err := header.WriteContentHeader(&headerBuf); err != nil {
		t.Fatalf("WriteContentHeader failed: %v", err)
	}

	ch.handleHeaderFrame(&codec.Frame{
		Type:    codec.FrameHeader,
		Channel: 1,
		Payload: headerBuf.Bytes(),
	})
	ch.handleBodyFrame(&codec.Frame{
		Type:    codec.FrameBody,
		Channel: 1,
		Payload: payload,
	})

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 4 {
		t.Fatalf("expected 4 frames, got %d", len(frames))
	}
	decoded0, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if _, ok := decoded0.(*codec.BasicReturn); !ok {
		t.Fatalf("expected BasicReturn, got %T", decoded0)
	}
	if frames[1].Type != codec.FrameHeader {
		t.Fatalf("expected header frame, got %d", frames[1].Type)
	}
	if frames[2].Type != codec.FrameBody {
		t.Fatalf("expected body frame, got %d", frames[2].Type)
	}
	decoded3, err := frames[3].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if _, ok := decoded3.(*codec.BasicAck); !ok {
		t.Fatalf("expected BasicAck, got %T", decoded3)
	}
}

func TestPrefetchBuffering(t *testing.T) {
	ch, buf := newTestChannel(t)
	ch.prefetchCount = 1
	ch.consumers["ctag"] = &consumer{
		tag:   "ctag",
		queue: "q",
		noAck: false,
	}

	props := map[string]string{"message-id": "m1"}
	ch.deliverMessage("q", []byte("one"), props)
	ch.deliverMessage("q", []byte("two"), props)

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames for first delivery, got %d", len(frames))
	}
	if _, err := frames[0].Decode(); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	beforeAck := buf.Len()
	if err := ch.handleBasicAck(&codec.BasicAck{DeliveryTag: 1}); err != nil {
		t.Fatalf("handleBasicAck failed: %v", err)
	}
	framesAfter := readFramesFrom(t, buf, beforeAck)
	if len(framesAfter) != 3 {
		t.Fatalf("expected 3 frames after ack, got %d", len(framesAfter))
	}
}

func TestChannelFlowQueueing(t *testing.T) {
	ch, buf := newTestChannel(t)
	ch.flow = false
	ch.consumers["ctag"] = &consumer{
		tag:   "ctag",
		queue: "q",
		noAck: true,
	}

	ch.deliverMessage("q", []byte("one"), map[string]string{})
	if buf.Len() != 0 {
		t.Fatalf("expected no frames while flow is disabled")
	}

	if err := ch.handleMethod(&codec.ChannelFlow{Active: true}); err != nil {
		t.Fatalf("handleMethod failed: %v", err)
	}

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 4 {
		t.Fatalf("expected 4 frames after flow resume, got %d", len(frames))
	}
	decoded0, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if _, ok := decoded0.(*codec.ChannelFlowOk); !ok {
		t.Fatalf("expected ChannelFlowOk, got %T", decoded0)
	}
}

func TestExchangeNotFoundOnPublish(t *testing.T) {
	ch, buf := newTestChannel(t)

	if err := ch.handleMethod(&codec.BasicPublish{
		Exchange:   "missing",
		RoutingKey: "rk",
	}); err != nil {
		t.Fatalf("handleMethod failed: %v", err)
	}

	frames := readFramesFrom(t, buf, 0)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	decoded, err := frames[0].Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	closeMsg, ok := decoded.(*codec.ChannelClose)
	if !ok {
		t.Fatalf("expected ChannelClose, got %T", decoded)
	}
	if closeMsg.ReplyCode != codec.NotFound {
		t.Fatalf("expected NotFound, got %d", closeMsg.ReplyCode)
	}
}

func TestConsumerQueueMatches(t *testing.T) {
	ch, _ := newTestChannel(t)

	tests := []struct {
		name  string
		cons  consumer
		topic string
		want  bool
	}{
		{
			name: "stream queue root topic",
			cons: consumer{
				queue:     "demo-events",
				queueName: "demo-events",
				pattern:   "",
			},
			topic: "$queue/demo-events",
			want:  true,
		},
		{
			name: "stream queue routing key",
			cons: consumer{
				queue:     "demo-events",
				queueName: "demo-events",
				pattern:   "",
			},
			topic: "$queue/demo-events/user/action",
			want:  true,
		},
		{
			name: "queue filter root",
			cons: consumer{
				queue:     "$queue/demo-orders/#",
				queueName: "demo-orders",
				pattern:   "#",
			},
			topic: "$queue/demo-orders",
			want:  true,
		},
		{
			name: "queue filter with routing key",
			cons: consumer{
				queue:     "$queue/demo-orders/#",
				queueName: "demo-orders",
				pattern:   "#",
			},
			topic: "$queue/demo-orders/new",
			want:  true,
		},
		{
			name: "queue filter mismatch",
			cons: consumer{
				queue:     "$queue/demo-orders/#",
				queueName: "demo-orders",
				pattern:   "#",
			},
			topic: "$queue/other/new",
			want:  false,
		},
		{
			name: "plain topic fallback",
			cons: consumer{
				queue: "sensor/#",
			},
			topic: "sensor/temperature",
			want:  true,
		},
		{
			name: "plain topic mismatch",
			cons: consumer{
				queue: "sensor/#",
			},
			topic: "control/restart",
			want:  false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := ch.consumerQueueMatches(&tc.cons, tc.topic)
			if got != tc.want {
				t.Fatalf("consumerQueueMatches() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseStreamOffsetString(t *testing.T) {
	first, ok := parseStreamOffsetString("first")
	if !ok || first.Position != qtypes.CursorEarliest {
		t.Fatalf("expected earliest, got %+v", first)
	}

	last, ok := parseStreamOffsetString("last")
	if !ok || last.Position != qtypes.CursorLatest {
		t.Fatalf("expected latest, got %+v", last)
	}

	offset, ok := parseStreamOffsetString("offset=42")
	if !ok || offset.Position != qtypes.CursorOffset || offset.Offset != 42 {
		t.Fatalf("expected offset 42, got %+v", offset)
	}

	ts, ok := parseStreamOffsetString("timestamp=1700000000")
	if !ok || ts.Position != qtypes.CursorTimestamp || ts.Timestamp.IsZero() {
		t.Fatalf("expected timestamp, got %+v", ts)
	}
}
