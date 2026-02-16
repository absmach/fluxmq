// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"connectrpc.com/connect"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
)

type stubForwardPublishHandler struct {
	errByTopic map[string]error
}

func (s *stubForwardPublishHandler) ForwardPublish(ctx context.Context, msg *Message) error {
	if s.errByTopic == nil {
		return nil
	}
	return s.errByTopic[msg.Topic]
}

func TestForwardPublishBatchNoHandler(t *testing.T) {
	tr := &Transport{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	req := connect.NewRequest(&clusterv1.ForwardPublishBatchRequest{
		Messages: []*clusterv1.ForwardPublishRequest{
			{Topic: "a/b"},
		},
	})
	resp, err := tr.ForwardPublishBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Msg.Success {
		t.Fatal("expected unsuccessful response")
	}
	if resp.Msg.Error == "" {
		t.Fatal("expected error message when handler is missing")
	}
}

func TestForwardPublishBatchReportsPartialFailures(t *testing.T) {
	topicErr := errors.New("delivery failed")
	tr := &Transport{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		forwardHandler: &stubForwardPublishHandler{
			errByTopic: map[string]error{
				"bad/topic": topicErr,
			},
		},
	}

	req := connect.NewRequest(&clusterv1.ForwardPublishBatchRequest{
		Messages: []*clusterv1.ForwardPublishRequest{
			nil,
			{Topic: "ok/topic", Payload: []byte("ok")},
			{Topic: "bad/topic", Payload: []byte("bad")},
		},
	})
	resp, err := tr.ForwardPublishBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Msg.Success {
		t.Fatal("expected unsuccessful response for partial failure")
	}
	if resp.Msg.Delivered != 1 {
		t.Fatalf("expected 1 delivered, got %d", resp.Msg.Delivered)
	}
	if len(resp.Msg.Failures) != 2 {
		t.Fatalf("expected 2 failures, got %d", len(resp.Msg.Failures))
	}
	if resp.Msg.Failures[0].Index != 0 || resp.Msg.Failures[0].Error == "" {
		t.Fatalf("unexpected failure[0]: %+v", resp.Msg.Failures[0])
	}
	if resp.Msg.Failures[1].Index != 2 || resp.Msg.Failures[1].Topic != "bad/topic" {
		t.Fatalf("unexpected failure[1]: %+v", resp.Msg.Failures[1])
	}
	if resp.Msg.Failures[1].Error != topicErr.Error() {
		t.Fatalf("expected failure error %q, got %q", topicErr.Error(), resp.Msg.Failures[1].Error)
	}
}
