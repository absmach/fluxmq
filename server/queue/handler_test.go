// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"sort"
	"testing"
	"time"

	"connectrpc.com/connect"
	queuev1 "github.com/absmach/fluxmq/pkg/proto/queue/v1"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestListQueuesFilteringAndPagination(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memlog.New()
	h := NewHandler(nil, store, nil, nil)

	for _, name := range []string{"alpha", "beta", "delta", "gamma"} {
		cfg := types.DefaultQueueConfig(name, "$queue/"+name+"/#")
		if err := store.CreateQueue(ctx, cfg); err != nil {
			t.Fatalf("create queue %q: %v", name, err)
		}
	}

	filteredResp, err := h.ListQueues(ctx, connect.NewRequest(&queuev1.ListQueuesRequest{
		Prefix: "g",
	}))
	if err != nil {
		t.Fatalf("list filtered queues: %v", err)
	}
	if len(filteredResp.Msg.Queues) != 1 || filteredResp.Msg.Queues[0].Name != "gamma" {
		t.Fatalf("unexpected filtered queues: %#v", filteredResp.Msg.Queues)
	}

	page1Resp, err := h.ListQueues(ctx, connect.NewRequest(&queuev1.ListQueuesRequest{
		Limit: 2,
	}))
	if err != nil {
		t.Fatalf("list queues page 1: %v", err)
	}
	if got := len(page1Resp.Msg.Queues); got != 2 {
		t.Fatalf("unexpected page 1 size: %d", got)
	}
	page1Names := []string{page1Resp.Msg.Queues[0].Name, page1Resp.Msg.Queues[1].Name}
	if !sort.StringsAreSorted(page1Names) {
		t.Fatalf("page 1 not sorted: %#v", page1Names)
	}
	if page1Resp.Msg.NextPageToken == "" {
		t.Fatalf("expected next_page_token on first page")
	}

	page2Resp, err := h.ListQueues(ctx, connect.NewRequest(&queuev1.ListQueuesRequest{
		Limit:     2,
		PageToken: page1Resp.Msg.NextPageToken,
	}))
	if err != nil {
		t.Fatalf("list queues page 2: %v", err)
	}
	if got := len(page2Resp.Msg.Queues); got != 2 {
		t.Fatalf("unexpected page 2 size: %d", got)
	}
	if page2Resp.Msg.NextPageToken != "" {
		t.Fatalf("expected empty next_page_token on last page, got %q", page2Resp.Msg.NextPageToken)
	}
	page2Names := []string{page2Resp.Msg.Queues[0].Name, page2Resp.Msg.Queues[1].Name}
	if !sort.StringsAreSorted(page2Names) {
		t.Fatalf("page 2 not sorted: %#v", page2Names)
	}
}

func TestUpdateQueueAppliesConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memlog.New()
	h := NewHandler(nil, store, nil, nil)

	cfg := types.DefaultQueueConfig("orders", "$queue/orders/#")
	if err := store.CreateQueue(ctx, cfg); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	retention := 2 * time.Hour
	updateResp, err := h.UpdateQueue(ctx, connect.NewRequest(&queuev1.UpdateQueueRequest{
		Name: "orders",
		Config: &queuev1.QueueConfig{
			Retention: &queuev1.RetentionConfig{
				MaxAge:      durationpb.New(retention),
				MaxBytes:    2048,
				MinMessages: 10,
			},
			MaxMessageSize: 4096,
		},
	}))
	if err != nil {
		t.Fatalf("update queue: %v", err)
	}

	updated, err := store.GetQueue(ctx, "orders")
	if err != nil {
		t.Fatalf("read updated queue: %v", err)
	}

	if updated.MessageTTL != retention {
		t.Fatalf("unexpected message ttl: got %v want %v", updated.MessageTTL, retention)
	}
	if updated.Retention.RetentionTime != retention {
		t.Fatalf("unexpected retention time: got %v want %v", updated.Retention.RetentionTime, retention)
	}
	if updated.Retention.RetentionBytes != 2048 {
		t.Fatalf("unexpected retention bytes: got %d", updated.Retention.RetentionBytes)
	}
	if updated.Retention.RetentionMessages != 10 {
		t.Fatalf("unexpected retention messages: got %d", updated.Retention.RetentionMessages)
	}
	if updated.MaxMessageSize != 4096 {
		t.Fatalf("unexpected max message size: got %d", updated.MaxMessageSize)
	}

	if got := updateResp.Msg.Config.GetRetention().GetMaxAge().AsDuration(); got != retention {
		t.Fatalf("response retention mismatch: got %v want %v", got, retention)
	}
}

func TestSeekToTimestamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memlog.New()
	h := NewHandler(nil, store, nil, nil)

	cfg := types.DefaultQueueConfig("events", "$queue/events/#")
	if err := store.CreateQueue(ctx, cfg); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	base := time.Unix(1700000000, 0).UTC()
	points := []time.Time{
		base,
		base.Add(1 * time.Minute),
		base.Add(2 * time.Minute),
	}
	for i, ts := range points {
		if _, err := store.Append(ctx, "events", &types.Message{
			ID:        "m",
			Topic:     "events",
			Payload:   []byte{byte(i)},
			CreatedAt: ts,
		}); err != nil {
			t.Fatalf("append message %d: %v", i, err)
		}
	}

	exactResp, err := h.SeekToTimestamp(ctx, connect.NewRequest(&queuev1.SeekToTimestampRequest{
		QueueName: "events",
		Timestamp: timestamppb.New(points[1]),
	}))
	if err != nil {
		t.Fatalf("seek exact: %v", err)
	}
	if exactResp.Msg.Offset != 1 || !exactResp.Msg.ExactMatch {
		t.Fatalf("unexpected exact seek response: %+v", exactResp.Msg)
	}

	between := points[1].Add(30 * time.Second)
	betweenResp, err := h.SeekToTimestamp(ctx, connect.NewRequest(&queuev1.SeekToTimestampRequest{
		QueueName: "events",
		Timestamp: timestamppb.New(between),
	}))
	if err != nil {
		t.Fatalf("seek in-between: %v", err)
	}
	if betweenResp.Msg.Offset != 2 || betweenResp.Msg.ExactMatch {
		t.Fatalf("unexpected in-between seek response: %+v", betweenResp.Msg)
	}

	afterLast := points[2].Add(1 * time.Second)
	afterResp, err := h.SeekToTimestamp(ctx, connect.NewRequest(&queuev1.SeekToTimestampRequest{
		QueueName: "events",
		Timestamp: timestamppb.New(afterLast),
	}))
	if err != nil {
		t.Fatalf("seek after last: %v", err)
	}
	if afterResp.Msg.Offset != 3 || afterResp.Msg.ExactMatch {
		t.Fatalf("unexpected tail seek response: %+v", afterResp.Msg)
	}
	if got := afterResp.Msg.Timestamp.AsTime(); !got.Equal(afterLast) {
		t.Fatalf("unexpected tail seek timestamp: got %v want %v", got, afterLast)
	}

	_, err = h.SeekToTimestamp(ctx, connect.NewRequest(&queuev1.SeekToTimestampRequest{
		QueueName: "events",
	}))
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("unexpected code for missing timestamp: got %s want %s", got, connect.CodeInvalidArgument)
	}
}
