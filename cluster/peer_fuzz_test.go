// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"connectrpc.com/connect"
	"github.com/absmach/fluxmq/message"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	queueTypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
	"google.golang.org/protobuf/proto"
)

const peerFuzzQueue = "tasks"

// peerSink accepts whatever a peer request delivers and honours each
// interface's ownership contract, so the fuzzer exercises decoding and
// dispatch rather than a handler.
type peerSink struct{}

func (peerSink) DeliverToClient(_ context.Context, _ string, msg *message.Envelope) error {
	message.Release(msg)
	return nil
}

func (peerSink) GetSessionStateAndClose(context.Context, string, *SessionIdentityGuard) (*clusterv1.SessionState, error) {
	return nil, nil
}

func (peerSink) HandleSessionLeaseLost(context.Context, []string) {}

func (peerSink) GetRetainedMessage(context.Context, string) (*message.Envelope, error) {
	return nil, nil
}

func (peerSink) GetWillMessage(context.Context, string) (*storage.WillMessage, error) {
	return nil, nil
}

func (peerSink) EnqueueLocal(context.Context, string, *message.Envelope) error { return nil }

func (peerSink) DeliverQueueMessage(_ context.Context, _ string, msg *message.Envelope) error {
	message.Release(msg)
	return nil
}

func (peerSink) HandleQueuePublish(context.Context, *message.Envelope, queueTypes.PublishMode, []string) error {
	return nil
}

func (peerSink) HandleForwardedGroupOp(context.Context, string, *clusterv1.GroupOperation) error {
	return nil
}

func (peerSink) ForwardPublish(_ context.Context, msg *message.Envelope) error {
	message.Release(msg)
	return nil
}

// peerRequest decodes data as one kind of inbound peer RPC and serves it.
type peerRequest func(t *Transport, data []byte) error

func servePeer[Req any, Resp any](
	handle func(*Transport, context.Context, *connect.Request[Req]) (*connect.Response[Resp], error),
) peerRequest {
	return func(t *Transport, data []byte) error {
		req := new(Req)
		msg, ok := any(req).(proto.Message)
		if !ok {
			panic("request is not a protobuf message")
		}
		// Connect rejects bytes that are not a valid message before any
		// handler runs, so only well-formed protobuf reaches one.
		if err := proto.Unmarshal(data, msg); err != nil {
			return nil
		}
		_, err := handle(t, context.Background(), connect.NewRequest(req))
		return err
	}
}

var peerRequests = []peerRequest{
	servePeer((*Transport).RoutePublish),
	servePeer((*Transport).RoutePublishBatch),
	servePeer((*Transport).TakeoverSession),
	servePeer((*Transport).FetchRetained),
	servePeer((*Transport).FetchWill),
	servePeer((*Transport).EnqueueRemote),
	servePeer((*Transport).RouteQueueMessage),
	servePeer((*Transport).RouteQueueBatch),
	servePeer((*Transport).ForwardGroupOp),
	servePeer((*Transport).ForwardPublishBatch),
}

// FuzzPeerRequests serves every inbound peer RPC with whatever a faulty or
// hostile cluster node could send: any well-formed protobuf request, with
// arbitrary envelopes, identifiers and counts. No request may panic the
// receiving node.
func FuzzPeerRequests(f *testing.F) {
	valid := message.NewDelivery("devices/1", []byte("payload"), 1, false)
	valid.BrokerMeta.Queue.Name = peerFuzzQueue
	envelope, err := message.MarshalBinary(valid)
	message.Release(valid)
	if err != nil {
		f.Fatal(err)
	}

	seeds := []proto.Message{
		&clusterv1.PublishRequest{ClientId: "c1", Envelope: envelope},
		&clusterv1.PublishBatchRequest{Messages: []*clusterv1.PublishRequest{{ClientId: "c1", Envelope: envelope}, nil, {Envelope: []byte{0xff}}}},
		&clusterv1.TakeoverRequest{ClientId: "c1"},
		&clusterv1.FetchRetainedRequest{Topic: "devices/#"},
		&clusterv1.FetchWillRequest{ClientId: "c1"},
		&clusterv1.EnqueueRemoteRequest{QueueName: peerFuzzQueue, Envelope: envelope},
		&clusterv1.RouteQueueMessageRequest{ClientId: "c1", QueueName: peerFuzzQueue, Envelope: envelope},
		&clusterv1.RouteQueueBatchRequest{Messages: []*clusterv1.RouteQueueMessageRequest{{ClientId: "c1", QueueName: peerFuzzQueue, Envelope: envelope}, nil}},
		&clusterv1.ForwardGroupOpRequest{QueueName: peerFuzzQueue},
		&clusterv1.ForwardPublishBatchRequest{Messages: []*clusterv1.ForwardPublishRequest{{Envelope: envelope}, nil, {Envelope: envelope[:len(envelope)/2]}}},
	}
	for kind, seed := range seeds {
		data, err := proto.Marshal(seed)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(uint8(kind), data)
		f.Add(uint8(kind), []byte{})
	}

	tr := &Transport{
		nodeID:  "self",
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		handler: peerSink{},
		stopCh:  make(chan struct{}),
	}
	tr.SetQueueHandler(peerSink{})
	tr.SetForwardPublishHandler(peerSink{})

	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		_ = peerRequests[int(kind)%len(peerRequests)](tr, data)
	})
}
