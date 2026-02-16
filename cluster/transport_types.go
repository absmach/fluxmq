// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"connectrpc.com/connect"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
)

type (
	PublishReq            = connect.Request[clusterv1.PublishRequest]
	PublishResp           = connect.Response[clusterv1.PublishResponse]
	PublishBatchReq       = connect.Request[clusterv1.PublishBatchRequest]
	PublishBatchResp      = connect.Response[clusterv1.PublishBatchResponse]
	TakeoverReq           = connect.Request[clusterv1.TakeoverRequest]
	TakeoverResp          = connect.Response[clusterv1.TakeoverResponse]
	FetchRetainedReq      = connect.Request[clusterv1.FetchRetainedRequest]
	FetchRetainedResp     = connect.Response[clusterv1.FetchRetainedResponse]
	FetchWillReq          = connect.Request[clusterv1.FetchWillRequest]
	FetchWillResp         = connect.Response[clusterv1.FetchWillResponse]
	EnqueueRemoteReq      = connect.Request[clusterv1.EnqueueRemoteRequest]
	EnqueueRemoteResp     = connect.Response[clusterv1.EnqueueRemoteResponse]
	RouteQueueMessageReq  = connect.Request[clusterv1.RouteQueueMessageRequest]
	RouteQueueMessageResp = connect.Response[clusterv1.RouteQueueMessageResponse]
	RouteQueueBatchReq    = connect.Request[clusterv1.RouteQueueBatchRequest]
	RouteQueueBatchResp   = connect.Response[clusterv1.RouteQueueBatchResponse]
	AppendEntriesReq      = connect.Request[clusterv1.AppendEntriesRequest]
	AppendEntriesResp     = connect.Response[clusterv1.AppendEntriesResponse]
	RequestVoteReq        = connect.Request[clusterv1.RequestVoteRequest]
	RequestVoteResp       = connect.Response[clusterv1.RequestVoteResponse]
	InstallSnapshotReq    = connect.Request[clusterv1.InstallSnapshotRequest]
	InstallSnapshotResp   = connect.Response[clusterv1.InstallSnapshotResponse]
	ForwardGroupOpReq          = connect.Request[clusterv1.ForwardGroupOpRequest]
	ForwardGroupOpResp         = connect.Response[clusterv1.ForwardGroupOpResponse]
	ForwardPublishBatchReq     = connect.Request[clusterv1.ForwardPublishBatchRequest]
	ForwardPublishBatchResp    = connect.Response[clusterv1.ForwardPublishBatchResponse]
)
