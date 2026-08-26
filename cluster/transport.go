// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/message"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/pkg/proto/cluster/v1/clusterv1connect"
	queueTypes "github.com/absmach/fluxmq/queue/types"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const (
	errNoHandler      = "no handler configured"
	errMessageIsNil   = "message is nil"
	errNoQueueHandler = "no queue handler configured"
)

// QueueHandler defines callbacks for queue distribution operations.
type QueueHandler interface {
	// EnqueueLocal enqueues a message on this node (called by remote RPC).
	EnqueueLocal(ctx context.Context, queueName string, payload []byte, properties map[string]string) error

	// DeliverQueueMessage delivers a queue message to a local consumer and takes
	// ownership of msg on every return path.
	DeliverQueueMessage(ctx context.Context, clientID string, msg *message.Envelope) error

	// HandleQueuePublish handles a publish with the given mode.
	HandleQueuePublish(ctx context.Context, publish queueTypes.PublishRequest, mode queueTypes.PublishMode) error

	// HandleForwardedGroupOp applies a consumer group mutation that was
	// forwarded from a follower. This node is expected to be the Raft leader
	// for the queue's group.
	HandleForwardedGroupOp(ctx context.Context, queueName string, op *clusterv1.GroupOperation) error
}

// Transport handles inter-broker communication using Connect protocol.
type Transport struct {
	mu             sync.RWMutex
	nodeID         string
	bindAddr       string
	httpServer     *http.Server
	listener       net.Listener
	peerClients    map[string]clusterv1connect.BrokerServiceClient
	breakers       *peerBreakers
	logger         *slog.Logger
	handler        MessageHandler
	queueHandler   QueueHandler
	forwardHandler ForwardPublishHandler
	stopCh         chan struct{}
	tlsConfig      *TransportTLSConfig
	httpClient     *http.Client
}

// NewTransport creates a new Connect transport.
// If tlsCfg is nil, the transport uses insecure connections (development mode only).
func NewTransport(nodeID, bindAddr string, handler MessageHandler, tlsCfg *TransportTLSConfig, logger *slog.Logger) (*Transport, error) {
	var listener net.Listener
	var httpClient *http.Client
	var err error

	t := &Transport{
		nodeID:      nodeID,
		bindAddr:    bindAddr,
		peerClients: make(map[string]clusterv1connect.BrokerServiceClient),
		breakers:    newPeerBreakers(),
		logger:      logger,
		handler:     handler,
		stopCh:      make(chan struct{}),
		tlsConfig:   tlsCfg,
	}

	if tlsCfg != nil {
		serverTLSConfig, clientTLSConfig, err := LoadMutualTLSConfigs(tlsCfg)
		if err != nil {
			return nil, err
		}

		// Create TLS listener
		listener, err = tls.Listen("tcp", bindAddr, serverTLSConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS listener on %s: %w", bindAddr, err)
		}

		// Create HTTP client with TLS for peer connections
		httpClient = &http.Client{
			Transport: &http2.Transport{
				TLSClientConfig: clientTLSConfig,
			},
			Timeout: 30 * time.Second,
		}

		logger.Info("transport TLS enabled", slog.String("address", bindAddr))
	} else {
		// Insecure mode (development only)
		listener, err = net.Listen("tcp", bindAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to listen on %s: %w", bindAddr, err)
		}

		// Create HTTP client for insecure connections
		httpClient = &http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
			Timeout: 30 * time.Second,
		}

		logger.Warn("transport TLS disabled - using insecure connections (development mode only)")
	}

	t.listener = listener
	t.httpClient = httpClient

	// Create Connect handler
	mux := http.NewServeMux()
	path, connectHandler := clusterv1connect.NewBrokerServiceHandler(t)
	mux.Handle(path, connectHandler)

	// Create HTTP server with h2c support for HTTP/2 without TLS
	var httpHandler http.Handler
	if tlsCfg == nil {
		httpHandler = h2c.NewHandler(mux, &http2.Server{})
	} else {
		httpHandler = mux
	}

	t.httpServer = &http.Server{
		Handler:           httpHandler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       2 * time.Minute,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       5 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}

	return t, nil
}

// Start starts the HTTP server.
func (t *Transport) Start() error {
	go func() {
		t.logger.Info("starting Connect transport server", slog.String("address", t.bindAddr))
		if err := t.httpServer.Serve(t.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.logger.Error("HTTP server error", slog.String("error", err.Error()))
		}
	}()
	return nil
}

// Stop gracefully stops the HTTP server.
func (t *Transport) Stop() error {
	close(t.stopCh)

	// Shutdown HTTP server first to stop accepting new requests
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var err error
	if t.httpServer != nil {
		err = t.httpServer.Shutdown(ctx)
	}

	// Clear peer connections after server is stopped (no more in-flight RPCs)
	t.mu.Lock()
	t.peerClients = make(map[string]clusterv1connect.BrokerServiceClient)
	t.mu.Unlock()

	return err
}

// ConnectPeer establishes a Connect client connection to a peer node.
func (t *Transport) ConnectPeer(nodeID, addr string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if already connected
	if _, exists := t.peerClients[nodeID]; exists {
		return nil
	}

	// Determine URL scheme based on TLS config
	scheme := "http"
	if t.tlsConfig != nil {
		scheme = "https"
	}
	baseURL := fmt.Sprintf("%s://%s", scheme, addr)

	// Create Connect client
	client := clusterv1connect.NewBrokerServiceClient(t.httpClient, baseURL)
	t.peerClients[nodeID] = client

	t.logger.Info("connected to peer",
		slog.String("node_id", nodeID),
		slog.String("address", addr),
		slog.Bool("tls_enabled", t.tlsConfig != nil))
	return nil
}

// GetPeerClient returns the Connect client for a peer node.
func (t *Transport) GetPeerClient(nodeID string) (clusterv1connect.BrokerServiceClient, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	client, exists := t.peerClients[nodeID]
	if !exists {
		return nil, fmt.Errorf("no connection to peer %s", nodeID)
	}

	return client, nil
}

// HasPeerConnection checks if we have an active connection to a peer.
func (t *Transport) HasPeerConnection(nodeID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	_, exists := t.peerClients[nodeID]
	return exists
}

// RoutePublish implements BrokerServiceHandler.RoutePublish.
func (t *Transport) RoutePublish(ctx context.Context, req *PublishReq) (*PublishResp, error) {
	if t.handler == nil {
		return connect.NewResponse(&clusterv1.PublishResponse{
			Success: false,
			Error:   errNoHandler,
		}), nil
	}

	msg := envelopeFromWire(req.Msg.Topic, req.Msg.Payload, byte(req.Msg.Qos), req.Msg.Retain, req.Msg.Dup, req.Msg.Properties)

	err := t.handler.DeliverToClient(ctx, req.Msg.ClientId, msg)
	if err != nil {
		return connect.NewResponse(&clusterv1.PublishResponse{
			Success: false,
			Error:   err.Error(),
		}), nil
	}

	return connect.NewResponse(&clusterv1.PublishResponse{
		Success: true,
	}), nil
}

// RoutePublishBatch implements BrokerServiceHandler.RoutePublishBatch.
func (t *Transport) RoutePublishBatch(ctx context.Context, req *PublishBatchReq) (*PublishBatchResp, error) {
	if t.handler == nil {
		return connect.NewResponse(&clusterv1.PublishBatchResponse{
			Success: false,
			Error:   errNoHandler,
		}), nil
	}

	var (
		delivered uint32
		failures  []*clusterv1.PublishBatchError
	)

	for idx, m := range req.Msg.Messages {
		if m == nil {
			failures = append(failures, &clusterv1.PublishBatchError{
				Index: uint32(idx),
				Error: errMessageIsNil,
			})
			continue
		}

		msg := envelopeFromWire(m.Topic, m.Payload, byte(m.Qos), m.Retain, m.Dup, m.Properties)

		if err := t.handler.DeliverToClient(ctx, m.ClientId, msg); err != nil {
			failures = append(failures, &clusterv1.PublishBatchError{
				Index:    uint32(idx),
				ClientId: m.ClientId,
				Error:    err.Error(),
			})
			continue
		}
		delivered++
	}

	success := len(failures) == 0
	resp := &clusterv1.PublishBatchResponse{
		Success:   success,
		Delivered: delivered,
		Failures:  failures,
	}
	if !success {
		resp.Error = "one or more publish deliveries failed"
	}

	return connect.NewResponse(resp), nil
}

// TakeoverSession implements BrokerServiceHandler.TakeoverSession.
func (t *Transport) TakeoverSession(ctx context.Context, req *TakeoverReq) (*TakeoverResp, error) {
	if t.handler == nil {
		return connect.NewResponse(&clusterv1.TakeoverResponse{
			Success: false,
			Error:   errNoHandler,
		}), nil
	}

	sessionState, err := t.handler.GetSessionStateAndClose(ctx, req.Msg.ClientId)
	if err != nil {
		return connect.NewResponse(&clusterv1.TakeoverResponse{
			Success: false,
			Error:   err.Error(),
		}), nil
	}

	return connect.NewResponse(&clusterv1.TakeoverResponse{
		Success:      true,
		SessionState: sessionState,
	}), nil
}

// FetchRetained implements BrokerServiceHandler.FetchRetained.
func (t *Transport) FetchRetained(ctx context.Context, req *FetchRetainedReq) (*FetchRetainedResp, error) {
	if t.handler == nil {
		return connect.NewResponse(&clusterv1.FetchRetainedResponse{
			Found: false,
			Error: errNoHandler,
		}), nil
	}

	msg, err := t.handler.GetRetainedMessage(ctx, req.Msg.Topic)
	if err != nil {
		return connect.NewResponse(&clusterv1.FetchRetainedResponse{
			Found: false,
			Error: err.Error(),
		}), nil
	}

	if msg == nil {
		return connect.NewResponse(&clusterv1.FetchRetainedResponse{
			Found: false,
		}), nil
	}
	defer message.Release(msg)

	grpcMsg := &clusterv1.RetainedMessage{
		Topic:      msg.Topic,
		Payload:    bytes.Clone(msg.PayloadBytes()),
		Qos:        uint32(msg.Broker.Delivery.QoS),
		Retain:     msg.Broker.Delivery.Retain,
		Properties: message.ProjectProperties(msg, message.TrustedServiceProjection),
		Timestamp:  msg.Broker.Delivery.PublishedAt.Unix(),
	}

	return connect.NewResponse(&clusterv1.FetchRetainedResponse{
		Found:   true,
		Message: grpcMsg,
	}), nil
}

// FetchWill implements BrokerServiceHandler.FetchWill.
func (t *Transport) FetchWill(ctx context.Context, req *FetchWillReq) (*FetchWillResp, error) {
	if t.handler == nil {
		return connect.NewResponse(&clusterv1.FetchWillResponse{
			Found: false,
			Error: errNoHandler,
		}), nil
	}

	will, err := t.handler.GetWillMessage(ctx, req.Msg.ClientId)
	if err != nil {
		return connect.NewResponse(&clusterv1.FetchWillResponse{
			Found: false,
			Error: err.Error(),
		}), nil
	}

	if will == nil {
		return connect.NewResponse(&clusterv1.FetchWillResponse{
			Found: false,
		}), nil
	}

	grpcWill := &clusterv1.WillMessage{
		Topic:   will.Topic,
		Payload: will.Payload,
		Qos:     uint32(will.QoS),
		Retain:  will.Retain,
		Delay:   will.Delay,
	}

	return connect.NewResponse(&clusterv1.FetchWillResponse{
		Found:   true,
		Message: grpcWill,
	}), nil
}

// EnqueueRemote implements BrokerServiceHandler.EnqueueRemote.
func (t *Transport) EnqueueRemote(ctx context.Context, req *EnqueueRemoteReq) (*EnqueueRemoteResp, error) {
	t.mu.RLock()
	handler := t.queueHandler
	t.mu.RUnlock()

	if handler == nil {
		return connect.NewResponse(&clusterv1.EnqueueRemoteResponse{
			Success: false,
			Error:   errNoQueueHandler,
		}), nil
	}

	forwardedPublish := req.Msg.ForwardedPublish
	forwardToLeader := req.Msg.ForwardToLeader

	// Check if this is a forwarded publish (topic-based) vs direct enqueue (queue-based)
	if forwardedPublish {
		// This is a forwarded publish - handle with mode
		topic := req.Msg.QueueName // topic is passed in queueName field for forwards
		mode := queueTypes.PublishForwarded
		if forwardToLeader {
			mode = queueTypes.PublishNormal
		}

		err := handler.HandleQueuePublish(ctx, queueTypes.PublishRequest{
			Source:              message.SourceFromProperties(req.Msg.Properties),
			Trace:               message.TraceFromProperties(req.Msg.Properties),
			Topic:               topic,
			Payload:             req.Msg.Payload,
			Properties:          message.FilterUserProperties(req.Msg.Properties),
			ForwardTargetQueues: splitPropertyList(req.Msg.Properties[message.PropertyForwardTargetQueues]),
		}, mode)
		if err != nil {
			return connect.NewResponse(&clusterv1.EnqueueRemoteResponse{
				Success: false,
				Error:   err.Error(),
			}), nil
		}
		return connect.NewResponse(&clusterv1.EnqueueRemoteResponse{
			Success: true,
		}), nil
	}

	// Standard enqueue to a specific queue
	err := handler.EnqueueLocal(ctx, req.Msg.QueueName, req.Msg.Payload, req.Msg.Properties)
	if err != nil {
		return connect.NewResponse(&clusterv1.EnqueueRemoteResponse{
			Success: false,
			Error:   err.Error(),
		}), nil
	}

	return connect.NewResponse(&clusterv1.EnqueueRemoteResponse{
		Success: true,
	}), nil
}

// RouteQueueMessage implements BrokerServiceHandler.RouteQueueMessage.
func (t *Transport) RouteQueueMessage(ctx context.Context, req *RouteQueueMessageReq) (*RouteQueueMessageResp, error) {
	t.mu.RLock()
	handler := t.queueHandler
	t.mu.RUnlock()

	if handler == nil {
		return connect.NewResponse(&clusterv1.RouteQueueMessageResponse{
			Success: false,
			Error:   errNoQueueHandler,
		}), nil
	}

	msg, err := decodeRouteQueueMessage(req.Msg)
	if err != nil {
		return connect.NewResponse(&clusterv1.RouteQueueMessageResponse{
			Success: false,
			Error:   err.Error(),
		}), nil
	}

	err = handler.DeliverQueueMessage(ctx, req.Msg.ClientId, msg)
	if err != nil {
		return connect.NewResponse(&clusterv1.RouteQueueMessageResponse{
			Success:            false,
			Error:              err.Error(),
			ClientNotConnected: corebroker.IsErrClientNotConnected(err),
		}), nil
	}

	return connect.NewResponse(&clusterv1.RouteQueueMessageResponse{
		Success: true,
	}), nil
}

// RouteQueueBatch implements BrokerServiceHandler.RouteQueueBatch.
func (t *Transport) RouteQueueBatch(ctx context.Context, req *RouteQueueBatchReq) (*RouteQueueBatchResp, error) {
	t.mu.RLock()
	handler := t.queueHandler
	t.mu.RUnlock()

	if handler == nil {
		return connect.NewResponse(&clusterv1.RouteQueueBatchResponse{
			Success: false,
			Error:   errNoQueueHandler,
		}), nil
	}

	var (
		delivered uint32
		failures  []*clusterv1.RouteQueueBatchError
	)

	for idx, wire := range req.Msg.Messages {
		if wire == nil {
			failures = append(failures, &clusterv1.RouteQueueBatchError{
				Index: uint32(idx),
				Error: errMessageIsNil,
			})
			continue
		}

		msg, err := decodeRouteQueueMessage(wire)
		if err != nil {
			failures = append(failures, &clusterv1.RouteQueueBatchError{
				Index:     uint32(idx),
				ClientId:  wire.ClientId,
				QueueName: wire.QueueName,
				Error:     err.Error(),
			})
			continue
		}
		if err := handler.DeliverQueueMessage(ctx, wire.ClientId, msg); err != nil {
			failures = append(failures, &clusterv1.RouteQueueBatchError{
				Index:              uint32(idx),
				ClientId:           wire.ClientId,
				QueueName:          wire.QueueName,
				Error:              err.Error(),
				ClientNotConnected: corebroker.IsErrClientNotConnected(err),
			})
			continue
		}
		delivered++
	}

	success := len(failures) == 0
	resp := &clusterv1.RouteQueueBatchResponse{
		Success:   success,
		Delivered: delivered,
		Failures:  failures,
	}
	if !success {
		resp.Error = "one or more queue deliveries failed"
	}
	return connect.NewResponse(resp), nil
}

// ForwardGroupOp implements BrokerServiceHandler.ForwardGroupOp.
func (t *Transport) ForwardGroupOp(ctx context.Context, req *ForwardGroupOpReq) (*ForwardGroupOpResp, error) {
	t.mu.RLock()
	handler := t.queueHandler
	t.mu.RUnlock()

	if handler == nil {
		return connect.NewResponse(&clusterv1.ForwardGroupOpResponse{
			Success: false,
			Error:   errNoQueueHandler,
		}), nil
	}

	if err := handler.HandleForwardedGroupOp(ctx, req.Msg.QueueName, req.Msg.Operation); err != nil {
		return connect.NewResponse(&clusterv1.ForwardGroupOpResponse{
			Success: false,
			Error:   err.Error(),
		}), nil
	}

	return connect.NewResponse(&clusterv1.ForwardGroupOpResponse{
		Success: true,
	}), nil
}

// ForwardPublishBatch implements BrokerServiceHandler.ForwardPublishBatch.
func (t *Transport) ForwardPublishBatch(ctx context.Context, req *ForwardPublishBatchReq) (*ForwardPublishBatchResp, error) {
	t.mu.RLock()
	handler := t.forwardHandler
	t.mu.RUnlock()

	if handler == nil {
		return connect.NewResponse(&clusterv1.ForwardPublishBatchResponse{
			Success: false,
			Error:   "no forward publish handler configured",
		}), nil
	}

	var (
		delivered uint32
		failures  []*clusterv1.ForwardPublishBatchError
	)
	for idx, m := range req.Msg.Messages {
		if m == nil {
			failures = append(failures, &clusterv1.ForwardPublishBatchError{
				Index: uint32(idx),
				Error: errMessageIsNil,
			})
			continue
		}

		msg := envelopeFromWire(m.Topic, m.Payload, byte(m.Qos), m.Retain, false, m.Properties)

		if err := handler.ForwardPublish(ctx, msg); err != nil {
			t.logger.Warn("forward publish delivery failed",
				slog.String("topic", m.Topic),
				slog.String("error", err.Error()))
			failures = append(failures, &clusterv1.ForwardPublishBatchError{
				Index: uint32(idx),
				Topic: m.Topic,
				Error: err.Error(),
			})
			continue
		}
		delivered++
	}

	success := len(failures) == 0
	resp := &clusterv1.ForwardPublishBatchResponse{
		Success:   success,
		Delivered: delivered,
		Failures:  failures,
	}
	if !success {
		resp.Error = "one or more forward publish deliveries failed"
	}
	return connect.NewResponse(resp), nil
}

// SetQueueHandler sets the queue handler for queue distribution operations.
func (t *Transport) SetQueueHandler(handler QueueHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.queueHandler = handler
}

// SetForwardPublishHandler sets the handler for topic-based forward publish RPCs.
func (t *Transport) SetForwardPublishHandler(handler ForwardPublishHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.forwardHandler = handler
}

// SendPublish sends a PUBLISH message to a specific peer node with retry and circuit breaker.
func (t *Transport) SendPublish(ctx context.Context, nodeID, clientID, topic string, payload []byte, qos byte, retain, dup bool, properties map[string]string) error {
	return retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		req := connect.NewRequest(&clusterv1.PublishRequest{
			ClientId:   clientID,
			Topic:      topic,
			Payload:    payload,
			Qos:        uint32(qos),
			Retain:     retain,
			Dup:        dup,
			Properties: properties,
		})

		resp, err := client.RoutePublish(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		if !resp.Msg.Success {
			return fmt.Errorf("publish failed: %s", resp.Msg.Error)
		}

		return nil
	})
}

// SendTakeover sends a session takeover request to a peer node with retry and circuit breaker.
func (t *Transport) SendTakeover(ctx context.Context, nodeID, clientID, fromNode, toNode string) (*clusterv1.SessionState, error) {
	var state *clusterv1.SessionState
	err := retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		req := connect.NewRequest(&clusterv1.TakeoverRequest{
			ClientId: clientID,
			FromNode: fromNode,
			ToNode:   toNode,
		})

		resp, err := client.TakeoverSession(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		if !resp.Msg.Success {
			return fmt.Errorf("takeover failed: %s", resp.Msg.Error)
		}

		state = resp.Msg.SessionState
		return nil
	})
	return state, err
}

// SendFetchRetained fetches a retained message from a peer node with retry and circuit breaker.
func (t *Transport) SendFetchRetained(ctx context.Context, nodeID, topic string) (*clusterv1.RetainedMessage, error) {
	var msg *clusterv1.RetainedMessage
	err := retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		req := connect.NewRequest(&clusterv1.FetchRetainedRequest{
			Topic: topic,
		})

		resp, err := client.FetchRetained(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		if resp.Msg.Error != "" {
			return fmt.Errorf("fetch failed: %s", resp.Msg.Error)
		}

		if !resp.Msg.Found {
			msg = nil
			return nil
		}

		msg = resp.Msg.Message
		return nil
	})
	return msg, err
}

// SendFetchWill fetches a will message from a peer node with retry and circuit breaker.
func (t *Transport) SendFetchWill(ctx context.Context, nodeID, clientID string) (*clusterv1.WillMessage, error) {
	var will *clusterv1.WillMessage
	err := retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		req := connect.NewRequest(&clusterv1.FetchWillRequest{
			ClientId: clientID,
		})

		resp, err := client.FetchWill(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		if resp.Msg.Error != "" {
			return fmt.Errorf("fetch failed: %s", resp.Msg.Error)
		}

		if !resp.Msg.Found {
			will = nil
			return nil
		}

		will = resp.Msg.Message
		return nil
	})
	return will, err
}

// SendEnqueueRemote sends an enqueue request to a peer node with retry and circuit breaker.
func (t *Transport) SendEnqueueRemote(ctx context.Context, nodeID, queueName string, payload []byte, properties map[string]string, forwarded, forwardToLeader bool) error {
	return retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		req := connect.NewRequest(&clusterv1.EnqueueRemoteRequest{
			QueueName:        queueName,
			Payload:          payload,
			Properties:       properties,
			ForwardedPublish: forwarded,
			ForwardToLeader:  forwardToLeader,
		})

		resp, err := client.EnqueueRemote(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		if !resp.Msg.Success {
			return fmt.Errorf("enqueue failed: %s", resp.Msg.Error)
		}

		return nil
	})
}

// SendRouteQueueMessage sends a queue message delivery request to a peer node with retry and circuit breaker.
func (t *Transport) SendRouteQueueMessage(ctx context.Context, nodeID, clientID string, msg *message.Envelope) error {
	return retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		if msg == nil {
			return fmt.Errorf("queue message is nil")
		}

		req := connect.NewRequest(encodeRouteQueueMessage(clientID, msg))

		resp, err := client.RouteQueueMessage(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		if !resp.Msg.Success {
			if resp.Msg.ClientNotConnected {
				return fmt.Errorf("%w: route queue message failed: %s", corebroker.ErrClientNotConnected, resp.Msg.Error)
			}
			return fmt.Errorf("route queue message failed: %s", resp.Msg.Error)
		}

		return nil
	})
}

// SendPublishBatch sends multiple publish deliveries to a peer node in one RPC.
func (t *Transport) SendPublishBatch(ctx context.Context, nodeID string, messages []*clusterv1.PublishRequest) error {
	if len(messages) == 0 {
		return nil
	}

	remaining := messages
	for attempt := range maxPartialRetries {
		failed, err := t.sendPublishBatchOnce(ctx, nodeID, remaining)
		if err != nil {
			return err
		}
		if len(failed) == 0 {
			return nil
		}
		remaining = failed
		if attempt < maxPartialRetries-1 {
			t.logger.Warn("publish batch partial failure, retrying failed subset",
				slog.String("node_id", nodeID),
				slog.Int("failed", len(failed)),
				slog.Int("attempt", attempt+1))
		}
	}

	t.logger.Warn("publish batch partial failure after retries",
		slog.String("node_id", nodeID),
		slog.Int("remaining_failures", len(remaining)))
	if allPublishBatchQoS0(remaining) {
		return nil
	}

	return fmt.Errorf("publish batch failed after %d retries: %d messages still failing", maxPartialRetries, len(remaining))
}

func (t *Transport) sendPublishBatchOnce(
	ctx context.Context, nodeID string, messages []*clusterv1.PublishRequest,
) ([]*clusterv1.PublishRequest, error) {
	var failedMsgs []*clusterv1.PublishRequest

	err := retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		req := connect.NewRequest(&clusterv1.PublishBatchRequest{
			Messages: messages,
		})
		resp, err := client.RoutePublishBatch(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		failedMsgs = nil
		if resp.Msg.Success {
			if len(resp.Msg.Failures) > 0 {
				return fmt.Errorf("publish batch response marked success with %d failures", len(resp.Msg.Failures))
			}
			return nil
		}

		if len(resp.Msg.Failures) == 0 {
			if resp.Msg.Error == "" {
				return fmt.Errorf("publish batch failed: unknown error")
			}
			return fmt.Errorf("publish batch failed: %s", resp.Msg.Error)
		}

		failedIdx := make(map[uint32]struct{}, len(resp.Msg.Failures))
		for _, f := range resp.Msg.Failures {
			if int(f.Index) >= len(messages) {
				return fmt.Errorf("publish batch response has invalid failure index %d for batch size %d", f.Index, len(messages))
			}
			failedIdx[f.Index] = struct{}{}
		}
		for i, m := range messages {
			if _, ok := failedIdx[uint32(i)]; ok {
				failedMsgs = append(failedMsgs, m)
			}
		}
		if len(failedMsgs) == 0 {
			return fmt.Errorf("publish batch reported failures but none matched the request batch")
		}
		return nil
	})

	return failedMsgs, err
}

// SendRouteQueueBatch sends multiple queue deliveries to a peer node in one RPC.
func (t *Transport) SendRouteQueueBatch(ctx context.Context, nodeID string, deliveries []QueueDelivery) error {
	if len(deliveries) == 0 {
		return nil
	}

	remaining := deliveries
	var failures []queueBatchFailure
	for attempt := range maxPartialRetries {
		failed, err := t.sendRouteQueueBatchOnce(ctx, nodeID, remaining)
		if err != nil {
			return err
		}
		if len(failed) == 0 {
			return nil
		}
		failures = failed
		remaining = queueBatchFailureDeliveries(failed)
		if attempt < maxPartialRetries-1 {
			t.logger.Warn("route queue batch partial failure, retrying failed subset",
				slog.String("node_id", nodeID),
				slog.Int("failed", len(remaining)),
				slog.Int("attempt", attempt+1))
		}
	}

	t.logger.Warn("route queue batch partial failure after retries",
		slog.String("node_id", nodeID),
		slog.Int("remaining_failures", len(remaining)))
	batchErr := fmt.Errorf("route queue batch failed after %d retries: %d deliveries still failing: %s",
		maxPartialRetries, len(remaining), summarizeQueueBatchFailures(failures))
	// When every remaining failure is a not-connected target, wrap the sentinel
	// so the caller can evict via errors.Is without parsing the message. Batches
	// are built per single consumer, so "all" maps to one client.
	if noClientConnected(failures) {
		return fmt.Errorf("%w: %s", corebroker.ErrClientNotConnected, batchErr)
	}
	return batchErr
}

func noClientConnected(failures []queueBatchFailure) bool {
	if len(failures) == 0 {
		return false
	}
	for _, failure := range failures {
		if !failure.clientNotConnected {
			return false
		}
	}
	return true
}

type queueBatchFailure struct {
	delivery           QueueDelivery
	err                string
	clientNotConnected bool
}

func (t *Transport) sendRouteQueueBatchOnce(
	ctx context.Context, nodeID string, deliveries []QueueDelivery,
) ([]queueBatchFailure, error) {
	var failures []queueBatchFailure

	err := retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		wireMsgs := make([]*clusterv1.RouteQueueMessageRequest, 0, len(deliveries))
		wireToDelivery := make([]int, 0, len(deliveries))
		for i, delivery := range deliveries {
			if delivery.Message == nil {
				continue
			}
			wireMsgs = append(wireMsgs, encodeRouteQueueMessage(delivery.ClientID, delivery.Message))
			wireToDelivery = append(wireToDelivery, i)
		}
		if len(wireMsgs) == 0 {
			return nil
		}

		req := connect.NewRequest(&clusterv1.RouteQueueBatchRequest{
			Messages: wireMsgs,
		})
		resp, err := client.RouteQueueBatch(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		failures = nil
		if resp.Msg.Success {
			if len(resp.Msg.Failures) > 0 {
				return fmt.Errorf("route queue batch response marked success with %d failures", len(resp.Msg.Failures))
			}
			return nil
		}

		if len(resp.Msg.Failures) == 0 {
			if resp.Msg.Error == "" {
				return fmt.Errorf("route queue batch failed: unknown error")
			}
			return fmt.Errorf("route queue batch failed: %s", resp.Msg.Error)
		}

		seen := make(map[int]struct{}, len(resp.Msg.Failures))
		for _, f := range resp.Msg.Failures {
			if int(f.Index) >= len(wireToDelivery) {
				return fmt.Errorf("route queue batch response has invalid failure index %d for wire batch size %d", f.Index, len(wireToDelivery))
			}
			deliveryIdx := wireToDelivery[f.Index]
			if _, ok := seen[deliveryIdx]; ok {
				continue
			}
			seen[deliveryIdx] = struct{}{}
			failures = append(failures, queueBatchFailure{
				delivery:           deliveries[deliveryIdx],
				err:                f.Error,
				clientNotConnected: f.ClientNotConnected,
			})
		}
		if len(failures) == 0 {
			return fmt.Errorf("route queue batch reported failures but none matched the request batch")
		}
		return nil
	})

	return failures, err
}

func queueBatchFailureDeliveries(failures []queueBatchFailure) []QueueDelivery {
	deliveries := make([]QueueDelivery, 0, len(failures))
	for _, failure := range failures {
		deliveries = append(deliveries, failure.delivery)
	}
	return deliveries
}

func summarizeQueueBatchFailures(failures []queueBatchFailure) string {
	if len(failures) == 0 {
		return "unknown error"
	}

	parts := make([]string, 0, len(failures))
	for _, failure := range failures {
		reason := strings.TrimSpace(failure.err)
		if reason == "" {
			reason = "unknown error"
		}
		parts = append(parts, fmt.Sprintf("client %s queue %s: %s",
			failure.delivery.ClientID, failure.delivery.Message.Broker.Queue.Name, reason))
	}
	return strings.Join(parts, "; ")
}

// SendForwardGroupOp forwards a consumer group operation to a peer node with retry and circuit breaker.
func (t *Transport) SendForwardGroupOp(ctx context.Context, nodeID, queueName string, op *clusterv1.GroupOperation) error {
	return retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		req := connect.NewRequest(&clusterv1.ForwardGroupOpRequest{
			QueueName: queueName,
			Operation: op,
		})

		resp, err := client.ForwardGroupOp(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		if !resp.Msg.Success {
			return fmt.Errorf("forward group op failed: %s", resp.Msg.Error)
		}

		return nil
	})
}

// SendForwardPublishBatch sends a batch of topic-based forward publish messages to a peer node.
// Transport errors are retried by the circuit breaker. Partial delivery failures
// (some messages delivered, some not) are retried with only the failed subset to
// avoid re-delivering already-delivered messages.
func (t *Transport) SendForwardPublishBatch(ctx context.Context, nodeID string, messages []*clusterv1.ForwardPublishRequest) error {
	if len(messages) == 0 {
		return nil
	}

	remaining := messages
	for attempt := range maxPartialRetries {
		failed, err := t.sendForwardPublishBatchOnce(ctx, nodeID, remaining)
		if err != nil {
			return err
		}
		if len(failed) == 0 {
			return nil
		}
		remaining = failed
		if attempt < maxPartialRetries-1 {
			t.logger.Warn("forward publish batch partial failure, retrying failed subset",
				slog.String("node_id", nodeID),
				slog.Int("failed", len(failed)),
				slog.Int("attempt", attempt+1))

			// Exponential backoff gives the receiving node time to drain inflight messages.
			delay := retryBaseDelay << attempt
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	t.logger.Warn("forward publish batch partial failure after retries",
		slog.String("node_id", nodeID),
		slog.Int("remaining_failures", len(remaining)))
	if allForwardPublishBatchQoS0(remaining) {
		return nil
	}

	return fmt.Errorf("forward publish batch failed after %d retries: %d messages still failing", maxPartialRetries, len(remaining))
}

func (t *Transport) sendForwardPublishBatchOnce(
	ctx context.Context, nodeID string, messages []*clusterv1.ForwardPublishRequest,
) ([]*clusterv1.ForwardPublishRequest, error) {
	var failedMsgs []*clusterv1.ForwardPublishRequest

	err := retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		req := connect.NewRequest(&clusterv1.ForwardPublishBatchRequest{
			Messages: messages,
		})
		resp, err := client.ForwardPublishBatch(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		// RPC succeeded. Extract any per-message failures so the caller
		// can retry only the failed subset — never the whole batch.
		failedMsgs = nil
		if resp.Msg.Success {
			if len(resp.Msg.Failures) > 0 {
				return fmt.Errorf("forward publish batch response marked success with %d failures", len(resp.Msg.Failures))
			}
			return nil
		}

		if len(resp.Msg.Failures) == 0 {
			if resp.Msg.Error == "" {
				return fmt.Errorf("forward publish batch failed: unknown error")
			}
			return fmt.Errorf("forward publish batch failed: %s", resp.Msg.Error)
		}

		failedIdx := make(map[uint32]struct{}, len(resp.Msg.Failures))
		for _, f := range resp.Msg.Failures {
			if int(f.Index) >= len(messages) {
				return fmt.Errorf("forward publish batch response has invalid failure index %d for batch size %d", f.Index, len(messages))
			}
			failedIdx[f.Index] = struct{}{}
		}
		for i, m := range messages {
			if _, ok := failedIdx[uint32(i)]; ok {
				failedMsgs = append(failedMsgs, m)
			}
		}
		if len(failedMsgs) == 0 {
			return fmt.Errorf("forward publish batch reported failures but none matched the request batch")
		}
		return nil
	})

	return failedMsgs, err
}

func allPublishBatchQoS0(messages []*clusterv1.PublishRequest) bool {
	if len(messages) == 0 {
		return false
	}

	for _, msg := range messages {
		if msg == nil || msg.Qos != 0 {
			return false
		}
	}
	return true
}

func allForwardPublishBatchQoS0(messages []*clusterv1.ForwardPublishRequest) bool {
	if len(messages) == 0 {
		return false
	}

	for _, msg := range messages {
		if msg == nil || msg.Qos != 0 {
			return false
		}
	}
	return true
}

func encodeRouteQueueMessage(clientID string, msg *message.Envelope) *clusterv1.RouteQueueMessageRequest {
	return &clusterv1.RouteQueueMessageRequest{
		ClientId:   clientID,
		QueueName:  msg.Broker.Queue.Name,
		MessageId:  msg.Broker.Queue.MessageID,
		Topic:      msg.Topic,
		Payload:    msg.PayloadBytes(),
		Properties: message.ProjectProperties(msg, message.TrustedServiceProjection),
		Sequence:   int64(msg.Broker.Queue.Offset),
	}
}

func decodeRouteQueueMessage(wire *clusterv1.RouteQueueMessageRequest) (*message.Envelope, error) {
	if wire == nil {
		return nil, errors.New(errMessageIsNil)
	}
	if wire.Topic == "" {
		return nil, errors.New("queue delivery topic is required")
	}
	envelope := message.New(wire.Topic, wire.Payload)
	message.ApplyTrustedProperties(envelope, wire.Properties)
	if wire.MessageId != "" {
		envelope.Broker.Queue.MessageID = wire.MessageId
	}
	if wire.QueueName != "" {
		envelope.Broker.Queue.Name = wire.QueueName
	}
	if wire.Sequence >= 0 {
		envelope.Broker.Queue.Offset = uint64(wire.Sequence)
	}
	envelope.Broker.Delivery.QoS = 1
	return envelope, nil
}

func splitPropertyList(raw string) []string {
	if raw == "" {
		return nil
	}
	seen := make(map[string]struct{})
	values := make([]string, 0, 4)
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		values = append(values, item)
	}
	return values
}
