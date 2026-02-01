// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"connectrpc.com/connect"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/pkg/proto/cluster/v1/clusterv1connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// QueueHandler defines callbacks for queue distribution operations.
type QueueHandler interface {
	// EnqueueLocal enqueues a message on this node (called by remote RPC).
	EnqueueLocal(ctx context.Context, queueName string, payload []byte, properties map[string]string) (string, error)

	// DeliverQueueMessage delivers a queue message to a local consumer.
	DeliverQueueMessage(ctx context.Context, clientID string, msg any) error

	// PublishLocal publishes a message to local matching queues (called by remote forward).
	PublishLocal(ctx context.Context, topic string, payload []byte, properties map[string]string) error
}

// Transport handles inter-broker communication using Connect protocol.
type Transport struct {
	mu           sync.RWMutex
	nodeID       string
	bindAddr     string
	httpServer   *http.Server
	listener     net.Listener
	peerClients  map[string]clusterv1connect.BrokerServiceClient
	breakers     *peerBreakers
	logger       *slog.Logger
	handler      MessageHandler
	queueHandler QueueHandler
	stopCh       chan struct{}
	tlsConfig    *TransportTLSConfig
	httpClient   *http.Client
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
		// Load server certificate and key
		cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load server certificate: %w", err)
		}

		// Load CA certificate for client verification
		caCert, err := os.ReadFile(tlsCfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load CA certificate: %w", err)
		}

		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		// Server TLS config (for accepting connections)
		serverTLSConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientCAs:    caPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS12,
			NextProtos:   []string{"h2"},
		}

		// Client TLS config (for connecting to peers)
		clientTLSConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caPool,
			MinVersion:   tls.VersionTLS12,
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
	}

	return t, nil
}

// Start starts the HTTP server.
func (t *Transport) Start() error {
	go func() {
		t.logger.Info("starting Connect transport server", slog.String("address", t.bindAddr))
		if err := t.httpServer.Serve(t.listener); err != nil && err != http.ErrServerClosed {
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
func (t *Transport) RoutePublish(ctx context.Context, req *connect.Request[clusterv1.PublishRequest]) (*connect.Response[clusterv1.PublishResponse], error) {
	if t.handler == nil {
		return connect.NewResponse(&clusterv1.PublishResponse{
			Success: false,
			Error:   "no handler configured",
		}), nil
	}

	msg := &Message{
		Topic:      req.Msg.Topic,
		Payload:    req.Msg.Payload,
		QoS:        byte(req.Msg.Qos),
		Retain:     req.Msg.Retain,
		Dup:        req.Msg.Dup,
		Properties: req.Msg.Properties,
	}

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

// TakeoverSession implements BrokerServiceHandler.TakeoverSession.
func (t *Transport) TakeoverSession(ctx context.Context, req *connect.Request[clusterv1.TakeoverRequest]) (*connect.Response[clusterv1.TakeoverResponse], error) {
	if t.handler == nil {
		return connect.NewResponse(&clusterv1.TakeoverResponse{
			Success: false,
			Error:   "no handler configured",
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
func (t *Transport) FetchRetained(ctx context.Context, req *connect.Request[clusterv1.FetchRetainedRequest]) (*connect.Response[clusterv1.FetchRetainedResponse], error) {
	if t.handler == nil {
		return connect.NewResponse(&clusterv1.FetchRetainedResponse{
			Found: false,
			Error: "no handler configured",
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

	grpcMsg := &clusterv1.RetainedMessage{
		Topic:      msg.Topic,
		Payload:    msg.Payload,
		Qos:        uint32(msg.QoS),
		Retain:     msg.Retain,
		Properties: msg.Properties,
		Timestamp:  msg.PublishTime.Unix(),
	}

	return connect.NewResponse(&clusterv1.FetchRetainedResponse{
		Found:   true,
		Message: grpcMsg,
	}), nil
}

// FetchWill implements BrokerServiceHandler.FetchWill.
func (t *Transport) FetchWill(ctx context.Context, req *connect.Request[clusterv1.FetchWillRequest]) (*connect.Response[clusterv1.FetchWillResponse], error) {
	if t.handler == nil {
		return connect.NewResponse(&clusterv1.FetchWillResponse{
			Found: false,
			Error: "no handler configured",
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
func (t *Transport) EnqueueRemote(ctx context.Context, req *connect.Request[clusterv1.EnqueueRemoteRequest]) (*connect.Response[clusterv1.EnqueueRemoteResponse], error) {
	t.mu.RLock()
	handler := t.queueHandler
	t.mu.RUnlock()

	if handler == nil {
		return connect.NewResponse(&clusterv1.EnqueueRemoteResponse{
			Success: false,
			Error:   "no queue handler configured",
		}), nil
	}

	// Check if this is a forwarded publish (topic-based) vs direct enqueue (queue-based)
	if req.Msg.Properties != nil && req.Msg.Properties["_forward_publish"] == "true" {
		// This is a forwarded publish - call PublishLocal with the topic
		topic := req.Msg.QueueName // topic is passed in queueName field for forwards
		props := make(map[string]string)
		for k, v := range req.Msg.Properties {
			if k != "_forward_publish" {
				props[k] = v
			}
		}
		err := handler.PublishLocal(ctx, topic, req.Msg.Payload, props)
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
	messageID, err := handler.EnqueueLocal(ctx, req.Msg.QueueName, req.Msg.Payload, req.Msg.Properties)
	if err != nil {
		return connect.NewResponse(&clusterv1.EnqueueRemoteResponse{
			Success: false,
			Error:   err.Error(),
		}), nil
	}

	return connect.NewResponse(&clusterv1.EnqueueRemoteResponse{
		Success:   true,
		MessageId: messageID,
	}), nil
}

// RouteQueueMessage implements BrokerServiceHandler.RouteQueueMessage.
func (t *Transport) RouteQueueMessage(ctx context.Context, req *connect.Request[clusterv1.RouteQueueMessageRequest]) (*connect.Response[clusterv1.RouteQueueMessageResponse], error) {
	t.mu.RLock()
	handler := t.queueHandler
	t.mu.RUnlock()

	if handler == nil {
		return connect.NewResponse(&clusterv1.RouteQueueMessageResponse{
			Success: false,
			Error:   "no queue handler configured",
		}), nil
	}

	msg := map[string]interface{}{
		"id":         req.Msg.MessageId,
		"queueName":  req.Msg.QueueName,
		"payload":    req.Msg.Payload,
		"properties": req.Msg.Properties,
		"sequence":   req.Msg.Sequence,
	}

	err := handler.DeliverQueueMessage(ctx, req.Msg.ClientId, msg)
	if err != nil {
		return connect.NewResponse(&clusterv1.RouteQueueMessageResponse{
			Success: false,
			Error:   err.Error(),
		}), nil
	}

	return connect.NewResponse(&clusterv1.RouteQueueMessageResponse{
		Success: true,
	}), nil
}

// AppendEntries implements BrokerServiceHandler.AppendEntries (Raft).
func (t *Transport) AppendEntries(ctx context.Context, req *connect.Request[clusterv1.AppendEntriesRequest]) (*connect.Response[clusterv1.AppendEntriesResponse], error) {
	// TODO: Implement Raft consensus
	return connect.NewResponse(&clusterv1.AppendEntriesResponse{
		Term:    req.Msg.Term,
		Success: false,
	}), nil
}

// RequestVote implements BrokerServiceHandler.RequestVote (Raft).
func (t *Transport) RequestVote(ctx context.Context, req *connect.Request[clusterv1.RequestVoteRequest]) (*connect.Response[clusterv1.RequestVoteResponse], error) {
	// TODO: Implement Raft consensus
	return connect.NewResponse(&clusterv1.RequestVoteResponse{
		Term:        req.Msg.Term,
		VoteGranted: false,
	}), nil
}

// InstallSnapshot implements BrokerServiceHandler.InstallSnapshot (Raft).
func (t *Transport) InstallSnapshot(ctx context.Context, req *connect.Request[clusterv1.InstallSnapshotRequest]) (*connect.Response[clusterv1.InstallSnapshotResponse], error) {
	// TODO: Implement Raft consensus
	return connect.NewResponse(&clusterv1.InstallSnapshotResponse{
		Term: req.Msg.Term,
	}), nil
}

// SetQueueHandler sets the queue handler for queue distribution operations.
func (t *Transport) SetQueueHandler(handler QueueHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.queueHandler = handler
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
func (t *Transport) SendEnqueueRemote(ctx context.Context, nodeID, queueName string, payload []byte, properties map[string]string) (string, error) {
	var messageID string
	err := retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		req := connect.NewRequest(&clusterv1.EnqueueRemoteRequest{
			QueueName:  queueName,
			Payload:    payload,
			Properties: properties,
		})

		resp, err := client.EnqueueRemote(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		if !resp.Msg.Success {
			return fmt.Errorf("enqueue failed: %s", resp.Msg.Error)
		}

		messageID = resp.Msg.MessageId
		return nil
	})
	return messageID, err
}

// SendRouteQueueMessage sends a queue message delivery request to a peer node with retry and circuit breaker.
func (t *Transport) SendRouteQueueMessage(ctx context.Context, nodeID, clientID, queueName, messageID string, payload []byte, properties map[string]string, sequence int64) error {
	return retryWithBreaker(ctx, t.breakers, nodeID, func() error {
		client, err := t.GetPeerClient(nodeID)
		if err != nil {
			return err
		}

		req := connect.NewRequest(&clusterv1.RouteQueueMessageRequest{
			ClientId:   clientID,
			QueueName:  queueName,
			MessageId:  messageID,
			Payload:    payload,
			Properties: properties,
			Sequence:   sequence,
		})

		resp, err := client.RouteQueueMessage(ctx, req)
		if err != nil {
			return fmt.Errorf("connect call failed: %w", err)
		}

		if !resp.Msg.Success {
			return fmt.Errorf("route queue message failed: %s", resp.Msg.Error)
		}

		return nil
	})
}
