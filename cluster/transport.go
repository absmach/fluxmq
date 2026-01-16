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
	"os"
	"sync"

	"github.com/absmach/fluxmq/cluster/grpc"
	"github.com/absmach/fluxmq/core"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// QueueHandler defines callbacks for queue distribution operations.
type QueueHandler interface {
	// EnqueueLocal enqueues a message on this node (called by remote RPC).
	EnqueueLocal(ctx context.Context, queueName string, payload []byte, properties map[string]string) (string, error)

	// DeliverQueueMessage delivers a queue message to a local consumer.
	DeliverQueueMessage(ctx context.Context, clientID string, msg any) error
}

// Transport handles inter-broker gRPC communication.
type Transport struct {
	grpc.UnimplementedBrokerServiceServer
	mu           sync.RWMutex
	nodeID       string
	bindAddr     string
	grpcServer   *gogrpc.Server
	listener     net.Listener
	peerClients  map[string]grpc.BrokerServiceClient
	logger       *slog.Logger
	handler      MessageHandler
	queueHandler QueueHandler
	stopCh       chan struct{}
	tlsConfig    *TransportTLSConfig
	clientCreds  credentials.TransportCredentials
}

// NewTransport creates a new gRPC transport.
// If tlsCfg is nil, the transport uses insecure connections (development mode only).
func NewTransport(nodeID, bindAddr string, handler MessageHandler, tlsCfg *TransportTLSConfig, logger *slog.Logger) (*Transport, error) {
	var listener net.Listener
	var grpcServer *gogrpc.Server
	var clientCreds credentials.TransportCredentials
	var err error

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

		// Create gRPC server with TLS credentials
		serverCreds := credentials.NewTLS(serverTLSConfig)
		grpcServer = gogrpc.NewServer(gogrpc.Creds(serverCreds))

		// Store client credentials for peer connections
		clientCreds = credentials.NewTLS(clientTLSConfig)

		logger.Info("transport TLS enabled", slog.String("address", bindAddr))
	} else {
		// Insecure mode (development only)
		listener, err = net.Listen("tcp", bindAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to listen on %s: %w", bindAddr, err)
		}

		grpcServer = gogrpc.NewServer()
		clientCreds = insecure.NewCredentials()

		logger.Warn("transport TLS disabled - using insecure connections (development mode only)")
	}

	t := &Transport{
		nodeID:      nodeID,
		bindAddr:    bindAddr,
		grpcServer:  grpcServer,
		listener:    listener,
		peerClients: make(map[string]grpc.BrokerServiceClient),
		logger:      logger,
		handler:     handler,
		stopCh:      make(chan struct{}),
		tlsConfig:   tlsCfg,
		clientCreds: clientCreds,
	}

	// Register gRPC service
	grpc.RegisterBrokerServiceServer(grpcServer, t)

	return t, nil
}

// Start starts the gRPC server.
func (t *Transport) Start() error {
	go func() {
		t.logger.Info("starting gRPC transport server", slog.String("address", t.bindAddr))
		if err := t.grpcServer.Serve(t.listener); err != nil {
			t.logger.Error("gRPC server error", slog.String("error", err.Error()))
		}
	}()
	return nil
}

// Stop gracefully stops the gRPC server.
func (t *Transport) Stop() error {
	close(t.stopCh)

	// Close peer connections
	t.mu.Lock()
	for _, client := range t.peerClients {
		if conn, ok := client.(interface{ Close() error }); ok {
			conn.Close()
		}
	}
	t.peerClients = nil
	t.mu.Unlock()

	// Stop gRPC server
	if t.grpcServer != nil {
		t.grpcServer.GracefulStop()
	}

	return nil
}

// ConnectPeer establishes a gRPC connection to a peer node.
func (t *Transport) ConnectPeer(nodeID, addr string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if already connected
	if _, exists := t.peerClients[nodeID]; exists {
		return nil
	}

	// Create gRPC connection using stored credentials (TLS or insecure)
	conn, err := gogrpc.NewClient(addr, gogrpc.WithTransportCredentials(t.clientCreds))
	if err != nil {
		return fmt.Errorf("failed to connect to peer %s at %s: %w", nodeID, addr, err)
	}

	client := grpc.NewBrokerServiceClient(conn)
	t.peerClients[nodeID] = client

	t.logger.Info("connected to peer",
		slog.String("node_id", nodeID),
		slog.String("address", addr),
		slog.Bool("tls_enabled", t.tlsConfig != nil))
	return nil
}

// GetPeerClient returns the gRPC client for a peer node.
func (t *Transport) GetPeerClient(nodeID string) (grpc.BrokerServiceClient, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	client, exists := t.peerClients[nodeID]
	if !exists {
		return nil, fmt.Errorf("no connection to peer %s", nodeID)
	}

	return client, nil
}

// HasPeerConnection checks if we have an active gRPC connection to a peer.
func (t *Transport) HasPeerConnection(nodeID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	_, exists := t.peerClients[nodeID]
	return exists
}

// RoutePublish implements BrokerServiceServer.RoutePublish.
// This is called by peer brokers to deliver a message to a local client.
func (t *Transport) RoutePublish(ctx context.Context, req *grpc.PublishRequest) (*grpc.PublishResponse, error) {
	if t.handler == nil {
		return &grpc.PublishResponse{
			Success: false,
			Error:   "no handler configured",
		}, nil
	}

	msg := &core.Message{
		Topic:      req.Topic,
		Payload:    req.Payload,
		QoS:        byte(req.Qos),
		Retain:     req.Retain,
		Dup:        req.Dup,
		Properties: req.Properties,
	}

	err := t.handler.DeliverToClient(ctx, req.ClientId, msg)
	if err != nil {
		return &grpc.PublishResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &grpc.PublishResponse{
		Success: true,
	}, nil
}

// TakeoverSession implements BrokerServiceServer.TakeoverSession.
// This is called by peer brokers to take over a session.
func (t *Transport) TakeoverSession(ctx context.Context, req *grpc.TakeoverRequest) (*grpc.TakeoverResponse, error) {
	if t.handler == nil {
		return &grpc.TakeoverResponse{
			Success: false,
			Error:   "no handler configured",
		}, nil
	}

	// Get session state from the handler (which will disconnect the client)
	sessionState, err := t.handler.GetSessionStateAndClose(ctx, req.ClientId)
	if err != nil {
		return &grpc.TakeoverResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &grpc.TakeoverResponse{
		Success:      true,
		SessionState: sessionState,
	}, nil
}

// SendPublish sends a PUBLISH message to a specific peer node.
func (t *Transport) SendPublish(ctx context.Context, nodeID, clientID, topic string, payload []byte, qos byte, retain, dup bool, properties map[string]string) error {
	client, err := t.GetPeerClient(nodeID)
	if err != nil {
		return err
	}

	req := &grpc.PublishRequest{
		ClientId:   clientID,
		Topic:      topic,
		Payload:    payload,
		Qos:        uint32(qos),
		Retain:     retain,
		Dup:        dup,
		Properties: properties,
	}

	resp, err := client.RoutePublish(ctx, req)
	if err != nil {
		return fmt.Errorf("grpc call failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("publish failed: %s", resp.Error)
	}

	return nil
}

// SendTakeover sends a session takeover request to a peer node and returns the session state.
func (t *Transport) SendTakeover(ctx context.Context, nodeID, clientID, fromNode, toNode string) (*grpc.SessionState, error) {
	client, err := t.GetPeerClient(nodeID)
	if err != nil {
		return nil, err
	}

	req := &grpc.TakeoverRequest{
		ClientId: clientID,
		FromNode: fromNode,
		ToNode:   toNode,
	}

	resp, err := client.TakeoverSession(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpc call failed: %w", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("takeover failed: %s", resp.Error)
	}

	return resp.SessionState, nil
}

// FetchRetained implements BrokerServiceServer.FetchRetained.
// This is called by peer brokers to fetch a retained message payload from the local store.
func (t *Transport) FetchRetained(ctx context.Context, req *grpc.FetchRetainedRequest) (*grpc.FetchRetainedResponse, error) {
	if t.handler == nil {
		return &grpc.FetchRetainedResponse{
			Found: false,
			Error: "no handler configured",
		}, nil
	}

	// Get retained message from local storage via handler
	msg, err := t.handler.GetRetainedMessage(ctx, req.Topic)
	if err != nil {
		return &grpc.FetchRetainedResponse{
			Found: false,
			Error: err.Error(),
		}, nil
	}

	// Message not found
	if msg == nil {
		return &grpc.FetchRetainedResponse{
			Found: false,
		}, nil
	}

	// Convert storage.Message to grpc.RetainedMessage
	grpcMsg := &grpc.RetainedMessage{
		Topic:      msg.Topic,
		Payload:    msg.Payload,
		Qos:        uint32(msg.QoS),
		Retain:     msg.Retain,
		Properties: msg.Properties,
		Timestamp:  msg.PublishTime.Unix(),
	}

	return &grpc.FetchRetainedResponse{
		Found:   true,
		Message: grpcMsg,
	}, nil
}

// SendFetchRetained fetches a retained message from a peer node.
func (t *Transport) SendFetchRetained(ctx context.Context, nodeID, topic string) (*grpc.RetainedMessage, error) {
	client, err := t.GetPeerClient(nodeID)
	if err != nil {
		return nil, err
	}

	req := &grpc.FetchRetainedRequest{
		Topic: topic,
	}

	resp, err := client.FetchRetained(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpc call failed: %w", err)
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("fetch failed: %s", resp.Error)
	}

	if !resp.Found {
		return nil, nil // Message not found (not an error)
	}

	return resp.Message, nil
}

// FetchWill handles incoming requests to fetch a will message from this node.
func (t *Transport) FetchWill(ctx context.Context, req *grpc.FetchWillRequest) (*grpc.FetchWillResponse, error) {
	if t.handler == nil {
		return &grpc.FetchWillResponse{
			Found: false,
			Error: "no handler configured",
		}, nil
	}

	// Get will message from local storage via handler
	will, err := t.handler.GetWillMessage(ctx, req.ClientId)
	if err != nil {
		return &grpc.FetchWillResponse{
			Found: false,
			Error: err.Error(),
		}, nil
	}

	// Will message not found
	if will == nil {
		return &grpc.FetchWillResponse{
			Found: false,
		}, nil
	}

	// Convert storage.WillMessage to grpc.WillMessage
	grpcWill := &grpc.WillMessage{
		Topic:   will.Topic,
		Payload: will.Payload,
		Qos:     uint32(will.QoS),
		Retain:  will.Retain,
		Delay:   will.Delay,
	}

	return &grpc.FetchWillResponse{
		Found:   true,
		Message: grpcWill,
	}, nil
}

// SendFetchWill fetches a will message from a peer node.
func (t *Transport) SendFetchWill(ctx context.Context, nodeID, clientID string) (*grpc.WillMessage, error) {
	client, err := t.GetPeerClient(nodeID)
	if err != nil {
		return nil, err
	}

	req := &grpc.FetchWillRequest{
		ClientId: clientID,
	}

	resp, err := client.FetchWill(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("grpc call failed: %w", err)
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("fetch failed: %s", resp.Error)
	}

	if !resp.Found {
		return nil, nil // Will message not found (not an error)
	}

	return resp.Message, nil
}

// SetQueueHandler sets the queue handler for queue distribution operations.
func (t *Transport) SetQueueHandler(handler QueueHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.queueHandler = handler
}

// EnqueueRemote implements BrokerServiceServer.EnqueueRemote.
// This is called by peer brokers to enqueue a message on this node.
func (t *Transport) EnqueueRemote(ctx context.Context, req *grpc.EnqueueRemoteRequest) (*grpc.EnqueueRemoteResponse, error) {
	t.mu.RLock()
	handler := t.queueHandler
	t.mu.RUnlock()

	if handler == nil {
		return &grpc.EnqueueRemoteResponse{
			Success: false,
			Error:   "no queue handler configured",
		}, nil
	}

	messageID, err := handler.EnqueueLocal(ctx, req.QueueName, req.Payload, req.Properties)
	if err != nil {
		return &grpc.EnqueueRemoteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &grpc.EnqueueRemoteResponse{
		Success:   true,
		MessageId: messageID,
	}, nil
}

// SendEnqueueRemote sends an enqueue request to a peer node.
func (t *Transport) SendEnqueueRemote(ctx context.Context, nodeID, queueName string, payload []byte, properties map[string]string) (string, error) {
	client, err := t.GetPeerClient(nodeID)
	if err != nil {
		return "", err
	}

	req := &grpc.EnqueueRemoteRequest{
		QueueName:  queueName,
		Payload:    payload,
		Properties: properties,
	}

	resp, err := client.EnqueueRemote(ctx, req)
	if err != nil {
		return "", fmt.Errorf("grpc call failed: %w", err)
	}

	if !resp.Success {
		return "", fmt.Errorf("enqueue failed: %s", resp.Error)
	}

	return resp.MessageId, nil
}

// RouteQueueMessage implements BrokerServiceServer.RouteQueueMessage.
// This is called by peer brokers to deliver a queue message to a local consumer.
func (t *Transport) RouteQueueMessage(ctx context.Context, req *grpc.RouteQueueMessageRequest) (*grpc.RouteQueueMessageResponse, error) {
	t.mu.RLock()
	handler := t.queueHandler
	t.mu.RUnlock()

	if handler == nil {
		return &grpc.RouteQueueMessageResponse{
			Success: false,
			Error:   "no queue handler configured",
		}, nil
	}

	// Create a simplified message structure for delivery
	msg := map[string]interface{}{
		"id":          req.MessageId,
		"queueName":   req.QueueName,
		"payload":     req.Payload,
		"properties":  req.Properties,
		"sequence":    req.Sequence,
		"partitionId": req.PartitionId,
	}

	err := handler.DeliverQueueMessage(ctx, req.ClientId, msg)
	if err != nil {
		return &grpc.RouteQueueMessageResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &grpc.RouteQueueMessageResponse{
		Success: true,
	}, nil
}

// SendRouteQueueMessage sends a queue message delivery request to a peer node.
func (t *Transport) SendRouteQueueMessage(ctx context.Context, nodeID, clientID, queueName, messageID string, payload []byte, properties map[string]string, sequence int64, partitionID int) error {
	client, err := t.GetPeerClient(nodeID)
	if err != nil {
		return err
	}

	req := &grpc.RouteQueueMessageRequest{
		ClientId:    clientID,
		QueueName:   queueName,
		MessageId:   messageID,
		Payload:     payload,
		Properties:  properties,
		Sequence:    sequence,
		PartitionId: int32(partitionID),
	}

	resp, err := client.RouteQueueMessage(ctx, req)
	if err != nil {
		return fmt.Errorf("grpc call failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("route queue message failed: %s", resp.Error)
	}

	return nil
}
