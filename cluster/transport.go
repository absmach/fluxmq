// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/absmach/mqtt/cluster/grpc"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Transport handles inter-broker gRPC communication.
type Transport struct {
	grpc.UnimplementedBrokerServiceServer
	mu          sync.RWMutex
	nodeID      string
	bindAddr    string
	grpcServer  *gogrpc.Server
	listener    net.Listener
	peerClients map[string]grpc.BrokerServiceClient
	handler     TransportHandler
	stopCh      chan struct{}
}

// TransportHandler handles incoming messages from other brokers.
type TransportHandler interface {
	// HandlePublish handles a PUBLISH message routed from another broker.
	HandlePublish(ctx context.Context, clientID, topic string, payload []byte, qos byte, retain bool, dup bool, properties map[string]string) error

	// HandleTakeover handles a session takeover request from another broker.
	// Returns the session state from the old node.
	HandleTakeover(ctx context.Context, clientID, fromNode, toNode string, state *grpc.SessionState) (*grpc.SessionState, error)
}

// NewTransport creates a new gRPC transport.
func NewTransport(nodeID, bindAddr string, handler TransportHandler) (*Transport, error) {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", bindAddr, err)
	}

	grpcServer := gogrpc.NewServer()

	t := &Transport{
		nodeID:      nodeID,
		bindAddr:    bindAddr,
		grpcServer:  grpcServer,
		listener:    listener,
		peerClients: make(map[string]grpc.BrokerServiceClient),
		handler:     handler,
		stopCh:      make(chan struct{}),
	}

	// Register gRPC service
	grpc.RegisterBrokerServiceServer(grpcServer, t)

	return t, nil
}

// Start starts the gRPC server.
func (t *Transport) Start() error {
	go func() {
		log.Printf("Starting gRPC transport server on %s", t.bindAddr)
		if err := t.grpcServer.Serve(t.listener); err != nil {
			log.Printf("gRPC server error: %v", err)
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

	// Create gRPC connection
	conn, err := gogrpc.NewClient(addr, gogrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to peer %s at %s: %w", nodeID, addr, err)
	}

	client := grpc.NewBrokerServiceClient(conn)
	t.peerClients[nodeID] = client

	log.Printf("Connected to peer %s at %s", nodeID, addr)
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

// RoutePublish implements BrokerServiceServer.RoutePublish.
// This is called by peer brokers to deliver a message to a local client.
func (t *Transport) RoutePublish(ctx context.Context, req *grpc.PublishRequest) (*grpc.PublishResponse, error) {
	if t.handler == nil {
		return &grpc.PublishResponse{
			Success: false,
			Error:   "no handler configured",
		}, nil
	}

	err := t.handler.HandlePublish(
		ctx,
		req.ClientId,
		req.Topic,
		req.Payload,
		byte(req.Qos),
		req.Retain,
		req.Dup,
		req.Properties,
	)
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
	sessionState, err := t.handler.HandleTakeover(
		ctx,
		req.ClientId,
		req.FromNode,
		req.ToNode,
		nil,
	)
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
