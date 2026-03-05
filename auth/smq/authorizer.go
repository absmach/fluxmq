// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package smq

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	grpcChannelsV1 "github.com/absmach/supermq/api/grpc/channels/v1"
	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultTimeout    = 2 * time.Second
	defaultClientType = "client"
	publishConnType   = uint32(1)
	subscribeConnType = uint32(2)

	smqMessagesPrefix    = "m"
	smqChannelsSeparator = "c"

	cbMaxRequests     = 3
	cbOpenInterval    = 10 * time.Second
	cbFailureThreshold = 5
)

type channelsClient interface {
	Authorize(ctx context.Context, in *grpcChannelsV1.AuthzReq, opts ...grpc.CallOption) (*grpcChannelsV1.AuthzRes, error)
}

// Config configures the SuperMQ-backed authorizer.
type Config struct {
	Address    string
	Timeout    time.Duration
	ClientType string
	Insecure   bool
}

// Authorizer uses SuperMQ Channels gRPC Authorize RPC for topic-level decisions.
type Authorizer struct {
	cfg    Config
	client channelsClient
	conn   *grpc.ClientConn
	cb     *gobreaker.CircuitBreaker
	logger *slog.Logger
}

var _ corebroker.Authorizer = (*Authorizer)(nil)

// NewAuthorizer creates a SuperMQ-backed topic authorizer.
func NewAuthorizer(cfg Config, logger *slog.Logger) (*Authorizer, error) {
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, fmt.Errorf("supermq authorizer address is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	if strings.TrimSpace(cfg.ClientType) == "" {
		cfg.ClientType = defaultClientType
	}
	if logger == nil {
		logger = slog.Default()
	}

	creds := credentials.TransportCredentials(insecure.NewCredentials())
	if !cfg.Insecure {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}

	conn, err := grpc.NewClient(
		cfg.Address,
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create SuperMQ channels grpc client for %s: %w", cfg.Address, err)
	}

	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "smq-authorizer",
		MaxRequests: cbMaxRequests,
		Interval:    0,
		Timeout:     cbOpenInterval,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= cbFailureThreshold
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			logger.Warn("smq authorizer circuit breaker state changed",
				slog.String("from", from.String()),
				slog.String("to", to.String()))
		},
	})

	return &Authorizer{
		cfg:    cfg,
		client: grpcChannelsV1.NewChannelsServiceClient(conn),
		conn:   conn,
		cb:     cb,
		logger: logger,
	}, nil
}

// Close closes the underlying gRPC connection.
func (a *Authorizer) Close() error {
	if a.conn == nil {
		return nil
	}
	return a.conn.Close()
}

// CanPublish checks publish authorization via SuperMQ channels service.
func (a *Authorizer) CanPublish(clientID string, topic string) bool {
	domainID, channelID, handled, err := parseSuperMQTopic(topic)
	if err != nil {
		a.logger.Warn("smq_authorize_publish_invalid_topic",
			slog.String("client_id", clientID),
			slog.String("topic", topic),
			slog.String("error", err.Error()))
		return false
	}
	if !handled {
		return true
	}

	return a.authorize(clientID, domainID, channelID, publishConnType)
}

// CanSubscribe checks subscribe authorization via SuperMQ channels service.
func (a *Authorizer) CanSubscribe(clientID string, filter string) bool {
	domainID, channelID, handled, err := parseSuperMQTopic(filter)
	if err != nil {
		a.logger.Warn("smq_authorize_subscribe_invalid_filter",
			slog.String("client_id", clientID),
			slog.String("filter", filter),
			slog.String("error", err.Error()))
		return false
	}
	if !handled {
		return true
	}

	return a.authorize(clientID, domainID, channelID, subscribeConnType)
}

func (a *Authorizer) authorize(clientID, domainID, channelID string, connType uint32) bool {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.Timeout)
	defer cancel()

	req := &grpcChannelsV1.AuthzReq{
		DomainId:   domainID,
		ClientId:   normalizeClientID(clientID),
		ClientType: a.cfg.ClientType,
		ChannelId:  channelID,
		Type:       connType,
	}

	result, err := a.cb.Execute(func() (any, error) {
		return a.client.Authorize(ctx, req)
	})
	if err != nil {
		a.logger.Warn("smq_authorize_rpc_failed",
			slog.String("client_id", clientID),
			slog.String("domain_id", domainID),
			slog.String("channel_id", channelID),
			slog.String("error", err.Error()))
		return false
	}

	res := result.(*grpcChannelsV1.AuthzRes)
	if !res.GetAuthorized() {
		a.logger.Debug("smq_authorize_denied",
			slog.String("client_id", clientID),
			slog.String("domain_id", domainID),
			slog.String("channel_id", channelID),
			slog.Uint64("type", uint64(connType)))
		return false
	}

	return true
}

func parseSuperMQTopic(topic string) (domainID, channelID string, handled bool, err error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(topic), "/")
	if trimmed == "" {
		return "", "", false, nil
	}
	parts := strings.Split(trimmed, "/")
	if parts[0] != smqMessagesPrefix {
		return "", "", false, nil
	}
	if len(parts) < 4 {
		return "", "", true, fmt.Errorf("malformed supermq topic: expected m/<domain>/c/<channel>")
	}
	if parts[2] != smqChannelsSeparator {
		return "", "", true, fmt.Errorf("malformed supermq topic: expected m/<domain>/c/<channel>")
	}
	if parts[1] == "" || parts[3] == "" {
		return "", "", true, fmt.Errorf("malformed supermq topic: empty domain or channel")
	}
	if strings.ContainsAny(parts[1], "+#") || strings.ContainsAny(parts[3], "+#") {
		return "", "", true, fmt.Errorf("malformed supermq topic: wildcards not allowed in domain/channel")
	}

	return parts[1], parts[3], true, nil
}

func normalizeClientID(clientID string) string {
	prefixes := []string{
		corebroker.AMQP091ClientPrefix,
		corebroker.AMQP1ClientPrefix,
		corebroker.HTTPClientPrefix,
		corebroker.CoAPClientPrefix,
	}
	for _, p := range prefixes {
		if after, ok := strings.CutPrefix(clientID, p); ok {
			return after
		}
	}
	return clientID
}
