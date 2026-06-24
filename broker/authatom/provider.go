// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authatom

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/absmach/fluxmq/broker"
	atomv1 "github.com/absmach/fluxmq/pkg/proto/atom/v1"
	"github.com/absmach/fluxmq/topics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	defaultTimeout = 2 * time.Second

	ProtocolMQTT    = "mqtt"
	ProtocolHTTP    = "http"
	ProtocolCoAP    = "coap"
	ProtocolAMQP    = "amqp"
	ProtocolAMQP091 = "amqp091"

	objectKindResource = "resource"
)

var (
	_ broker.Authenticator = (*Provider)(nil)
	_ broker.Authorizer    = (*Provider)(nil)
)

// Options configures a direct Atom auth provider.
type Options struct {
	GRPCAddr string
	Insecure bool
	CAFile   string
	CertFile string
	KeyFile  string

	ServiceToken string
	Protocol     string
	Timeout      time.Duration
	Logger       *slog.Logger

	AuthnCacheTTL          time.Duration
	AliasCacheTTL          time.Duration
	DecisionCacheTTL       time.Duration
	UnsupportedTopicPolicy string
}

// Provider implements FluxMQ authentication and authorization against Atom.
type Provider struct {
	authn atomv1.AuthServiceClient
	authz atomv1.AuthzServiceClient
	alias atomv1.AliasServiceClient

	serviceToken string
	protocol     string
	timeout      time.Duration
	logger       *slog.Logger

	authnCache    *ttlCache[string]
	aliasCache    *ttlCache[string]
	decisionCache *ttlCache[bool]
}

// LoadServiceToken loads the Atom service credential from env first, then file.
func LoadServiceToken(envName, file string) (string, error) {
	if envName != "" {
		if token := strings.TrimSpace(os.Getenv(envName)); token != "" {
			return token, nil
		}
	}
	if file == "" {
		return "", fmt.Errorf("service token not configured")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("read service token file: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("service token file is empty")
	}
	return token, nil
}

// Dial opens a gRPC connection to Atom using the configured transport security.
func Dial(ctx context.Context, opts Options) (*grpc.ClientConn, error) {
	if strings.TrimSpace(opts.GRPCAddr) == "" {
		return nil, fmt.Errorf("atom grpc address is required")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	creds, err := transportCredentials(opts)
	if err != nil {
		return nil, err
	}

	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return grpc.DialContext(dialCtx, opts.GRPCAddr, grpc.WithTransportCredentials(creds), grpc.WithBlock()) //nolint:staticcheck // DialContext keeps existing project gRPC style simple.
}

// New creates a provider from an already-open Atom gRPC connection.
func New(conn grpc.ClientConnInterface, opts Options) *Provider {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	protocol := strings.ToLower(strings.TrimSpace(opts.Protocol))
	if protocol == "" {
		protocol = ProtocolMQTT
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Provider{
		authn:         atomv1.NewAuthServiceClient(conn),
		authz:         atomv1.NewAuthzServiceClient(conn),
		alias:         atomv1.NewAliasServiceClient(conn),
		serviceToken:  strings.TrimSpace(opts.ServiceToken),
		protocol:      protocol,
		timeout:       timeout,
		logger:        logger,
		authnCache:    newTTLCache[string](opts.AuthnCacheTTL),
		aliasCache:    newTTLCache[string](opts.AliasCacheTTL),
		decisionCache: newTTLCache[bool](opts.DecisionCacheTTL),
	}
}

// Authenticate validates a protocol credential as an Atom JWT or API key.
func (p *Provider) Authenticate(clientID, username, secret string) (*broker.AuthnResult, error) {
	token := normalizeBearerToken(secret)
	if token == "" {
		return &broker.AuthnResult{}, nil
	}

	cacheKey := tokenCacheKey(token)
	if entityID, ok := p.authnCache.Get(cacheKey); ok {
		return &broker.AuthnResult{Authenticated: true, ID: entityID}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	res, err := p.authn.Authenticate(ctx, &atomv1.AuthenticateRequest{Token: token})
	if err != nil {
		if isAuthnDeny(err) {
			p.logger.Info("atom_authenticate",
				slog.String("client_id", clientID),
				slog.String("protocol", p.protocol),
				slog.String("status", "denied"))
			return &broker.AuthnResult{}, nil
		}
		p.logger.Info("atom_authenticate",
			slog.String("client_id", clientID),
			slog.String("protocol", p.protocol),
			slog.String("status", "error"),
			slog.String("error", err.Error()))
		return &broker.AuthnResult{}, err
	}

	entityID := strings.TrimSpace(res.GetEntityId())
	if entityID == "" {
		return &broker.AuthnResult{}, nil
	}
	p.authnCache.Set(cacheKey, entityID)

	p.logger.Info("atom_authenticate",
		slog.String("client_id", clientID),
		slog.String("protocol", p.protocol),
		slog.String("status", "ok"))

	return &broker.AuthnResult{Authenticated: true, ID: entityID}, nil
}

// CanPublish checks Atom authorization for a publish operation.
func (p *Provider) CanPublish(externalID string, topic string) bool {
	return p.authorize(externalID, topic, actionPublish)
}

// CanSubscribe checks Atom authorization for a subscribe operation.
func (p *Provider) CanSubscribe(externalID string, filter string) bool {
	return p.authorize(externalID, filter, actionSubscribe)
}

func (p *Provider) authorize(externalID, rawTopic string, action topicAction) bool {
	externalID = strings.TrimSpace(externalID)
	if externalID == "" {
		return false
	}

	normalized := p.normalizeTopic(rawTopic, action)
	route, err := parseMagistralaTopic(normalized, action)
	if err != nil {
		p.logger.Info("atom_authorize",
			slog.String("external_id", externalID),
			slog.String("protocol", p.protocol),
			slog.String("action", string(action)),
			slog.String("topic", rawTopic),
			slog.String("status", "denied"),
			slog.String("reason", err.Error()))
		return false
	}
	route.raw = rawTopic

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	ctx = p.withServiceToken(ctx)

	resourceID, err := p.resolveResourceID(ctx, route)
	if err != nil {
		p.logger.Info("atom_authorize",
			slog.String("external_id", externalID),
			slog.String("protocol", p.protocol),
			slog.String("action", string(action)),
			slog.String("topic", rawTopic),
			slog.String("status", "error"),
			slog.String("error", err.Error()))
		return false
	}

	decisionKey := decisionCacheKey(p.protocol, externalID, resourceID, string(action), route.normalized)
	if allowed, ok := p.decisionCache.Get(decisionKey); ok {
		return allowed
	}

	req := &atomv1.CheckRequest{
		SubjectId:  externalID,
		Action:     string(action),
		ObjectKind: objectKindResource,
		ObjectId:   resourceID,
		Context:    p.checkContext(route, action),
	}

	res, err := p.authz.Check(ctx, req)
	if err != nil {
		p.logger.Info("atom_authorize",
			slog.String("external_id", externalID),
			slog.String("protocol", p.protocol),
			slog.String("action", string(action)),
			slog.String("topic", rawTopic),
			slog.String("resource_id", resourceID),
			slog.String("status", "error"),
			slog.String("error", err.Error()))
		return false
	}

	allowed := res.GetAllowed()
	p.decisionCache.Set(decisionKey, allowed)

	p.logger.Info("atom_authorize",
		slog.String("external_id", externalID),
		slog.String("protocol", p.protocol),
		slog.String("action", string(action)),
		slog.String("topic", rawTopic),
		slog.String("resource_id", resourceID),
		slog.String("status", "ok"),
		slog.Bool("allowed", allowed),
		slog.String("reason", res.GetReason()))

	return allowed
}

func (p *Provider) normalizeTopic(topic string, action topicAction) string {
	switch p.protocol {
	case ProtocolAMQP, ProtocolAMQP091:
		if action == actionSubscribe {
			return topics.AMQPFilterToMQTT(topic)
		}
		return topics.AMQPTopicToMQTT(topic)
	default:
		return topic
	}
}

func (p *Provider) resolveResourceID(ctx context.Context, route topicRoute) (string, error) {
	if route.channelUUID {
		return route.channel, nil
	}

	cacheKey := aliasCacheKey(route)
	if objectID, ok := p.aliasCache.Get(cacheKey); ok {
		return objectID, nil
	}

	req := &atomv1.ResolveAliasRequest{
		ObjectKind:  objectKindResource,
		ObjectAlias: route.channelAlias,
	}
	if route.tenantUUID {
		req.TenantId = route.tenant
	} else {
		req.TenantAlias = route.tenantAlias
	}

	res, err := p.alias.ResolveAlias(ctx, req)
	if err != nil {
		return "", fmt.Errorf("resolve alias: %w", err)
	}
	objectID := strings.TrimSpace(res.GetObjectId())
	if objectID == "" {
		return "", fmt.Errorf("resolve alias: empty object_id")
	}
	p.aliasCache.Set(cacheKey, objectID)
	return objectID, nil
}

func (p *Provider) checkContext(route topicRoute, action topicAction) map[string]string {
	ctx := map[string]string{
		"protocol":      p.protocol,
		"raw_topic":     route.raw,
		"topic":         route.normalized,
		"route_tenant":  route.tenant,
		"route_channel": route.channel,
	}
	if action == actionSubscribe {
		ctx["filter"] = route.subpath
	} else {
		ctx["subtopic"] = route.subpath
	}
	return ctx
}

func (p *Provider) withServiceToken(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+p.serviceToken)
}

func normalizeBearerToken(secret string) string {
	secret = strings.TrimSpace(secret)
	if strings.HasPrefix(strings.ToLower(secret), "bearer ") {
		return strings.TrimSpace(secret[len("bearer "):])
	}
	return secret
}

func tokenCacheKey(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func aliasCacheKey(route topicRoute) string {
	if route.tenantUUID {
		return "tenant_id:" + route.tenant + "|resource:" + route.channelAlias
	}
	return "tenant_alias:" + route.tenantAlias + "|resource:" + route.channelAlias
}

func decisionCacheKey(protocol, externalID, resourceID, action, topic string) string {
	return protocol + "|" + action + "|" + externalID + "|" + resourceID + "|" + topic
}

func isAuthnDeny(err error) bool {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument:
		return true
	default:
		return false
	}
}

func transportCredentials(opts Options) (credentials.TransportCredentials, error) {
	if opts.Insecure {
		return insecure.NewCredentials(), nil
	}

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if opts.CAFile != "" {
		caPEM, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read atom ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("parse atom ca file: no certificates found")
		}
		tlsCfg.RootCAs = pool
	}
	if opts.CertFile != "" || opts.KeyFile != "" {
		if opts.CertFile == "" || opts.KeyFile == "" {
			return nil, fmt.Errorf("atom cert_file and key_file must be set together")
		}
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load atom client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return credentials.NewTLS(tlsCfg), nil
}
