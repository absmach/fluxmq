// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"
	amqp1broker "github.com/absmach/fluxmq/amqp1/broker"
	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/stretchr/testify/require"
)

func TestAMQP1ClientInventoryAndStats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker := amqp1broker.New(nil, nil, nil)
	server, client := net.Pipe()
	go broker.HandleConnection(ctx, server)
	conn, err := amqp.NewConn(ctx, client, &amqp.ConnOptions{ContainerID: "inventory-client"})
	require.NoError(t, err)
	defer conn.Close()
	session, err := conn.NewSession(ctx, nil)
	require.NoError(t, err)
	receiver, err := session.NewReceiver(ctx, "orders.*", nil)
	require.NoError(t, err)
	defer receiver.Close(ctx)
	srv := New(Config{}, nil, nil, nil, nil, nil, nil, nil)
	srv.SetAMQP1Broker(broker)
	id := corebroker.PrefixedAMQP1ClientID("inventory-client")
	require.Eventually(t, func() bool { ss := broker.ConnectionSnapshots(); return len(ss) == 1 && len(ss[0].Subscriptions) == 1 }, time.Second, time.Millisecond)
	list, next, err := srv.listSessions(ctx, "", "connected", 1, "")
	require.NoError(t, err)
	require.Empty(t, next)
	require.Len(t, list, 1)
	require.Equal(t, id, list[0].ClientID)
	require.Equal(t, "amqp1.0", list[0].Protocol)
	require.Equal(t, "tcp", list[0].Transport)
	require.Equal(t, 1, list[0].SubscriptionCount)
	for _, state := range []string{"disconnected"} {
		ss, _, err := srv.listSessions(ctx, "", state, 0, "")
		require.NoError(t, err)
		require.Empty(t, ss)
	}
	ss, _, err := srv.listSessions(ctx, "missing", "all", 0, "")
	require.NoError(t, err)
	require.Empty(t, ss)
	ss, _, err = srv.listSessions(ctx, "", "all", 1, id)
	require.NoError(t, err)
	require.Empty(t, ss)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/"+url.PathEscape(id), nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var detail sessionResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &detail))
	require.Len(t, detail.Subscriptions, 1)
	require.Equal(t, "orders/+", detail.Subscriptions[0].Filter)
	subs, _, err := srv.listSubscriptions("connected", "", 0, "")
	require.NoError(t, err)
	require.Len(t, subs, 1)
	clients, _, err := srv.listSubscriptionClients("orders/+", "connected", "", 0, "")
	require.NoError(t, err)
	require.Len(t, clients, 1)
	require.Equal(t, id, clients[0].ClientID)
	stats := srv.buildStatsResponse()
	require.NotNil(t, stats.ByProtocol.AMQP1)
	require.Equal(t, uint64(1), stats.Connections.Current)
	require.Equal(t, uint64(1), stats.ByProtocol.AMQP1.Connections.Current)
	require.Equal(t, uint64(1), stats.ByProtocol.AMQP1.Sessions)
	for _, path := range []string{"/api/v1/stats", "/api/v1/overview"} {
		rec := httptest.NewRecorder()
		srv.httpServer.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusOK, rec.Code, path)
		if path == "/api/v1/overview" {
			var overview overviewResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &overview))
			require.Equal(t, sessionSummary{Connected: 1, Total: 1}, overview.Sessions)
		}
	}

	_ = conn.Close() // net.Pipe may close before the peer close response is written.
	require.Eventually(t, func() bool { ss, _, _ := srv.listSessions(ctx, "", "all", 0, ""); return len(ss) == 0 }, time.Second, time.Millisecond)
	rec = httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}
