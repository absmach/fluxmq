package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubscriptionClientsPaginationAndNodeIdentity(t *testing.T) {
	var aCalls, bCalls atomic.Int32
	filter := "factory/+/telemetry/#"
	upstream := func(calls *atomic.Int32, more bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			if r.URL.EscapedPath() != "/api/v1/subscriptions/"+url.PathEscape(filter)+"/clients" || r.URL.Query().Get("state") != "connected" || r.URL.Query().Get("limit") != "200" {
				t.Errorf("wrong upstream request: %s", r.URL.String())
			}
			token := ""
			id := "same-client"
			if more {
				token = "client/with+reserved?token"
				if r.URL.Query().Get("page_token") == token {
					id = "later-client"
					token = ""
				}
			}
			writeJSON(w, map[string]any{"clients": []map[string]any{{"client_id": id, "qos": 2}}, "next_page_token": token})
		}))
	}
	a, b := upstream(&aCalls, true), upstream(&bCalls, false)
	defer a.Close()
	defer b.Close()
	c := newCollector(nil, false)
	c.nodeURLs = map[string]string{"node-a": a.URL, "node-b": b.URL}
	get := func(params url.Values) subscriptionClientsPage {
		t.Helper()
		w := httptest.NewRecorder()
		c.subscriptionClients(w, httptest.NewRequest("GET", "/console-api/subscription-clients?"+params.Encode(), nil))
		if w.Code != 200 {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		var page subscriptionClientsPage
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		return page
	}
	first := get(url.Values{"filter": {filter}})
	if len(first.Clients) != 2 || first.Clients[0].NodeID != "node-a" || first.Clients[1].NodeID != "node-b" || len(first.Next) != 1 {
		t.Fatalf("node identities or next pages lost: %+v", first)
	}
	cursor, _ := json.Marshal(first.Next)
	next := get(url.Values{"filter": {filter}, "cursor": {string(cursor)}})
	if len(next.Clients) != 1 || next.Clients[0].ClientID != "later-client" || len(next.Next) != 0 || aCalls.Load() != 2 || bCalls.Load() != 1 {
		t.Fatalf("invalid continuation: %+v", next)
	}
	for _, q := range []url.Values{{"filter": {filter}, "node": {"unknown"}}, {"filter": {filter}, "cursor": {`{"unknown":"x"}`}}, {"filter": {filter}, "cursor": {`{}`}}} {
		w := httptest.NewRecorder()
		c.subscriptionClients(w, httptest.NewRequest("GET", "/?"+q.Encode(), nil))
		if w.Code != 400 {
			t.Fatalf("invalid target/cursor accepted: %d", w.Code)
		}
	}
	if aCalls.Load() != 2 || bCalls.Load() != 1 {
		t.Fatal("invalid inputs contacted an upstream")
	}
	a.Close()
	partial := get(url.Values{"filter": {filter}})
	if len(partial.FailedNodes) != 1 || partial.FailedNodes[0] != "node-a" || len(partial.Clients) != 1 {
		t.Fatalf("partial failure hidden: %+v", partial)
	}
	if _, retryable := partial.Next["node-a"]; !retryable {
		t.Fatal("failed node cannot be retried")
	}
}

func TestDemoSubscriptionCountsMatchClientDrilldown(t *testing.T) {
	c := newCollector(nil, true)
	c.publish(demoSnapshot(time.Now()))
	for _, sub := range c.current.Subscriptions {
		q := url.Values{"filter": {sub.Filter}, "node": {sub.NodeID}}
		w := httptest.NewRecorder()
		c.subscriptionClients(w, httptest.NewRequest("GET", "/?"+q.Encode(), nil))
		var page subscriptionClientsPage
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		if len(page.Clients) != sub.Subscribers {
			t.Fatalf("%s count %d != %d", sub.Filter, len(page.Clients), sub.Subscribers)
		}
		for _, client := range page.Clients {
			if client.NodeID != sub.NodeID {
				t.Fatal("node scope was ignored")
			}
		}
	}
}

func TestBrokerMetadataAndClientSettingsArePreserved(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/overview" {
			writeJSON(w, map[string]any{"node_id": "n1", "cluster_mode": true, "is_leader": false, "cluster": map[string]any{"nodes": []map[string]any{{"id": "n1", "address": "10.0.0.1:7946", "version": "v1"}}}, "stats": map[string]any{"errors": map[string]int{"protocol": 4}, "by_protocol": map[string]any{"mqtt": map[string]any{"errors": map[string]int{"auth": 1, "authz": 2, "packet": 3}}}}})
			return
		}
		writeJSON(w, map[string]any{"client_id": "client/a", "connection_name": "worker", "protocol": "mqtt", "version": 5, "connected": false, "clean_start": false, "expiry_interval": 0, "receive_maximum": 32, "max_packet_size": 4096, "topic_alias_max": 8, "has_will": true, "request_response": false, "request_problem": true, "disconnected_at": "2026-11-22T10:00:00Z", "subscriptions": []map[string]any{{"filter": "factory/+", "qos": 1, "no_local": false, "retain_as_published": true, "retain_handling": 0, "subscription_id": 0, "consumer_group": "workers"}}})
	}))
	defer upstream.Close()
	c := newCollector([]string{upstream.URL}, false)
	snap := c.collect(context.Background(), false)
	n := snap.Nodes[0]
	if n.ClusterMode == nil || !*n.ClusterMode || n.Version != "v1" || n.TransportAddress != "10.0.0.1:7946" || n.Errors != 10 || n.AuthzErrors != 2 {
		t.Fatalf("node metadata lost: %+v", n)
	}
	w := httptest.NewRecorder()
	c.detail(w, httptest.NewRequest("GET", "/?node=n1&id=client%2Fa", nil))
	var got session
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.NodeID != "n1" || got.ConnectionName != "worker" || got.CleanStart == nil || *got.CleanStart || got.ExpiryInterval == nil || *got.ExpiryInterval != 0 || got.ReceiveMaximum == nil || *got.ReceiveMaximum != 32 || got.HasWill == nil || !*got.HasWill || got.DisconnectedAt == "" {
		t.Fatalf("client settings lost: %+v", got)
	}
	if len(got.Subscriptions) != 1 || got.Subscriptions[0].NoLocal == nil || *got.Subscriptions[0].NoLocal || got.Subscriptions[0].SubscriptionID == nil || *got.Subscriptions[0].SubscriptionID != 0 || got.Subscriptions[0].ConsumerGroup != "workers" {
		t.Fatalf("subscription options lost: %+v", got.Subscriptions)
	}
}
