package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

func TestInventorySearchBeyondFirstPageAndRetry(t *testing.T) {
	var fail atomic.Bool
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Query().Get("limit") != "200" || r.URL.Query().Get("state") != "connected" {
			t.Errorf("unbounded or incorrect query: %s", r.URL)
		}
		if fail.Load() {
			http.Error(w, "down", 503)
			return
		}
		token := r.URL.Query().Get("page_token")
		if token == "later/+" {
			writeJSON(w, map[string]any{"sessions": []session{{ClientID: "target", Protocol: "mqtt", Version: 5, Connected: true}}, "next_page_token": ""})
			return
		}
		sessions := make([]session, 200)
		for i := range sessions {
			sessions[i] = session{ClientID: fmt.Sprintf("first-%03d", i), Connected: true, Protocol: "mqtt", Version: 4}
		}
		writeJSON(w, map[string]any{"sessions": sessions, "next_page_token": "later/+"})
	}))
	defer upstream.Close()
	c := newCollector(nil, false)
	c.nodeURLs = map[string]string{"n1": upstream.URL, "n2": upstream.URL}
	get := func(params url.Values) inventoryPage {
		t.Helper()
		w := httptest.NewRecorder()
		c.inventoryPage(w, httptest.NewRequest("GET", "/?"+params.Encode(), nil))
		if w.Code != 200 {
			t.Fatalf("%d %s", w.Code, w.Body)
		}
		var page inventoryPage
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		return page
	}
	params := url.Values{"kind": {"sessions"}, "state": {"connected"}, "q": {"target"}, "protocol": {"mqtt5"}}
	first := get(params)
	if len(first.Sessions) != 0 || len(first.Next) != 2 {
		t.Fatalf("empty search match lost continuation: %+v", first)
	}
	cursor, _ := json.Marshal(first.Next)
	params.Set("cursor", string(cursor))
	fail.Store(true)
	failed := get(params)
	if len(failed.FailedNodes) != 2 || len(failed.Next) != 2 {
		t.Fatalf("cannot retry: %+v", failed)
	}
	fail.Store(false)
	last := get(params)
	if len(last.Sessions) != 2 || last.Sessions[0].NodeID == last.Sessions[1].NodeID || last.Sessions[0].Protocol != "mqtt5" || len(last.Next) != 0 {
		t.Fatalf("lost identity or data: %+v", last)
	}
	before := calls.Load()
	for _, invalid := range []string{"kind=invalid", "kind=sessions&node=unknown", "kind=sessions&cursor=%7B%7D", "kind=sessions&cursor=%7B%22unknown%22%3A%22x%22%7D"} {
		w := httptest.NewRecorder()
		c.inventoryPage(w, httptest.NewRequest("GET", "/?"+invalid, nil))
		if w.Code != 400 {
			t.Fatalf("accepted invalid request %s", invalid)
		}
	}
	if calls.Load() != before {
		t.Fatal("invalid request contacted upstream")
	}
}
func TestInventoryRepeatedTokenAndCancellation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"subscriptions": []subscription{}, "next_page_token": "repeat"})
	}))
	defer upstream.Close()
	c := newCollector(nil, false)
	c.nodeURLs = map[string]string{"n": upstream.URL}
	params := url.Values{"kind": {"subscriptions"}, "cursor": {`{"n":"repeat"}`}}
	w := httptest.NewRecorder()
	c.inventoryPage(w, httptest.NewRequest("GET", "/?"+params.Encode(), nil))
	var page inventoryPage
	json.Unmarshal(w.Body.Bytes(), &page)
	if len(page.FailedNodes) != 1 || page.Next["n"] != "repeat" {
		t.Fatalf("loop not detected %+v", page)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w = httptest.NewRecorder()
	c.inventoryPage(w, httptest.NewRequest("GET", "/?kind=sessions", nil).WithContext(ctx))
	json.Unmarshal(w.Body.Bytes(), &page)
	if len(page.FailedNodes) != 1 {
		t.Fatal("canceled request not reported")
	}
}
func TestReportedClusterMembersAreNotProbed(t *testing.T) {
	var forbidden atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forbidden.Add(1) }))
	defer peer.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"node_id": "configured", "cluster_mode": true, "cluster": map[string]any{"nodes": []map[string]any{{"id": "configured"}, {"id": "discovered", "address": peer.URL, "healthy": true, "version": "v1"}, {"id": "discovered", "address": peer.URL}}}})
	}))
	c := newCollector([]string{upstream.URL}, false)
	s := c.collect(context.Background(), false)
	if len(s.Nodes) != 2 || !s.Nodes[1].Unmonitored || s.Nodes[1].Address != "" || s.Nodes[1].ReportedHealthy == nil || !*s.Nodes[1].ReportedHealthy {
		t.Fatalf("missing membership metadata: %+v", s.Nodes)
	}
	if _, ok := c.nodeURLs["discovered"]; ok {
		t.Fatal("untrusted member became an admin destination")
	}
	if forbidden.Load() != 0 {
		t.Fatal("discovered address was probed")
	}
	c.publish(s)
	upstream.Close()
	offline := c.collect(context.Background(), false)
	if len(offline.Nodes) != 2 {
		t.Fatal("outage hid known membership")
	}
}
