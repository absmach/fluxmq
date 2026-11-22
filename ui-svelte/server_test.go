package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRoutesAndExplicitDemo(t *testing.T) {
	c := newCollector(nil, true)
	c.publish(demoSnapshot(time.Now()))
	h := handler(c)
	for _, tc := range []struct {
		path    string
		status  int
		content string
	}{
		{"/dashboard/sessions", 200, "text/html"},
		{"/console-api/snapshot", 200, "application/json"},
		{"/console-api/not-a-route", 404, "text/plain"},
		{"/missing.js", 404, "text/plain"},
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.status || !strings.HasPrefix(w.Header().Get("Content-Type"), tc.content) {
			t.Errorf("%s: %d %s", tc.path, w.Code, w.Header().Get("Content-Type"))
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/console-api/snapshot", nil))
	var got snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Mode != "demo" || len(got.Nodes) != 3 || len(got.Sessions) != 36 {
		t.Fatalf("unexpected demo snapshot: %+v", got)
	}
}

func TestStreamSnapshotUpdatesAndDisconnect(t *testing.T) {
	c := newCollector(nil, true)
	c.publish(demoSnapshot(time.Now()))
	server := httptest.NewServer(handler(c))
	defer server.Close()
	response, err := http.Get(server.URL + "/console-api/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 4096), 4<<20)
	next := func() snapshot {
		t.Helper()
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				var s snapshot
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &s); err != nil {
					t.Fatal(err)
				}
				return s
			}
		}
		t.Fatalf("missing SSE snapshot: %v", scanner.Err())
		return snapshot{}
	}
	initial := next()
	if len(initial.History["all"]) != 1 {
		t.Fatal("initial event must include history")
	}
	update := demoSnapshot(time.Now().Add(2 * time.Second))
	c.publish(update)
	latest := next()
	if latest.Timestamp != update.Timestamp || latest.History != nil {
		t.Fatal("update should include newest snapshot without replaying history")
	}
	response.Body.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.mu.RLock()
		count := len(c.listeners)
		c.mu.RUnlock()
		if count == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("disconnected client was not removed")
}

func TestLiveCollectionRatesAndUnreachableNode(t *testing.T) {
	received := float64(1200)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/overview":
			writeJSON(w, map[string]any{"node_id": "node-a", "is_leader": true, "uptime_seconds": 60, "sessions": map[string]int{"connected": 7}, "stats": map[string]any{"messages": map[string]float64{"received": received, "sent": 900}, "bytes": map[string]int{"received": 12000, "sent": 9000}}})
		case "/api/v1/sessions":
			writeJSON(w, map[string]any{"sessions": []session{{ClientID: "client-a", Connected: true}}, "next_page_token": "more"})
		case "/api/v1/subscriptions":
			writeJSON(w, map[string]any{"subscriptions": []subscription{{Filter: "devices/+", Subscribers: 3}}})
		default:
			http.NotFound(w, r)
		}
	}))
	c := newCollector([]string{upstream.URL}, false)
	first := c.collect(context.Background(), true)
	if first.Mode != "live" || len(first.Nodes) != 1 || !first.Nodes[0].Healthy || first.Nodes[0].Connections != 7 || first.Sessions[0].NodeID != "node-a" || !first.Limited {
		t.Fatalf("bad live snapshot: %+v", first)
	}
	first.Timestamp -= 2000
	first.Nodes[0].Received = 1000
	c.publish(first)
	second := c.collect(context.Background(), false)
	if rate := second.Points["all"].Incoming; rate < 90 || rate > 101 {
		t.Fatalf("incorrect elapsed-time rate: %v", rate)
	}
	// Counter resets must not create negative rates or spikes.
	second.Timestamp -= 2000
	second.Nodes[0].Received = 5000
	c.publish(second)
	reset := c.collect(context.Background(), false)
	if reset.Points["all"].Incoming != 0 {
		t.Fatal("counter reset produced a nonzero rate")
	}
	c.publish(reset)
	upstream.Close()
	failed := c.collect(context.Background(), false)
	if failed.Warning == "" || failed.Nodes[0].Healthy || failed.Nodes[0].Connections != 7 || failed.Points["all"].Incoming != 0 {
		t.Fatalf("failed node not marked stale: %+v", failed)
	}
}

func TestHistoryIsBoundedAndSlowSubscribersDoNotBlock(t *testing.T) {
	c := newCollector(nil, true)
	ch := make(chan []byte, 1)
	c.listeners[ch] = struct{}{}
	for i := 0; i < 500; i++ {
		c.publish(snapshot{Timestamp: int64(i), Points: map[string]point{"all": {Time: int64(i)}}})
	}
	if len(c.history["all"]) != 450 {
		t.Fatal("history should be bounded")
	}
	var last snapshot
	if err := json.Unmarshal(<-ch, &last); err != nil {
		t.Fatal(err)
	}
	if last.Timestamp != 499 {
		t.Fatal("slow listener should receive the newest snapshot")
	}
}

func TestRecoveryReplacesTemporaryNodeIdentity(t *testing.T) {
	available := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !available {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/api/v1/overview" {
			writeJSON(w, map[string]any{"node_id": "recovered-node"})
		} else {
			writeJSON(w, map[string]any{"sessions": []session{{ClientID: "live-client"}}})
		}
	}))
	defer upstream.Close()
	c := newCollector([]string{upstream.URL}, false)
	c.publish(c.collect(context.Background(), false))
	available = true
	c.publish(c.collect(context.Background(), false))
	if len(c.nodeURLs) != 1 || c.nodeURLs["recovered-node"] != upstream.URL {
		t.Fatalf("stale identities after recovery: %+v", c.nodeURLs)
	}
	w := httptest.NewRecorder()
	c.inventoryPage(w, httptest.NewRequest("GET", "/console-api/inventory?kind=sessions", nil))
	var page inventoryPage
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Sessions) != 1 || page.Sessions[0].NodeID != "recovered-node" {
		t.Fatalf("duplicate inventory after recovery: %+v", page)
	}
}
