package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueInspectionIsReadOnlyAndPreservesOffsets(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		method := strings.TrimPrefix(r.URL.Path, "/fluxmq.queue.v1.QueueService/")
		allowed := map[string]bool{"ListQueues": true, "GetQueueInfo": true, "GetQueue": true, "ListConsumerGroups": true, "GetPending": true, "ReadBatch": true}
		if !allowed[method] || r.Method != "POST" || r.Header.Get("Connect-Protocol-Version") != "1" {
			t.Errorf("unexpected RPC: %s %s", r.Method, r.URL)
		}
		var args map[string]any
		json.NewDecoder(r.Body).Decode(&args)
		if method == "ReadBatch" && (args["startOffset"] != "9007199254740993" || args["limit"] != float64(20)) {
			t.Errorf("offset lost precision or unbounded read: %+v", args)
		}
		if method == "GetQueue" && args["name"] != "factory/a+/#" {
			t.Errorf("queue name changed: %+v", args)
		}
		io.WriteString(w, `{"messages":[{"offset":"9007199254740993","value":"aGVsbG8="}]}`)
	}))
	defer upstream.Close()
	c := newCollector(nil, false)
	c.nodeURLs = map[string]string{"n": upstream.URL}
	for _, action := range []string{"list", "info", "config", "groups", "pending", "messages"} {
		q := url.Values{"node": {"n"}, "action": {action}, "queue": {"factory/a+/#"}, "group": {"group"}, "offset": {"9007199254740993"}}
		w := httptest.NewRecorder()
		handler(c).ServeHTTP(w, httptest.NewRequest("GET", "/console-api/queues?"+q.Encode(), nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), "9007199254740993") {
			t.Fatalf("%s: %d %s", action, w.Code, w.Body)
		}
	}
	for _, q := range []string{"node=n&action=Consume", "node=n&action=Ack", "node=http://evil&action=list", "node=n&action=messages&queue=q&offset=-1", "node=n&action=pending&queue=q"} {
		w := httptest.NewRecorder()
		handler(c).ServeHTTP(w, httptest.NewRequest("GET", "/console-api/queues?"+q, nil))
		if w.Code != 400 {
			t.Fatalf("invalid request accepted: %s", q)
		}
	}
	if calls.Load() != 6 {
		t.Fatal("invalid input reached broker")
	}
	w := httptest.NewRecorder()
	handler(c).ServeHTTP(w, httptest.NewRequest("POST", "/console-api/queues?node=n&action=Ack", nil))
	if w.Code != 405 {
		t.Fatal("write route exposed")
	}
}
func TestQueueServiceUnavailableAndOversize(t *testing.T) {
	for _, response := range []string{strings.Repeat("x", (8<<20)+1), "invalid json"} {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, response) }))
		c := newCollector(nil, false)
		c.nodeURLs = map[string]string{"n": upstream.URL}
		w := httptest.NewRecorder()
		c.queueRead(w, httptest.NewRequest("GET", "/?node=n&action=list", nil))
		upstream.Close()
		if w.Code != 502 {
			t.Fatal("invalid or oversized response accepted")
		}
	}
	upstream := httptest.NewServer(http.NotFoundHandler())
	defer upstream.Close()
	c := newCollector(nil, false)
	c.nodeURLs = map[string]string{"n": upstream.URL}
	w := httptest.NewRecorder()
	c.queueRead(w, httptest.NewRequest("GET", "/?node=n&action=list", nil))
	if w.Code != 502 || !strings.Contains(w.Body.String(), "404") {
		t.Fatal("disabled queue service hidden")
	}
}
func TestDemoQueueReadBounds(t *testing.T) {
	c := newCollector(nil, true)
	c.publish(demoSnapshot(time.Now()))
	w := httptest.NewRecorder()
	c.queueRead(w, httptest.NewRequest("GET", "/?node=eu-west-01&action=messages&queue=device-events&offset=0", nil))
	var data struct {
		Messages []struct {
			Offset string `json:"offset"`
		}
	}
	if err := json.Unmarshal(w.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	if len(data.Messages) != 20 || data.Messages[0].Offset != "12000" {
		t.Fatalf("invalid preview %+v", data)
	}
}

func TestDiagnosticsPreserveReportedAndAbsentMetrics(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"node_id":"n","stats":{"by_protocol":{"mqtt":{"connections":{"current":3},"errors":{"auth":2}},"amqp":{"channels":7,"local_principals":{"authentication":{"failure":4}}}},"queues":{"capture_dropped":9,"acknowledgments":{"poison_pending":3,"poison_pending_no_destination":2,"ack":123},"pending":{"current":17}}}}`)
	}))
	defer upstream.Close()
	c := newCollector([]string{upstream.URL}, false)
	s := c.collect(context.Background(), false)
	if len(s.Nodes) != 1 || !strings.Contains(string(s.Nodes[0].QueueMetrics), `"poison_pending":3`) || !strings.Contains(string(s.Nodes[0].ProtocolMetrics), `"failure":4`) {
		t.Fatalf("diagnostic counters were dropped: %+v", s.Nodes)
	}
	var absent overview
	if err := json.Unmarshal([]byte(`{"stats":{}}`), &absent); err != nil {
		t.Fatal(err)
	}
	if absent.RawQueueMetrics != nil || absent.RawProtocolMetrics != nil {
		t.Fatal("unreported metrics fabricated")
	}
}
