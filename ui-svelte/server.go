// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// FluxMQ Console serves embedded Svelte assets and a shared, read-only SSE feed.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

//go:embed all:dist
var assets embed.FS

type point struct {
	BytesIn     float64 `json:"bytes_in"`
	BytesOut    float64 `json:"bytes_out"`
	Time        int64   `json:"time"`
	Incoming    float64 `json:"incoming"`
	Outgoing    float64 `json:"outgoing"`
	Bandwidth   float64 `json:"bandwidth"`
	Connections int     `json:"connections"`
}
type node struct {
	QueueMetrics     json.RawMessage `json:"queue_metrics,omitempty"`
	ProtocolMetrics  json.RawMessage `json:"protocol_metrics,omitempty"`
	Unmonitored      bool            `json:"unmonitored,omitempty"`
	ReportedHealthy  *bool           `json:"reported_healthy,omitempty"`
	RetainedMessages int             `json:"retained_messages"`
	ClusterMode      *bool           `json:"cluster_mode,omitempty"`
	Version          string          `json:"version,omitempty"`
	TransportAddress string          `json:"transport_address,omitempty"`
	ProtocolErrors   int             `json:"protocol_errors"`
	AuthErrors       int             `json:"auth_errors"`
	AuthzErrors      int             `json:"authz_errors"`
	PacketErrors     int             `json:"packet_errors"`
	ID               string          `json:"id"`
	Address          string          `json:"address"`
	Leader           bool            `json:"leader"`
	Healthy          bool            `json:"healthy"`
	Uptime           float64         `json:"uptime"`
	Connections      int             `json:"connections"`
	Subscriptions    int             `json:"subscriptions"`
	Received         float64         `json:"received"`
	Sent             float64         `json:"sent"`
	BytesIn          float64         `json:"bytes_in"`
	BytesOut         float64         `json:"bytes_out"`
	Errors           int             `json:"errors"`
	Incoming         float64         `json:"incoming"`
	Outgoing         float64         `json:"outgoing"`
}
type session struct {
	ConnectionName    string                `json:"connection_name,omitempty"`
	State             string                `json:"state"`
	Version           int                   `json:"version"`
	CleanStart        *bool                 `json:"clean_start,omitempty"`
	ExpiryInterval    *uint32               `json:"expiry_interval,omitempty"`
	DisconnectedAt    string                `json:"disconnected_at,omitempty"`
	ReceiveMaximum    *uint16               `json:"receive_maximum,omitempty"`
	MaxPacketSize     *uint32               `json:"max_packet_size,omitempty"`
	TopicAliasMax     *uint16               `json:"topic_alias_max,omitempty"`
	RequestResponse   *bool                 `json:"request_response,omitempty"`
	RequestProblem    *bool                 `json:"request_problem,omitempty"`
	HasWill           *bool                 `json:"has_will,omitempty"`
	ClientID          string                `json:"client_id"`
	NodeID            string                `json:"node_id"`
	Protocol          string                `json:"protocol"`
	Connected         bool                  `json:"connected"`
	SubscriptionCount int                   `json:"subscription_count"`
	InflightCount     int                   `json:"inflight_count"`
	OfflineQueueDepth int                   `json:"offline_queue_depth"`
	ConnectedAt       string                `json:"connected_at,omitempty"`
	Subscriptions     []sessionSubscription `json:"subscriptions,omitempty"`
}
type sessionSubscription struct {
	NoLocal           *bool   `json:"no_local,omitempty"`
	RetainAsPublished *bool   `json:"retain_as_published,omitempty"`
	RetainHandling    *uint8  `json:"retain_handling,omitempty"`
	ConsumerGroup     string  `json:"consumer_group,omitempty"`
	SubscriptionID    *uint32 `json:"subscription_id,omitempty"`
	Filter            string  `json:"filter"`
	QoS               int     `json:"qos"`
}
type subscription struct {
	Filter      string `json:"filter"`
	Subscribers int    `json:"subscriber_count"`
	QoS         int    `json:"max_qos"`
	NodeID      string `json:"node_id"`
}
type snapshot struct {
	Mode          string             `json:"mode"`
	Timestamp     int64              `json:"timestamp"`
	Nodes         []node             `json:"nodes"`
	Sessions      []session          `json:"sessions"`
	Subscriptions []subscription     `json:"subscriptions"`
	Points        map[string]point   `json:"points"`
	History       map[string][]point `json:"history,omitempty"`
	Warning       string             `json:"warning"`
	Limited       bool               `json:"inventory_limited"`
}
type collector struct {
	mu        sync.RWMutex
	current   snapshot
	history   map[string][]point
	listeners map[chan []byte]struct{}
	urls      []string
	nodeURLs  map[string]string
	client    *http.Client
	demo      bool
}

func newCollector(urls []string, demo bool) *collector {
	return &collector{urls: urls, demo: demo, history: map[string][]point{}, nodeURLs: map[string]string{}, listeners: map[chan []byte]struct{}{}, client: &http.Client{Timeout: 4 * time.Second}}
}
func (c *collector) publish(s snapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, p := range s.Points {
		history := append(c.history[id], p)
		if len(history) > 450 {
			history = history[len(history)-450:]
		}
		c.history[id] = history
	}
	c.current = s
	data, _ := json.Marshal(s)
	for ch := range c.listeners {
		select {
		case ch <- data:
		default:
			// A slow browser receives the newest state instead of blocking collection.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- data:
			default:
			}
		}
	}
}
func (c *collector) fullLocked() snapshot {
	s := c.current
	s.History = c.history
	return s
}
func (c *collector) snapshotHandler(w http.ResponseWriter, r *http.Request) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	writeJSON(w, c.fullLocked())
}
func (c *collector) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unavailable", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	ch := make(chan []byte, 1)
	c.mu.Lock()
	c.listeners[ch] = struct{}{}
	initial, _ := json.Marshal(c.fullLocked())
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.listeners, ch); c.mu.Unlock() }()
	send := func(data []byte) bool {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(10 * time.Second))
		if _, err := fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", data); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}
	if !send(initial) {
		return
	}
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case data := <-ch:
			if !send(data) {
				return
			}
		case <-heartbeat.C:
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (c *collector) get(ctx context.Context, endpoint string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("broker returned HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(dst)
}

type clusterPeer struct {
	ID      string  `json:"id"`
	Address string  `json:"address"`
	Version string  `json:"version"`
	Leader  bool    `json:"leader"`
	Healthy *bool   `json:"healthy"`
	Uptime  float64 `json:"uptime_seconds"`
}
type overview struct {
	RawProtocolMetrics json.RawMessage `json:"-"`
	RawQueueMetrics    json.RawMessage `json:"-"`
	ClusterMode        *bool           `json:"cluster_mode"`
	Cluster            struct {
		Nodes []clusterPeer `json:"nodes"`
	} `json:"cluster"`
	ID       string  `json:"node_id"`
	Leader   bool    `json:"is_leader"`
	Uptime   float64 `json:"uptime_seconds"`
	Sessions struct {
		Connected int `json:"connected"`
	} `json:"sessions"`
	Stats struct {
		Messages struct {
			Received float64 `json:"received"`
			Sent     float64 `json:"sent"`
		} `json:"messages"`
		Bytes struct {
			Received float64 `json:"received"`
			Sent     float64 `json:"sent"`
		} `json:"bytes"`
		Errors struct {
			Protocol int `json:"protocol"`
		} `json:"errors"`
		ByProtocol struct {
			MQTT struct {
				Subscriptions struct {
					Active           int `json:"active"`
					RetainedMessages int `json:"retained_messages"`
				} `json:"subscriptions"`
				Errors struct {
					Auth   int `json:"auth"`
					Authz  int `json:"authz"`
					Packet int `json:"packet"`
				} `json:"errors"`
			} `json:"mqtt"`
			AMQP struct {
				Consumers int `json:"consumers"`
			} `json:"amqp"`
		} `json:"by_protocol"`
	} `json:"stats"`
}

// Keep the protocol and queue diagnostic sections intact: absence means that
// the broker does not report this capability, not a zero-valued counter.
func (o *overview) UnmarshalJSON(data []byte) error {
	type plain overview
	if err := json.Unmarshal(data, (*plain)(o)); err != nil {
		return err
	}
	var diagnostic struct {
		Stats struct {
			Protocol json.RawMessage `json:"by_protocol"`
			Queues   json.RawMessage `json:"queues"`
		} `json:"stats"`
	}
	if err := json.Unmarshal(data, &diagnostic); err != nil {
		return err
	}
	o.RawProtocolMetrics, o.RawQueueMetrics = diagnostic.Stats.Protocol, diagnostic.Stats.Queues
	return nil
}

func (c *collector) collect(ctx context.Context, inventory bool) snapshot {
	now := time.Now()
	c.mu.RLock()
	previous := c.current
	c.mu.RUnlock()
	s := snapshot{Mode: "live", Timestamp: now.UnixMilli(), Nodes: []node{}, Sessions: previous.Sessions, Subscriptions: previous.Subscriptions, Points: map[string]point{}, Limited: previous.Limited}
	if s.Sessions == nil {
		s.Sessions = []session{}
	}
	if s.Subscriptions == nil {
		s.Subscriptions = []subscription{}
	}
	type result struct {
		node           node
		url            string
		sessions       []session
		subscriptions  []subscription
		limited        bool
		err            error
		inventoryError bool
		peers          []clusterPeer
	}
	results := make(chan result, len(c.urls))
	for _, base := range c.urls {
		go func(base string) {
			var raw overview
			out := result{url: base}
			if err := c.get(ctx, base+"/api/v1/overview", &raw); err != nil {
				out.err = err
				out.node = node{ID: base, Address: base}
				for _, old := range previous.Nodes {
					if old.Address == base {
						out.node = old
						out.node.Healthy = false
						out.node.Incoming = 0
						out.node.Outgoing = 0
					}
				}
				results <- out
				return
			}
			id := raw.ID
			if id == "" {
				id = base
			}
			n := node{ID: id, Address: base, Leader: raw.Leader, Healthy: true, Uptime: raw.Uptime, Connections: raw.Sessions.Connected, Subscriptions: raw.Stats.ByProtocol.MQTT.Subscriptions.Active + raw.Stats.ByProtocol.AMQP.Consumers, Received: raw.Stats.Messages.Received, Sent: raw.Stats.Messages.Sent, BytesIn: raw.Stats.Bytes.Received, BytesOut: raw.Stats.Bytes.Sent, Errors: raw.Stats.Errors.Protocol + raw.Stats.ByProtocol.MQTT.Errors.Auth + raw.Stats.ByProtocol.MQTT.Errors.Authz + raw.Stats.ByProtocol.MQTT.Errors.Packet}
			n.RetainedMessages = raw.Stats.ByProtocol.MQTT.Subscriptions.RetainedMessages
			n.ClusterMode = raw.ClusterMode
			n.QueueMetrics = raw.RawQueueMetrics
			n.ProtocolMetrics = raw.RawProtocolMetrics
			n.ProtocolErrors = raw.Stats.Errors.Protocol
			n.AuthErrors = raw.Stats.ByProtocol.MQTT.Errors.Auth
			n.AuthzErrors = raw.Stats.ByProtocol.MQTT.Errors.Authz
			n.PacketErrors = raw.Stats.ByProtocol.MQTT.Errors.Packet
			for _, peer := range raw.Cluster.Nodes {
				if peer.ID == id {
					n.Version = peer.Version
					n.TransportAddress = peer.Address
				}
			}
			out.node = n
			out.peers = raw.Cluster.Nodes
			if inventory {
				var list struct {
					Sessions []session `json:"sessions"`
					Next     string    `json:"next_page_token"`
				}
				if err := c.get(ctx, base+"/api/v1/sessions?limit=200", &list); err != nil {
					out.inventoryError = true
				} else {
					out.sessions = list.Sessions
					out.limited = list.Next != ""
					for i := range out.sessions {
						out.sessions[i].NodeID = id
					}
				}
				var subs struct {
					Subscriptions []subscription `json:"subscriptions"`
					Next          string         `json:"next_page_token"`
				}
				if err := c.get(ctx, base+"/api/v1/subscriptions?limit=200", &subs); err != nil {
					out.inventoryError = true
				} else {
					out.subscriptions = subs.Subscriptions
					out.limited = out.limited || subs.Next != ""
					for i := range out.subscriptions {
						out.subscriptions[i].NodeID = id
					}
				}
			}
			results <- out
		}(base)
	}
	if inventory {
		s.Sessions = []session{}
		s.Subscriptions = []subscription{}
		s.Limited = false
	}
	all := point{Time: s.Timestamp}
	// Preserve configured node order even when requests complete out of order.
	byURL := map[string]result{}
	for range c.urls {
		r := <-results
		byURL[r.url] = r
	}
	for _, base := range c.urls {
		r := byURL[base]
		n := r.node
		if r.err != nil {
			s.Warning = "Some brokers are unreachable. Their last known values are retained; current rates exclude them."
		}
		if r.inventoryError {
			s.Warning = "Session or subscription inventory could not be refreshed. Some results may be missing."
		}
		elapsed := float64(s.Timestamp-previous.Timestamp) / 1000
		p := point{Time: s.Timestamp, Connections: n.Connections}
		for _, old := range previous.Nodes {
			if old.ID == n.ID && old.Healthy && n.Healthy && elapsed > 0 {
				p.Incoming = math.Max(0, n.Received-old.Received) / elapsed
				p.Outgoing = math.Max(0, n.Sent-old.Sent) / elapsed
				p.BytesIn = math.Max(0, n.BytesIn-old.BytesIn) / elapsed
				p.BytesOut = math.Max(0, n.BytesOut-old.BytesOut) / elapsed
				p.Bandwidth = p.BytesIn + p.BytesOut
			}
		}
		n.Incoming = p.Incoming
		n.Outgoing = p.Outgoing
		s.Nodes = append(s.Nodes, n)
		if n.Healthy {
			s.Points[n.ID] = p
			all.Incoming += p.Incoming
			all.Outgoing += p.Outgoing
			all.Bandwidth += p.Bandwidth
			all.BytesIn += p.BytesIn
			all.BytesOut += p.BytesOut
			all.Connections += p.Connections
		}
		c.mu.Lock()
		c.nodeURLs[n.ID] = base
		c.mu.Unlock()
		if inventory {
			s.Sessions = append(s.Sessions, r.sessions...)
			s.Subscriptions = append(s.Subscriptions, r.subscriptions...)
			s.Limited = s.Limited || r.limited
		}
	}
	known := map[string]bool{}
	for _, n := range s.Nodes {
		known[n.ID] = true
	}
	for _, base := range c.urls {
		for _, peer := range byURL[base].peers {
			if peer.ID == "" || known[peer.ID] {
				continue
			}
			known[peer.ID] = true
			s.Nodes = append(s.Nodes, node{ID: peer.ID, TransportAddress: peer.Address, Version: peer.Version, Leader: peer.Leader, ClusterMode: valuePtr(true), Uptime: peer.Uptime, Unmonitored: true, ReportedHealthy: peer.Healthy})
		}
	}
	// Keep known membership when every overview request fails; do not hide peers
	// just because the admin service is temporarily unreachable.
	allFailed := len(c.urls) > 0
	for _, r := range byURL {
		allFailed = allFailed && r.err != nil
	}
	if allFailed {
		for _, old := range previous.Nodes {
			if old.Unmonitored && !known[old.ID] {
				s.Nodes = append(s.Nodes, old)
				known[old.ID] = true
			}
		}
	}
	s.Points["all"] = all
	return s
}

var demoTopics = []string{"factory/+/telemetry", "devices/+/status", "fleet/+/location", "events/orders/#", "sensors/+/temperature", "system/health", "energy/+/readings", "commands/+/ack"}

func valuePtr[T any](value T) *T { return &value }

func demoSnapshot(at time.Time) snapshot {
	s := snapshot{Mode: "demo", Timestamp: at.UnixMilli(), Nodes: []node{}, Sessions: []session{}, Subscriptions: []subscription{}, Points: map[string]point{}}
	t := float64(at.UnixMilli()) / 1000
	all := point{Time: s.Timestamp}
	names := []string{"eu-west-01", "eu-west-02", "eu-west-03"}
	for i, id := range names {
		phase := t/16 + float64(i)*1.8
		in := 4700 + 950*math.Sin(phase) + 500*math.Sin(t/5+float64(i)) + 170*math.Sin(t/1.9)
		out := in*.82 + 300*math.Cos(phase/2)
		connections := 810 + int(26*math.Sin(phase/4)) + i*45
		p := point{Time: s.Timestamp, Incoming: in, Outgoing: out, Bandwidth: (in + out) * 420, BytesIn: in * 420, BytesOut: out * 420, Connections: connections}
		n := node{ID: id, Address: fmt.Sprintf("10.0.1.%d:8082", 11+i), Leader: i == 0, Healthy: true, Uptime: 1209600 + math.Mod(t, 86400), Connections: connections, Subscriptions: 1350 + i*123, Received: 82400000 + float64(i)*1100000, Sent: 71500000 + float64(i)*1900000, BytesIn: 34e9, BytesOut: 29e9, Incoming: in, Outgoing: out}
		n.ClusterMode = valuePtr(true)
		n.Version = "demo"
		n.RetainedMessages = 120 + i*10
		n.TransportAddress = fmt.Sprintf("10.0.1.%d:7946", 11+i)
		s.Nodes = append(s.Nodes, n)
		s.Points[id] = p
		all.Incoming += in
		all.Outgoing += out
		all.Bandwidth += p.Bandwidth
		all.BytesIn += p.BytesIn
		all.BytesOut += p.BytesOut
		all.Connections += connections
		for j := 0; j < 12; j++ {
			protocol := []string{"mqtt5", "mqtt5", "mqtt3.1.1", "amqp0.9.1"}[j%4]
			connected := j != 10
			client := fmt.Sprintf("%s-%03d", []string{"edge-gateway", "factory-sensor", "fleet-tracker", "order-service", "energy-meter", "warehouse-hub"}[j%6], i*12+j+1)
			item := session{ClientID: client, NodeID: id, Protocol: protocol, Connected: connected, SubscriptionCount: 2, InflightCount: (j + i) % 4, ConnectedAt: at.Add(-time.Duration(j+3) * time.Hour).UTC().Format(time.RFC3339), Subscriptions: []sessionSubscription{{Filter: demoTopics[j%8], QoS: 1}, {Filter: "system/health", QoS: 0}}}
			item.State = "connected"
			if strings.HasPrefix(protocol, "mqtt") {
				item.Version = 5
				if protocol == "mqtt3.1.1" {
					item.Version = 4
				}
				item.CleanStart = valuePtr(false)
				item.ExpiryInterval = valuePtr(uint32(86400))
				item.ReceiveMaximum = valuePtr(uint16(64))
				item.MaxPacketSize = valuePtr(uint32(1048576))
				item.TopicAliasMax = valuePtr(uint16(16))
				item.HasWill = valuePtr(true)
				item.RequestResponse = valuePtr(false)
				item.RequestProblem = valuePtr(true)
				for k := range item.Subscriptions {
					item.Subscriptions[k].NoLocal = valuePtr(false)
					item.Subscriptions[k].RetainAsPublished = valuePtr(false)
					item.Subscriptions[k].RetainHandling = valuePtr(uint8(0))
				}
			} else {
				item.ConnectionName = "Orders worker " + client
			}
			if !connected {
				item.State = "disconnected"
				item.DisconnectedAt = at.Add(-time.Hour).UTC().Format(time.RFC3339)
				item.OfflineQueueDepth = 18
			}
			s.Sessions = append(s.Sessions, item)
		}
		for _, topic := range demoTopics {
			count, qos := 0, 0
			for _, client := range s.Sessions {
				if client.NodeID != id || !client.Connected {
					continue
				}
				found := false
				for _, sub := range client.Subscriptions {
					if sub.Filter == topic {
						found = true
						if sub.QoS > qos {
							qos = sub.QoS
						}
					}
				}
				if found {
					count++
				}
			}
			if count > 0 {
				s.Subscriptions = append(s.Subscriptions, subscription{Filter: topic, Subscribers: count, QoS: qos, NodeID: id})
			}
		}
	}
	s.Points["all"] = all
	for i := range s.Nodes {
		n := &s.Nodes[i]
		n.QueueMetrics, _ = json.Marshal(map[string]any{
			"capture_failures": 0, "capture_dropped": 0,
			"claims":          map[string]int{"attempts": 540, "successes": 540, "failures": 0},
			"steals":          map[string]int{"attempts": 12, "successes": 12, "failures": 0},
			"acknowledgments": map[string]int{"ack": 82410 + i*2140, "nack": 42, "reject": 7, "dlq": 7, "dlq_transfer_failures": 0, "poison_without_dlq": 0, "poison_pending": 0, "poison_pending_no_destination": 0},
			"pending":         map[string]int{"current": 45, "high_water": 92},
		})
		n.ProtocolMetrics, _ = json.Marshal(map[string]any{
			"mqtt": map[string]any{"connections": map[string]int{"current": n.Connections - 80, "total": 12000, "disconnections": 11000}, "messages": map[string]float64{"received": n.Received * .8, "sent": n.Sent * .8, "publish_received": n.Received * .7, "publish_sent": n.Sent * .7}, "bytes": map[string]float64{"received": n.BytesIn * .8, "sent": n.BytesOut * .8}, "errors": map[string]int{"protocol": 0, "auth": 0, "authz": 0, "packet": 0}},
			"amqp": map[string]any{"connections": map[string]int{"current": 80, "total": 200, "disconnections": 120}, "channels": 96, "consumers": 72, "messages": map[string]float64{"received": n.Received * .2, "sent": n.Sent * .2}, "bytes": map[string]float64{"received": n.BytesIn * .2, "sent": n.BytesOut * .2}, "errors": map[string]int{"protocol": 0}},
		})
	}
	return s
}
func (c *collector) run(ctx context.Context) {
	tick := 0
	for {
		if c.demo {
			c.publish(demoSnapshot(time.Now()))
		} else {
			c.publish(c.collect(ctx, tick%5 == 0))
		}
		tick++
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}
func (c *collector) detail(w http.ResponseWriter, r *http.Request) {
	id, nodeID := r.URL.Query().Get("id"), r.URL.Query().Get("node")
	c.mu.RLock()
	if c.demo {
		for _, s := range c.current.Sessions {
			if s.ClientID == id && s.NodeID == nodeID {
				c.mu.RUnlock()
				writeJSON(w, s)
				return
			}
		}
		c.mu.RUnlock()
		http.Error(w, "session not found", 404)
		return
	}
	base, ok := c.nodeURLs[nodeID]
	c.mu.RUnlock()
	if !ok || id == "" {
		http.Error(w, "unknown session or node", 400)
		return
	}
	var s session
	if err := c.get(r.Context(), base+"/api/v1/sessions/"+url.PathEscape(id), &s); err != nil {
		http.Error(w, "could not load session details", 502)
		return
	}
	s.NodeID = nodeID
	writeJSON(w, s)
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}
func handler(c *collector) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /console-api/snapshot", c.snapshotHandler)
	mux.HandleFunc("GET /console-api/stream", c.stream)
	mux.HandleFunc("GET /console-api/session", c.detail)
	mux.HandleFunc("GET /console-api/inventory", c.inventoryPage)
	mux.HandleFunc("GET /console-api/queues", c.queueRead)
	mux.HandleFunc("GET /console-api/subscription-clients", c.subscriptionClients)
	mux.HandleFunc("GET /console-api/", func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })
	dist, _ := fs.Sub(assets, "dist")
	files := http.FileServer(http.FS(dist))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			path = "index.html"
		}
		if _, err := fs.Stat(dist, path); err != nil {
			if strings.Contains(path, ".") {
				http.NotFound(w, r)
				return
			}
			content, _ := fs.ReadFile(dist, "index.html")
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(content)
			return
		}
		if strings.HasPrefix(path, "assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		files.ServeHTTP(w, r)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("X-Frame-Options", "DENY")
		mux.ServeHTTP(w, r)
	})
}
func main() {
	listen := flag.String("listen", "127.0.0.1:3002", "HTTP listen address")
	demo := flag.Bool("demo", false, "explicitly enable simulated broker data")
	flag.Parse()
	raw := os.Getenv("FLUXMQ_NODE_URLS")
	if raw == "" {
		raw = os.Getenv("FLUXMQ_API_URL")
	}
	urls := []string{}
	for _, v := range strings.Split(raw, ",") {
		if v = strings.TrimRight(strings.TrimSpace(v), "/"); v != "" {
			parsed, err := url.Parse(v)
			if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
				log.Fatal("broker URLs must be HTTP(S) origins without credentials, queries, or fragments")
			}
			urls = append(urls, v)
		}
	}
	if !*demo && len(urls) == 0 {
		log.Fatal("set FLUXMQ_API_URL/FLUXMQ_NODE_URLS, or use -demo for a sample console")
	}
	c := newCollector(urls, *demo)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *demo {
		now := time.Now()
		for i := 449; i >= 0; i-- {
			c.publish(demoSnapshot(now.Add(-time.Duration(i) * 2 * time.Second)))
		}
	} else {
		c.publish(c.collect(ctx, true))
	}
	go c.run(ctx)
	server := &http.Server{Addr: *listen, Handler: handler(c), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	log.Printf("FluxMQ Console at http://%s (demo=%t)", *listen, *demo)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
