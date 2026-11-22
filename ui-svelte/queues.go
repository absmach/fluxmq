// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Only these read-only RPCs are reachable through the console. ReadBatch reads
// the log directly; it never calls Consume, advances a group, or acknowledges.
func (c *collector) queueRead(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	id, action, name := q.Get("node"), q.Get("action"), q.Get("queue")
	c.mu.RLock()
	base, ok := c.nodeURLs[id]
	current := c.current
	c.mu.RUnlock()
	if c.demo {
		for _, n := range current.Nodes {
			if n.ID == id {
				ok = true
			}
		}
	}
	if !ok {
		http.Error(w, "unknown or unmonitored node", 400)
		return
	}
	args := map[string]any{}
	method := ""
	switch action {
	case "list":
		method = "ListQueues"
		args["limit"] = 100
		args["pageToken"] = q.Get("cursor")
	case "info":
		method = "GetQueueInfo"
		args["queueName"] = name
	case "config":
		method = "GetQueue"
		args["name"] = name
	case "groups":
		method = "ListConsumerGroups"
		args["queueName"] = name
	case "pending":
		if q.Get("group") == "" {
			http.Error(w, "group required", 400)
			return
		}
		method = "GetPending"
		args["queueName"] = name
		args["groupId"] = q.Get("group")
	case "messages":
		offset, err := strconv.ParseUint(q.Get("offset"), 10, 64)
		if err != nil {
			http.Error(w, "invalid offset", 400)
			return
		}
		method = "ReadBatch"
		args["queueName"] = name
		args["startOffset"] = strconv.FormatUint(offset, 10)
		args["limit"] = 20
	default:
		http.Error(w, "unknown read operation", 400)
		return
	}
	if action != "list" && name == "" {
		http.Error(w, "queue required", 400)
		return
	}
	if c.demo {
		data, valid := demoQueueRead(action, name, q.Get("group"), q.Get("offset"))
		if !valid {
			http.Error(w, "queue or group not found", 404)
			return
		}
		writeJSON(w, data)
		return
	}
	data, err := c.queueRPC(r.Context(), base, method, args)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(data)
}
func (c *collector) queueRPC(ctx context.Context, base, method string, args map[string]any) ([]byte, error) {
	body, _ := json.Marshal(args)
	req, err := http.NewRequestWithContext(ctx, "POST", base+"/fluxmq.queue.v1.QueueService/"+method, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("queue API could not be reached")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("queue API returned HTTP %d; the service may be disabled or the queue unavailable", resp.StatusCode)
	}
	// Broker ReadBatch currently ignores max_bytes. Bound the response here and
	// fail explicitly rather than returning a partial JSON document or fake data.
	const max = 8 << 20
	data, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return nil, fmt.Errorf("queue response interrupted")
	}
	if len(data) > max {
		return nil, fmt.Errorf("queue response exceeds 8 MiB; this batch cannot be previewed")
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("invalid queue response")
	}
	return data, nil
}
func demoQueueRead(action, name, group, offset string) (any, bool) {
	names := []string{"device-events", "telemetry-archive", "command-delivery"}
	configs := make([]map[string]any, 0, len(names))
	index := -1
	for i, n := range names {
		if name == n {
			index = i
		}
		configs = append(configs, map[string]any{"name": n, "type": []string{"classic", "stream", "classic"}[i], "topics": []string{"devices/" + []string{"+/events", "+/telemetry", "+/commands"}[i]}, "config": map[string]any{"retention": map[string]any{"maxAge": "86400s", "maxBytes": "1073741824"}, "maxMessageSize": 1048576}})
	}
	if action == "list" {
		return map[string]any{"queues": configs}, true
	}
	if index < 0 {
		return nil, false
	}
	head := uint64(12000 + index*1000)
	tail := head + uint64([]int{1240, 8420, 96}[index])
	switch action {
	case "config":
		return configs[index], true
	case "info":
		return map[string]any{"queueName": name, "headOffset": strconv.FormatUint(head, 10), "tailOffset": strconv.FormatUint(tail, 10), "messageCount": strconv.FormatUint(tail-head, 10)}, true
	case "groups":
		groups := []any{}
		for i, g := range []string{"event-processor", "archive-writer"} {
			pending := []int{12, 3}[i]
			lag := []uint64{240, 0}[i]
			if tail-head < lag {
				lag = 24
			}
			cursor := tail - lag
			groups = append(groups, map[string]any{"groupId": g, "queueName": name, "pendingCount": strconv.Itoa(pending), "cursor": map[string]any{"cursor": strconv.FormatUint(cursor, 10), "committed": strconv.FormatUint(cursor-uint64(pending), 10)}, "consumers": []any{map[string]any{"consumerId": g + "-01", "lastHeartbeat": time.Now().Add(-3 * time.Second).UTC().Format(time.RFC3339)}}})
		}
		return map[string]any{"groups": groups}, true
	case "pending":
		if group != "event-processor" && group != "archive-writer" {
			return nil, false
		}
		pending := 12
		lag := uint64(240)
		if tail-head < lag {
			lag = 24
		}
		if group == "archive-writer" {
			pending = 3
			lag = 0
		}
		entries := []any{}
		for i := 0; i < pending; i++ {
			entries = append(entries, map[string]any{"offset": strconv.FormatUint(tail-lag-uint64(pending)+uint64(i), 10), "consumerId": group + "-01", "deliveredAt": time.Now().Add(-time.Duration(20+i) * time.Second).UTC().Format(time.RFC3339), "deliveryCount": 1 + i%3})
		}
		return map[string]any{"entries": entries}, true
	case "messages":
		start, _ := strconv.ParseUint(offset, 10, 64)
		if start < head {
			start = head
		}
		messages := []any{}
		for i := start; i < tail && i-start < 20; i++ {
			payload := fmt.Sprintf(`{"device":"sensor-%03d","temperature":22.4,"sequence":%d}`, i%64, i)
			messages = append(messages, map[string]any{"sourceProtocol": []string{"mqtt", "amqp", "http"}[i%3], "offset": strconv.FormatUint(i, 10), "timestamp": time.Now().Add(-time.Duration(tail-i) * time.Second).UTC().Format(time.RFC3339), "value": base64.StdEncoding.EncodeToString([]byte(payload)), "headers": map[string]string{"content-type": base64.StdEncoding.EncodeToString([]byte("application/json"))}})
		}
		return map[string]any{"messages": messages}, true
	}
	return nil, false
}
