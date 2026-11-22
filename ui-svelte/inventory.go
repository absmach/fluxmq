// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

type inventoryPage struct {
	Sessions      []session         `json:"sessions"`
	Subscriptions []subscription    `json:"subscriptions"`
	Next          map[string]string `json:"next"`
	FailedNodes   []string          `json:"failed_nodes"`
}

// Fetch one bounded page per configured node. Filtering takes place before the
// response is returned; continuation remains available even for empty matches.
func (c *collector) inventoryPage(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	kind, nodeID, state := query.Get("kind"), query.Get("node"), query.Get("state")
	if (kind != "sessions" && kind != "subscriptions") || (state != "" && state != "all" && state != "connected" && state != "disconnected") {
		http.Error(w, "invalid inventory kind or state", http.StatusBadRequest)
		return
	}
	cursors := map[string]string{}
	more := query.Has("cursor")
	if more && (len(query.Get("cursor")) > 65536 || json.Unmarshal([]byte(query.Get("cursor")), &cursors) != nil || len(cursors) == 0) {
		http.Error(w, "invalid continuation", http.StatusBadRequest)
		return
	}
	c.mu.RLock()
	targets := map[string]string{}
	for id, base := range c.nodeURLs {
		if nodeID == "" || nodeID == id {
			targets[id] = base
		}
	}
	current := c.current
	c.mu.RUnlock()
	if c.demo {
		for _, n := range current.Nodes {
			if nodeID == "" || nodeID == n.ID {
				targets[n.ID] = ""
			}
		}
	}
	if len(targets) == 0 {
		http.Error(w, "unknown node or no configured brokers", http.StatusBadRequest)
		return
	}
	for id := range cursors {
		if _, ok := targets[id]; !ok {
			http.Error(w, "unknown continuation node", http.StatusBadRequest)
			return
		}
	}
	page := inventoryPage{Sessions: []session{}, Subscriptions: []subscription{}, Next: map[string]string{}, FailedNodes: []string{}}
	type result struct {
		id, next      string
		sessions      []session
		subscriptions []subscription
		failed        bool
	}
	results := make(chan result, len(targets))
	requested := 0
	for id, base := range targets {
		token, present := cursors[id]
		if more && !present {
			continue
		}
		requested++
		go func(id, base, token string) {
			out := result{id: id}
			if c.demo {
				for _, s := range current.Sessions {
					if s.NodeID == id && (state == "" || state == "all" || s.Connected == (state == "connected")) {
						out.sessions = append(out.sessions, s)
					}
				}
				for _, s := range current.Subscriptions {
					if s.NodeID == id {
						out.subscriptions = append(out.subscriptions, s)
					}
				}
			} else {
				params := url.Values{"limit": {"200"}}
				if state != "" {
					params.Set("state", state)
				}
				if token != "" {
					params.Set("page_token", token)
				}
				var raw struct {
					Sessions      []session      `json:"sessions"`
					Subscriptions []subscription `json:"subscriptions"`
					Next          string         `json:"next_page_token"`
				}
				err := c.get(r.Context(), base+"/api/v1/"+kind+"?"+params.Encode(), &raw)
				out.failed = err != nil || (raw.Next != "" && raw.Next == token)
				out.sessions, out.subscriptions, out.next = raw.Sessions, raw.Subscriptions, raw.Next
			}
			results <- out
		}(id, base, token)
	}
	term, protocol := strings.ToLower(query.Get("q")), query.Get("protocol")
	for range requested {
		out := <-results
		if out.failed {
			page.FailedNodes = append(page.FailedNodes, out.id)
			page.Next[out.id] = cursors[out.id]
			continue
		}
		if out.next != "" {
			page.Next[out.id] = out.next
		}
		if kind == "sessions" {
			for _, s := range out.sessions {
				s.NodeID = out.id
				s.Protocol = normalizedProtocol(s)
				if (protocol == "" || protocol == "all" || s.Protocol == protocol) && strings.Contains(strings.ToLower(s.ClientID+" "+s.ConnectionName+" "+s.NodeID), term) {
					page.Sessions = append(page.Sessions, s)
				}
			}
		} else {
			for _, s := range out.subscriptions {
				s.NodeID = out.id
				if strings.Contains(strings.ToLower(s.Filter), term) {
					page.Subscriptions = append(page.Subscriptions, s)
				}
			}
		}
	}
	sort.Slice(page.Sessions, func(i, j int) bool {
		a, b := page.Sessions[i], page.Sessions[j]
		if a.ClientID == b.ClientID {
			return a.NodeID < b.NodeID
		}
		return a.ClientID < b.ClientID
	})
	sort.Slice(page.Subscriptions, func(i, j int) bool {
		a, b := page.Subscriptions[i], page.Subscriptions[j]
		if a.Filter == b.Filter {
			return a.NodeID < b.NodeID
		}
		return a.Filter < b.Filter
	})
	sort.Strings(page.FailedNodes)
	writeJSON(w, page)
}

func normalizedProtocol(s session) string {
	raw := strings.ToLower(strings.TrimSpace(s.Protocol))
	switch {
	case s.Version == 5 || raw == "mqtt5" || raw == "mqtt 5" || raw == "mqtt 5.0":
		return "mqtt5"
	case s.Version == 4 || raw == "mqtt3.1.1" || raw == "mqtt 3.1.1" || raw == "mqtt311":
		return "mqtt3.1.1"
	case s.Version == 3 || raw == "mqtt3.1" || raw == "mqtt 3.1" || raw == "mqtt31":
		return "mqtt3.1"
	case raw == "amqp" || raw == "amqp0.9.1" || raw == "amqp 0.9.1" || raw == "amqp091" || raw == "amqp_0_9_1" || strings.HasPrefix(s.ClientID, "amqp091:"):
		return "amqp0.9.1"
	case raw == "amqp1" || raw == "amqp1.0" || raw == "amqp 1.0" || raw == "amqp10" || raw == "amqp_1_0" || strings.HasPrefix(s.ClientID, "amqp:"):
		return "amqp1.0"
	case raw == "":
		return "unknown"
	default:
		return raw
	}
}
