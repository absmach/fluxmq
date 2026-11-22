package main

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
)

type subscriptionClient struct {
	ClientID string `json:"client_id"`
	NodeID   string `json:"node_id"`
	QoS      int    `json:"qos"`
}
type subscriptionClientsPage struct {
	Clients     []subscriptionClient `json:"clients"`
	Next        map[string]string    `json:"next"`
	FailedNodes []string             `json:"failed_nodes"`
}

// Each request fetches a bounded page from each configured target. Continuation
// tokens are opaque broker tokens; they never control the upstream URL.
func (c *collector) subscriptionClients(w http.ResponseWriter, r *http.Request) {
	filter, nodeID := r.URL.Query().Get("filter"), r.URL.Query().Get("node")
	if filter == "" {
		http.Error(w, "filter is required", 400)
		return
	}
	cursors := map[string]string{}
	more := r.URL.Query().Has("cursor")
	if more && (len(r.URL.Query().Get("cursor")) > 65536 || json.Unmarshal([]byte(r.URL.Query().Get("cursor")), &cursors) != nil || len(cursors) == 0) {
		http.Error(w, "invalid continuation", 400)
		return
	}
	c.mu.RLock()
	targets := map[string]string{}
	for id, base := range c.nodeURLs {
		if nodeID == "" || id == nodeID {
			targets[id] = base
		}
	}
	current := c.current
	c.mu.RUnlock()
	page := subscriptionClientsPage{Clients: []subscriptionClient{}, Next: map[string]string{}, FailedNodes: []string{}}
	if c.demo {
		if nodeID != "" {
			found := false
			for _, n := range current.Nodes {
				if n.ID == nodeID {
					found = true
				}
			}
			if !found {
				http.Error(w, "unknown node", 400)
				return
			}
		}
		for _, client := range current.Sessions {
			if !client.Connected || (nodeID != "" && client.NodeID != nodeID) {
				continue
			}
			found, qos := false, 0
			for _, sub := range client.Subscriptions {
				if sub.Filter == filter {
					found = true
					if sub.QoS > qos {
						qos = sub.QoS
					}
				}
			}
			if found {
				page.Clients = append(page.Clients, subscriptionClient{ClientID: client.ClientID, NodeID: client.NodeID, QoS: qos})
			}
		}
	} else {
		if len(targets) == 0 {
			http.Error(w, "unknown node or no configured brokers", 400)
			return
		}
		for id := range cursors {
			if _, ok := targets[id]; !ok {
				http.Error(w, "unknown continuation node", 400)
				return
			}
		}
		type result struct {
			id, next string
			clients  []subscriptionClient
			err      error
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
				query := url.Values{"state": {"connected"}, "limit": {"200"}}
				if token != "" {
					query.Set("page_token", token)
				}
				var raw struct {
					Clients []subscriptionClient `json:"clients"`
					Next    string               `json:"next_page_token"`
				}
				err := c.get(r.Context(), base+"/api/v1/subscriptions/"+url.PathEscape(filter)+"/clients?"+query.Encode(), &raw)
				for i := range raw.Clients {
					raw.Clients[i].NodeID = id
				}
				results <- result{id: id, next: raw.Next, clients: raw.Clients, err: err}
			}(id, base, token)
		}
		for range requested {
			got := <-results
			if got.err != nil {
				page.FailedNodes = append(page.FailedNodes, got.id)
				page.Next[got.id] = cursors[got.id]
				continue
			}
			page.Clients = append(page.Clients, got.clients...)
			if got.next != "" {
				page.Next[got.id] = got.next
			}
		}
		// Keep a failed target retryable even when other targets succeeded.
		sort.Strings(page.FailedNodes)
	}
	sort.Slice(page.Clients, func(i, j int) bool {
		a, b := page.Clients[i], page.Clients[j]
		if a.ClientID == b.ClientID {
			return a.NodeID < b.NodeID
		}
		return a.ClientID < b.ClientID
	})
	writeJSON(w, page)
}
