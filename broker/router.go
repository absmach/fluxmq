package broker

import (
	"strings"
	"sync"
)

// Subscription represents a single client subscription.
type Subscription struct {
	SessionID string
	QoS       byte
}

// Router handles topic matching and subscription storage.
type Router struct {
	mu   sync.RWMutex
	root *node
}

type node struct {
	children map[string]*node
	subs     []Subscription // Subscriptions at this exact level
}

// NewRouter returns a new instance.
func NewRouter() *Router {
	return &Router{
		root: newNode(),
	}
}

func newNode() *node {
	return &node{
		children: make(map[string]*node),
	}
}

// Subscribe adds a subscription to the topic filter.
func (r *Router) Subscribe(filter string, sub Subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()

	levels := strings.Split(filter, "/")
	n := r.root
	for _, level := range levels {
		child, ok := n.children[level]
		if !ok {
			child = newNode()
			n.children[level] = child
		}
		n = child
	}
	n.subs = append(n.subs, sub)
}

// Unsubscribe removes a subscription from the topic filter for a specific session.
func (r *Router) Unsubscribe(filter string, sessionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	levels := strings.Split(filter, "/")
	n := r.root
	for _, level := range levels {
		child, ok := n.children[level]
		if !ok {
			// Filter not found, nothing to unsubscribe
			return
		}
		n = child
	}

	// Remove subscription for this session from the node
	filtered := n.subs[:0]
	for _, sub := range n.subs {
		if sub.SessionID != sessionID {
			filtered = append(filtered, sub)
		}
	}
	n.subs = filtered
}

// Match returns all subscriptions that match the topic name.
func (r *Router) Match(topic string) []Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()

	levels := strings.Split(topic, "/")
	var matched []Subscription
	matchLevel(r.root, levels, 0, &matched)
	return matched
}

func matchLevel(n *node, levels []string, index int, matched *[]Subscription) {
	if index == len(levels) {
		// Reached end of topic - include exact matches and # wildcards
		*matched = append(*matched, n.subs...)
		if wild, ok := n.children["#"]; ok {
			*matched = append(*matched, wild.subs...)
		}
		return
	}

	level := levels[index]

	// Check exact match
	if child, ok := n.children[level]; ok {
		matchLevel(child, levels, index+1, matched)
	}

	// Check single-level wildcard '+'
	if child, ok := n.children["+"]; ok {
		matchLevel(child, levels, index+1, matched)
	}

	// Check multi-level wildcard '#'
	if child, ok := n.children["#"]; ok {
		*matched = append(*matched, child.subs...)
	}
}
