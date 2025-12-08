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
	// 1. Check exact match
	if index == len(levels) {
		*matched = append(*matched, n.subs...)
		// Also check for '#' wildcard at this level (e.g. topic "a/b", filter "a/#")
		// But '#' is a child of 'a'. The node 'a' is where we are.
		// If 'a' has child '#', it matches "a/b"??
		// No, '#' matches parent and children.
		// If filter is "a/#", we have root->a->#
		// When processing "a/b", we consume "a", reach node 'a'.
		// We see child '#'. It matches.
		if wild, ok := n.children["#"]; ok {
			*matched = append(*matched, wild.subs...)
		}
		return
	}

	level := levels[index]

	// 2. Exact match traversal
	if child, ok := n.children[level]; ok {
		matchLevel(child, levels, index+1, matched)
	}

	// 3. Single level wildcard '+'
	if child, ok := n.children["+"]; ok {
		matchLevel(child, levels, index+1, matched)
	}

	// 4. Multi-level wildcard '#'
	if child, ok := n.children["#"]; ok {
		*matched = append(*matched, child.subs...)
	}
}
