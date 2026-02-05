// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"strings"
	"sync"

	"github.com/absmach/fluxmq/storage"
)

const separator = "/" // TrieRouter handles topic matching and subscription storage.
type TrieRouter struct {
	mu   sync.RWMutex
	root *node
}

type node struct {
	children map[string]*node
	subs     []*storage.Subscription // Subscriptions at this exact level
}

// NewRouter returns a new instance.
func NewRouter() *TrieRouter {
	return &TrieRouter{
		root: newNode(),
	}
}

func newNode() *node {
	return &node{
		children: make(map[string]*node),
	}
}

// Subscribe adds a subscription to the topic filter.
func (r *TrieRouter) Subscribe(clientID string, filter string, qos byte, opts storage.SubscribeOptions) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	sub := &storage.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      qos,
		Options:  opts,
	}

	levels := strings.Split(filter, separator)
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
	return nil
}

// Unsubscribe removes a subscription from the topic filter for a specific session.
func (r *TrieRouter) Unsubscribe(clientID string, filter string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	levels := strings.Split(filter, separator)
	n := r.root
	for _, level := range levels {
		child, ok := n.children[level]
		if !ok {
			return nil
		}
		n = child
	}

	filtered := n.subs[:0]
	for _, sub := range n.subs {
		if sub.ClientID != clientID {
			filtered = append(filtered, sub)
		}
	}
	n.subs = filtered
	return nil
}

// Match returns all subscriptions that match the topic name.
func (r *TrieRouter) Match(topic string) ([]*storage.Subscription, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	levels := strings.Split(topic, separator)
	matched := AcquireSubscriptionSlice()
	matchLevel(r.root, levels, 0, matched)
	// Copy out before releasing the pooled slice to avoid data races
	// when the pool reuses the backing array in other goroutines.
	result := append([]*storage.Subscription(nil), (*matched)...)

	// Release the pooled slice pointer back to pool
	ReleaseSubscriptionSlice(matched)

	return result, nil
}

func matchLevel(n *node, levels []string, index int, matched *[]*storage.Subscription) {
	if index == len(levels) {
		// Reached end of topic - include exact matches and # wildcards
		*matched = append(*matched, n.subs...)
		if wild, ok := n.children["#"]; ok {
			*matched = append(*matched, wild.subs...)
		}
		return
	}

	level := levels[index]

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
