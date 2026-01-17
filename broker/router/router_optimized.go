// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"strings"
	"sync"

	"github.com/absmach/fluxmq/storage"
)

// OptimizedRouter uses per-node RWMutex instead of a global lock.
// This allows concurrent reads on different parts of the trie and
// reduces lock contention significantly compared to the global mutex approach.
//
// Design:
// - Match operations lock only the nodes they traverse (RLock)
// - Subscribe/Unsubscribe lock only the affected path (Lock on final node)
// - Different branches can be accessed concurrently
// - Much better performance than both global mutex and pure lock-free CAS.
type OptimizedRouter struct {
	root *optimizedNode
}

// optimizedNode represents a node in the optimized trie.
// Each node has its own RWMutex for fine-grained locking.
type optimizedNode struct {
	mu       sync.RWMutex
	children map[string]*optimizedNode
	subs     []*storage.Subscription
}

// NewOptimizedRouter creates a new optimized router.
func NewOptimizedRouter() *OptimizedRouter {
	return &OptimizedRouter{
		root: &optimizedNode{
			children: make(map[string]*optimizedNode),
		},
	}
}

// Match returns all subscriptions that match the given topic.
// Uses RLocks on nodes as it traverses, allowing concurrent reads.
func (r *OptimizedRouter) Match(topic string) ([]*storage.Subscription, error) {
	levels := strings.Split(topic, separator)
	var matched []*storage.Subscription
	optimizedMatchLevel(r.root, levels, 0, &matched)
	return matched, nil
}

// optimizedMatchLevel recursively matches topic levels against the trie.
// Locks each node briefly to read children, then recurses without holding locks.
func optimizedMatchLevel(n *optimizedNode, levels []string, index int, matched *[]*storage.Subscription) {
	if n == nil {
		return
	}

	n.mu.RLock()

	if index == len(levels) {
		// Reached end of topic - include exact matches
		*matched = append(*matched, n.subs...)

		// Include # wildcards at this level
		if hashNode, ok := n.children["#"]; ok {
			n.mu.RUnlock()
			hashNode.mu.RLock()
			*matched = append(*matched, hashNode.subs...)
			hashNode.mu.RUnlock()
			return
		}
		n.mu.RUnlock()
		return
	}

	level := levels[index]

	// Get references to children while holding lock
	exactChild := n.children[level]
	plusChild := n.children["+"]
	hashChild := n.children["#"]

	n.mu.RUnlock()

	// Recursively match children (without holding parent lock)
	if exactChild != nil {
		optimizedMatchLevel(exactChild, levels, index+1, matched)
	}

	if plusChild != nil {
		optimizedMatchLevel(plusChild, levels, index+1, matched)
	}

	if hashChild != nil {
		hashChild.mu.RLock()
		*matched = append(*matched, hashChild.subs...)
		hashChild.mu.RUnlock()
	}
}

// Subscribe adds a subscription to the router.
// Locks only the nodes being modified.
func (r *OptimizedRouter) Subscribe(clientID string, filter string, qos byte, opts storage.SubscribeOptions) error {
	sub := &storage.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      qos,
		Options:  opts,
	}

	levels := strings.Split(filter, separator)
	n := r.root

	// Navigate to target node, creating path if needed
	for _, level := range levels {
		n.mu.Lock()

		child, ok := n.children[level]
		if !ok {
			// Create new child node
			child = &optimizedNode{
				children: make(map[string]*optimizedNode),
			}
			n.children[level] = child
		}

		n.mu.Unlock()
		n = child
	}

	// Lock final node and add subscription
	n.mu.Lock()
	n.subs = append(n.subs, sub)
	n.mu.Unlock()

	return nil
}

// Unsubscribe removes a subscription from the router.
// Locks only the nodes being modified.
func (r *OptimizedRouter) Unsubscribe(clientID string, filter string) error {
	levels := strings.Split(filter, separator)
	n := r.root

	// Navigate to target node
	for _, level := range levels {
		n.mu.RLock()
		child, ok := n.children[level]
		n.mu.RUnlock()

		if !ok {
			// Path doesn't exist
			return nil
		}
		n = child
	}

	// Lock final node and remove subscription
	n.mu.Lock()
	defer n.mu.Unlock()

	filtered := n.subs[:0]
	for _, sub := range n.subs {
		if sub.ClientID != clientID {
			filtered = append(filtered, sub)
		}
	}
	n.subs = filtered

	return nil
}
