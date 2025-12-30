// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"strings"
	"sync/atomic"

	"github.com/absmach/mqtt/storage"
)

// LockFreeRouter implements a lock-free topic router using atomic operations
// and copy-on-write semantics. This provides significantly better performance
// for read-heavy workloads (topic matching) which is the common case in MQTT.
//
// Design:
// - Match operations are lock-free (only atomic loads)
// - Subscribe/Unsubscribe use copy-on-write with CAS
// - All data structures (maps, slices) are immutable
// - Contention on writes is handled by CAS retry loop
type LockFreeRouter struct {
	root atomic.Pointer[lockFreeNode]
}

// lockFreeNode represents a node in the lock-free trie.
// All fields are atomic pointers to immutable data structures.
type lockFreeNode struct {
	// children points to an immutable map of child nodes
	// The map itself is never modified; updates create a new map
	children atomic.Pointer[map[string]*lockFreeNode]

	// subs points to an immutable slice of subscriptions
	// The slice itself is never modified; updates create a new slice
	subs atomic.Pointer[[]*storage.Subscription]
}

// NewLockFreeRouter creates a new lock-free router.
func NewLockFreeRouter() *LockFreeRouter {
	r := &LockFreeRouter{}
	// Initialize with an empty root node
	emptyNode := &lockFreeNode{}
	emptyChildren := make(map[string]*lockFreeNode)
	emptySubs := make([]*storage.Subscription, 0)
	emptyNode.children.Store(&emptyChildren)
	emptyNode.subs.Store(&emptySubs)
	r.root.Store(emptyNode)
	return r
}

// Match returns all subscriptions that match the given topic.
// This operation is lock-free and only performs atomic loads.
func (r *LockFreeRouter) Match(topic string) ([]*storage.Subscription, error) {
	root := r.root.Load()
	if root == nil {
		return nil, nil
	}

	levels := strings.Split(topic, separator)
	var matched []*storage.Subscription
	lockFreeMatchLevel(root, levels, 0, &matched)
	return matched, nil
}

// lockFreeMatchLevel recursively matches topic levels against the trie.
// This is identical to the original matchLevel but operates on lockFreeNode.
func lockFreeMatchLevel(n *lockFreeNode, levels []string, index int, matched *[]*storage.Subscription) {
	if n == nil {
		return
	}

	if index == len(levels) {
		// Reached end of topic - include exact matches
		subs := n.subs.Load()
		if subs != nil {
			*matched = append(*matched, *subs...)
		}

		// Include # wildcards at this level
		children := n.children.Load()
		if children != nil {
			if hashNode, ok := (*children)["#"]; ok {
				hashSubs := hashNode.subs.Load()
				if hashSubs != nil {
					*matched = append(*matched, *hashSubs...)
				}
			}
		}
		return
	}

	level := levels[index]
	children := n.children.Load()
	if children == nil {
		return
	}

	// Exact match
	if child, ok := (*children)[level]; ok {
		lockFreeMatchLevel(child, levels, index+1, matched)
	}

	// Single-level wildcard '+'
	if child, ok := (*children)["+"]; ok {
		lockFreeMatchLevel(child, levels, index+1, matched)
	}

	// Multi-level wildcard '#'
	if child, ok := (*children)["#"]; ok {
		subs := child.subs.Load()
		if subs != nil {
			*matched = append(*matched, *subs...)
		}
	}
}

// Subscribe adds a subscription to the router.
// Uses copy-on-write with CAS to ensure atomicity without locks.
func (r *LockFreeRouter) Subscribe(clientID string, filter string, qos byte, opts storage.SubscribeOptions) error {
	sub := &storage.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      qos,
		Options:  opts,
	}

	levels := strings.Split(filter, separator)

	// CAS retry loop
	for {
		oldRoot := r.root.Load()
		newRoot := r.copyAndAddSubscription(oldRoot, levels, sub)

		if r.root.CompareAndSwap(oldRoot, newRoot) {
			// Successfully installed new root
			return nil
		}
		// CAS failed, retry with new root
	}
}

// copyAndAddSubscription creates a new path through the trie with the subscription added.
// Returns a new root node with the subscription included.
func (r *LockFreeRouter) copyAndAddSubscription(n *lockFreeNode, levels []string, sub *storage.Subscription) *lockFreeNode {
	if n == nil {
		// Create new node
		n = &lockFreeNode{}
		emptyChildren := make(map[string]*lockFreeNode)
		emptySubs := make([]*storage.Subscription, 0)
		n.children.Store(&emptyChildren)
		n.subs.Store(&emptySubs)
	}

	// Create new node (copy)
	newNode := &lockFreeNode{}

	if len(levels) == 0 {
		// At target level - add subscription
		oldSubs := n.subs.Load()
		var newSubs []*storage.Subscription
		if oldSubs != nil && len(*oldSubs) > 0 {
			// Copy existing subscriptions
			newSubs = make([]*storage.Subscription, len(*oldSubs), len(*oldSubs)+1)
			copy(newSubs, *oldSubs)
		} else {
			newSubs = make([]*storage.Subscription, 0, 1)
		}
		newSubs = append(newSubs, sub)
		newNode.subs.Store(&newSubs)

		// Reuse children (they haven't changed)
		oldChildren := n.children.Load()
		if oldChildren != nil {
			newNode.children.Store(oldChildren)
		} else {
			emptyChildren := make(map[string]*lockFreeNode)
			newNode.children.Store(&emptyChildren)
		}

		return newNode
	}

	// Navigate deeper - need to copy path to target
	level := levels[0]
	oldChildren := n.children.Load()

	// Create new children map
	newChildren := make(map[string]*lockFreeNode)
	if oldChildren != nil {
		// Copy all existing children
		for k, v := range *oldChildren {
			newChildren[k] = v
		}
	}

	// Recursively update the target child
	oldChild := newChildren[level]
	newChildren[level] = r.copyAndAddSubscription(oldChild, levels[1:], sub)

	// Install new children map
	newNode.children.Store(&newChildren)

	// Reuse subscriptions at this level (unchanged)
	oldSubs := n.subs.Load()
	if oldSubs != nil {
		newNode.subs.Store(oldSubs)
	} else {
		emptySubs := make([]*storage.Subscription, 0)
		newNode.subs.Store(&emptySubs)
	}

	return newNode
}

// Unsubscribe removes a subscription from the router.
// Uses copy-on-write with CAS to ensure atomicity without locks.
func (r *LockFreeRouter) Unsubscribe(clientID string, filter string) error {
	levels := strings.Split(filter, separator)

	// CAS retry loop
	for {
		oldRoot := r.root.Load()
		newRoot := r.copyAndRemoveSubscription(oldRoot, levels, clientID)

		if r.root.CompareAndSwap(oldRoot, newRoot) {
			// Successfully installed new root
			return nil
		}
		// CAS failed, retry with new root
	}
}

// copyAndRemoveSubscription creates a new path through the trie with the subscription removed.
// Returns a new root node without the specified subscription.
func (r *LockFreeRouter) copyAndRemoveSubscription(n *lockFreeNode, levels []string, clientID string) *lockFreeNode {
	if n == nil {
		return nil
	}

	// Create new node (copy)
	newNode := &lockFreeNode{}

	if len(levels) == 0 {
		// At target level - remove subscription
		oldSubs := n.subs.Load()
		if oldSubs == nil || len(*oldSubs) == 0 {
			// No subscriptions, return unchanged node
			return n
		}

		// Filter out the target subscription
		newSubs := make([]*storage.Subscription, 0, len(*oldSubs))
		for _, sub := range *oldSubs {
			if sub.ClientID != clientID {
				newSubs = append(newSubs, sub)
			}
		}
		newNode.subs.Store(&newSubs)

		// Reuse children (unchanged)
		oldChildren := n.children.Load()
		if oldChildren != nil {
			newNode.children.Store(oldChildren)
		} else {
			emptyChildren := make(map[string]*lockFreeNode)
			newNode.children.Store(&emptyChildren)
		}

		return newNode
	}

	// Navigate deeper
	level := levels[0]
	oldChildren := n.children.Load()
	if oldChildren == nil {
		// Path doesn't exist, nothing to remove
		return n
	}

	oldChild, exists := (*oldChildren)[level]
	if !exists {
		// Path doesn't exist, nothing to remove
		return n
	}

	// Create new children map
	newChildren := make(map[string]*lockFreeNode)
	for k, v := range *oldChildren {
		newChildren[k] = v
	}

	// Recursively update the target child
	newChildren[level] = r.copyAndRemoveSubscription(oldChild, levels[1:], clientID)

	// Install new children map
	newNode.children.Store(&newChildren)

	// Reuse subscriptions at this level (unchanged)
	oldSubs := n.subs.Load()
	if oldSubs != nil {
		newNode.subs.Store(oldSubs)
	} else {
		emptySubs := make([]*storage.Subscription, 0)
		newNode.subs.Store(&emptySubs)
	}

	return newNode
}
