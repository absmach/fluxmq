// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"strings"
	"sync"

	"github.com/absmach/fluxmq/storage"
)

var _ storage.SubscriptionStore = (*SubscriptionStore)(nil)

// SubscriptionStore is an in-memory implementation of store.SubscriptionStore.
// It uses a trie for efficient topic matching.
type SubscriptionStore struct {
	mu    sync.RWMutex
	root  *trieNode
	count int
	// byClient provides O(1) lookup for client's subscriptions
	byClient map[string]map[string]*storage.Subscription // clientID -> filter -> subscription
}

type trieNode struct {
	children map[string]*trieNode
	subs     map[string]*storage.Subscription // clientID -> subscription at this level
}

func newTrieNode() *trieNode {
	return &trieNode{
		children: make(map[string]*trieNode),
		subs:     make(map[string]*storage.Subscription),
	}
}

// NewSubscriptionStore creates a new in-memory subscription store.
func NewSubscriptionStore() *SubscriptionStore {
	return &SubscriptionStore{
		root:     newTrieNode(),
		byClient: make(map[string]map[string]*storage.Subscription),
	}
}

// Add adds or updates a subscription.
func (s *SubscriptionStore) Add(sub *storage.Subscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if this is an update
	isNew := true
	if clientSubs, ok := s.byClient[sub.ClientID]; ok {
		if _, exists := clientSubs[sub.Filter]; exists {
			isNew = false
		}
	}

	// Navigate/create trie path
	levels := strings.Split(sub.Filter, "/")
	node := s.root
	for _, level := range levels {
		child, ok := node.children[level]
		if !ok {
			child = newTrieNode()
			node.children[level] = child
		}
		node = child
	}

	// Store subscription in trie
	subCopy := storage.CopySubscription(sub)
	node.subs[sub.ClientID] = subCopy

	// Store in client index
	if s.byClient[sub.ClientID] == nil {
		s.byClient[sub.ClientID] = make(map[string]*storage.Subscription)
	}
	s.byClient[sub.ClientID][sub.Filter] = subCopy

	if isNew {
		s.count++
	}

	return nil
}

// Remove removes a subscription.
func (s *SubscriptionStore) Remove(clientID, filter string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if subscription exists
	clientSubs, ok := s.byClient[clientID]
	if !ok {
		return nil
	}
	if _, exists := clientSubs[filter]; !exists {
		return nil
	}

	// Remove from trie
	levels := strings.Split(filter, "/")
	node := s.root
	for _, level := range levels {
		child, ok := node.children[level]
		if !ok {
			break
		}
		node = child
	}
	delete(node.subs, clientID)

	// Remove from client index
	delete(clientSubs, filter)
	if len(clientSubs) == 0 {
		delete(s.byClient, clientID)
	}

	s.count--
	return nil
}

// RemoveAll removes all subscriptions for a client.
func (s *SubscriptionStore) RemoveAll(clientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	clientSubs, ok := s.byClient[clientID]
	if !ok {
		return nil
	}

	// Remove each subscription from trie
	for filter := range clientSubs {
		levels := strings.Split(filter, "/")
		node := s.root
		for _, level := range levels {
			child, ok := node.children[level]
			if !ok {
				break
			}
			node = child
		}
		delete(node.subs, clientID)
		s.count--
	}

	// Remove client index
	delete(s.byClient, clientID)
	return nil
}

// GetForClient returns all subscriptions for a client.
func (s *SubscriptionStore) GetForClient(clientID string) ([]*storage.Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	clientSubs, ok := s.byClient[clientID]
	if !ok {
		return nil, nil
	}

	result := make([]*storage.Subscription, 0, len(clientSubs))
	for _, sub := range clientSubs {
		result = append(result, storage.CopySubscription(sub))
	}
	return result, nil
}

// Match returns all subscriptions matching a topic.
func (s *SubscriptionStore) Match(topic string) ([]*storage.Subscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	levels := strings.Split(topic, "/")
	var matched []*storage.Subscription
	s.matchLevel(s.root, levels, 0, &matched)

	// Deduplicate by clientID (keep highest QoS)
	return s.deduplicate(matched), nil
}

func (s *SubscriptionStore) matchLevel(node *trieNode, levels []string, index int, matched *[]*storage.Subscription) {
	if index == len(levels) {
		// Exact match reached
		for _, sub := range node.subs {
			*matched = append(*matched, storage.CopySubscription(sub))
		}
		// Check for '#' wildcard at this level
		if wild, ok := node.children["#"]; ok {
			for _, sub := range wild.subs {
				*matched = append(*matched, storage.CopySubscription(sub))
			}
		}
		return
	}

	level := levels[index]

	// Exact match traversal
	if child, ok := node.children[level]; ok {
		s.matchLevel(child, levels, index+1, matched)
	}

	// Single level wildcard '+'
	if child, ok := node.children["+"]; ok {
		s.matchLevel(child, levels, index+1, matched)
	}

	// Multi-level wildcard '#'
	if child, ok := node.children["#"]; ok {
		for _, sub := range child.subs {
			*matched = append(*matched, storage.CopySubscription(sub))
		}
	}
}

// deduplicate removes duplicate subscriptions for the same client, keeping highest QoS.
func (s *SubscriptionStore) deduplicate(subs []*storage.Subscription) []*storage.Subscription {
	seen := make(map[string]*storage.Subscription)
	for _, sub := range subs {
		if existing, ok := seen[sub.ClientID]; ok {
			if sub.QoS > existing.QoS {
				seen[sub.ClientID] = sub
			}
		} else {
			seen[sub.ClientID] = sub
		}
	}

	result := make([]*storage.Subscription, 0, len(seen))
	for _, sub := range seen {
		result = append(result, sub)
	}
	return result
}

// Count returns total subscription count.
func (s *SubscriptionStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}
