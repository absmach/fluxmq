// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import "strings"

const patternSeparator = "/"

// patternTrie maps topic patterns to the queues bound to them, and matches a
// topic against all of them at once.
//
// It replaces a linear scan that called topics.TopicMatch per registered
// pattern. That cost grew with the number of patterns configured rather than
// with the topic being matched, which put a ceiling on publish throughput on any
// broker binding many queues to ordinary topics. Descending a trie instead makes
// matching a function of the topic's own depth and the wildcards actually
// present, not of how many patterns exist.
//
// Matching mirrors the MQTT rules topics.TopicMatch implements: "+" matches
// exactly one level, "#" matches zero or more trailing levels — so "a/#" matches
// "a" itself — and a filter with no "#" must consume the whole topic. The one
// rule it does not implement is the "$" guard, because TopicIndex partitions
// patterns on that prefix before they reach a trie: a "#" pattern is only ever
// in the half consulted for ordinary topics.
//
// It is not safe for concurrent use; TopicIndex holds the lock.
type patternTrie struct {
	root *patternNode
}

type patternNode struct {
	children map[string]*patternNode
	// queues are the queues whose pattern terminates at this node.
	queues []string
}

func newPatternTrie() *patternTrie {
	return &patternTrie{root: newPatternNode()}
}

func newPatternNode() *patternNode {
	return &patternNode{children: make(map[string]*patternNode)}
}

// add binds a queue to a pattern. Adding the same pair twice is a no-op, so a
// queue cannot be reported twice for one pattern.
func (t *patternTrie) add(pattern, queueName string) {
	node := t.root
	remaining := pattern
	for {
		level, rest, more := strings.Cut(remaining, patternSeparator)
		child, ok := node.children[level]
		if !ok {
			child = newPatternNode()
			node.children[level] = child
		}
		node = child
		if !more {
			break
		}
		remaining = rest
	}

	for _, existing := range node.queues {
		if existing == queueName {
			return
		}
	}
	node.queues = append(node.queues, queueName)
}

// remove unbinds a queue from a pattern and prunes the nodes the pattern no
// longer needs, so repeated registration churn cannot grow the trie without
// bound.
func (t *patternTrie) remove(pattern, queueName string) {
	removeQueueFromPattern(t.root, pattern, queueName)
}

func removeQueueFromPattern(node *patternNode, remaining, queueName string) {
	level, rest, more := strings.Cut(remaining, patternSeparator)
	child, ok := node.children[level]
	if !ok {
		return
	}

	if more {
		removeQueueFromPattern(child, rest, queueName)
	} else {
		for i, existing := range child.queues {
			if existing != queueName {
				continue
			}
			child.queues = append(child.queues[:i], child.queues[i+1:]...)
			break
		}
	}

	// Prune on the way back out: a node with no queues and no children can no
	// longer contribute to any match.
	if len(child.queues) == 0 && len(child.children) == 0 {
		delete(node.children, level)
	}
}

// match collects every queue whose pattern matches the topic.
//
// It allocates nothing: levels are walked with strings.Cut rather than split
// into a slice, and the result is appended to only when a queue actually
// matches.
func (t *patternTrie) match(topic string, matched *matchedQueues) {
	matchPatternNode(t.root, topic, false, matched)
}

func matchPatternNode(node *patternNode, remaining string, consumed bool, matched *matchedQueues) {
	if consumed {
		// The topic ended here, so patterns terminating at this node match, as
		// does a "#" beneath it: "a/#" matches "a".
		collectQueues(node, matched)
		if child, ok := node.children["#"]; ok {
			collectQueues(child, matched)
		}
		return
	}

	level, rest, more := strings.Cut(remaining, patternSeparator)

	if child, ok := node.children[level]; ok {
		matchPatternNode(child, rest, !more, matched)
	}
	if child, ok := node.children["+"]; ok {
		matchPatternNode(child, rest, !more, matched)
	}
	// "#" consumes every remaining level, so it matches without descending.
	if child, ok := node.children["#"]; ok {
		collectQueues(child, matched)
	}
}

func collectQueues(node *patternNode, matched *matchedQueues) {
	for _, queueName := range node.queues {
		matched.add(queueName)
	}
}
