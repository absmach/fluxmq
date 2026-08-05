// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/absmach/fluxmq/topics"
)

// referenceFindMatching is the linear scan the trie replaced: one
// topics.TopicMatch per registered pattern. It is the oracle for the
// equivalence test below, because the rewrite exists to change the cost of
// matching and must not change a single answer.
func referenceFindMatching(patterns map[string][]string, topic string) []string {
	if topic == "" {
		return nil
	}

	var matched matchedQueues
	// Sorted so the reference is deterministic; the trie's own order is not
	// specified and both sides are sorted before comparison.
	keys := make([]string, 0, len(patterns))
	for pattern := range patterns {
		keys = append(keys, pattern)
	}
	sort.Strings(keys)

	for _, pattern := range keys {
		if !topics.TopicMatch(pattern, topic) {
			continue
		}
		for _, queueName := range patterns[pattern] {
			matched.add(queueName)
		}
	}
	return matched.names
}

// TestPatternTrieMatchesTheLinearScan compares the trie against the scan it
// replaced over every combination of a small alphabet, so the cases the two
// could plausibly disagree on — a trailing "#", a "+" against a shorter or
// longer topic, a pattern that is a prefix of another, several queues on one
// pattern — are all covered together rather than one at a time.
func TestPatternTrieMatchesTheLinearScan(t *testing.T) {
	// "#" only ever appears as the final level, because that is the only place
	// MQTT allows it. Elsewhere it is a malformed filter, which the two
	// implementations deliberately disagree about; see
	// TestPatternTrieIgnoresMisplacedMultiLevelWildcard.
	interior := []string{"a", "b", "+"}
	terminal := []string{"a", "b", "+", "#"}

	var candidates []string
	for _, first := range terminal {
		candidates = append(candidates, first)
	}
	for _, first := range interior {
		for _, second := range terminal {
			candidates = append(candidates, first+"/"+second)
		}
	}
	for _, first := range interior {
		for _, second := range interior {
			for _, third := range terminal {
				candidates = append(candidates, first+"/"+second+"/"+third)
			}
		}
	}

	// Every candidate is a pattern, bound to two queues so deduplication and
	// multi-queue collection are exercised as well.
	index := NewTopicIndex()
	patterns := make(map[string][]string, len(candidates))
	for i, pattern := range candidates {
		first := fmt.Sprintf("q%d", i)
		shared := "shared"
		index.AddQueue(first, []string{pattern})
		patterns[pattern] = append(patterns[pattern], first)
		if i%3 == 0 {
			// AddQueue replaces a queue's patterns, so the shared queue is
			// registered once at the end with everything it should match.
			patterns[pattern] = append(patterns[pattern], shared)
		}
	}
	sharedPatterns := make([]string, 0)
	for i, pattern := range candidates {
		if i%3 == 0 {
			sharedPatterns = append(sharedPatterns, pattern)
		}
	}
	index.AddQueue("shared", sharedPatterns)

	// Topics contain no wildcards, as a published topic never does.
	topicLevels := []string{"a", "b", "c"}
	var probes []string
	for _, first := range topicLevels {
		probes = append(probes, first)
		for _, second := range topicLevels {
			probes = append(probes, first+"/"+second)
			for _, third := range topicLevels {
				probes = append(probes, first+"/"+second+"/"+third)
			}
		}
	}

	for _, topic := range probes {
		got := append([]string(nil), index.FindMatching(topic)...)
		want := append([]string(nil), referenceFindMatching(patterns, topic)...)
		sort.Strings(got)
		sort.Strings(want)

		if len(got) != len(want) {
			t.Fatalf("FindMatching(%q) = %v, reference = %v", topic, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("FindMatching(%q) = %v, reference = %v", topic, got, want)
			}
		}
	}
}

// Removing a pattern must leave nothing behind, or repeated registration churn
// would grow the trie without bound.
func TestPatternTriePrunesRemovedPatterns(t *testing.T) {
	trie := newPatternTrie()
	trie.add("a/b/c", "q1")
	trie.add("a/b/d", "q2")

	trie.remove("a/b/c", "q1")
	if _, exists := trie.root.children["a"].children["b"].children["c"]; exists {
		t.Fatal("removed pattern left its node behind")
	}
	if _, exists := trie.root.children["a"].children["b"].children["d"]; !exists {
		t.Fatal("removing one pattern pruned a sibling still in use")
	}

	trie.remove("a/b/d", "q2")
	if len(trie.root.children) != 0 {
		t.Fatalf("trie retained %d nodes after every pattern was removed", len(trie.root.children))
	}
}

// A pattern shared by several queues must survive until the last one leaves.
func TestPatternTrieKeepsPatternWhileAnotherQueueUsesIt(t *testing.T) {
	trie := newPatternTrie()
	trie.add("m/#", "q1")
	trie.add("m/#", "q2")
	// Adding the same pair twice must not duplicate it.
	trie.add("m/#", "q2")

	var matched matchedQueues
	trie.match("m/acme", &matched)
	if len(matched.names) != 2 {
		t.Fatalf("matched %v, want both queues once each", matched.names)
	}

	trie.remove("m/#", "q1")
	var afterRemove matchedQueues
	trie.match("m/acme", &afterRemove)
	if len(afterRemove.names) != 1 || afterRemove.names[0] != "q2" {
		t.Fatalf("matched %v after removing q1, want [q2]", afterRemove.names)
	}
}

// A "#" anywhere but the final level is a malformed filter. topics.TopicMatch
// returns true as soon as it sees one, so under the previous linear scan a queue
// configured with "#/a" captured every publish on the broker. The trie treats
// the pattern as the literal path it is, so it captures nothing.
//
// Such a pattern is now rejected by queue configuration validation, so it should
// never reach the index. This pins the trie's behaviour anyway, as defence in
// depth: an index that captured everything on a pattern that reached it by some
// other path would be far worse than one that captures nothing.
func TestPatternTrieIgnoresMisplacedMultiLevelWildcard(t *testing.T) {
	index := NewTopicIndex()
	index.AddQueue("malformed", []string{"#/a"})
	index.AddQueue("wellformed", []string{"m/#"})

	if got := index.FindMatching("anything/at/all"); len(got) != 0 {
		t.Fatalf("FindMatching = %v, want nothing; a misplaced # must not capture every topic", got)
	}
	if got := index.FindMatching("m/acme"); len(got) != 1 || got[0] != "wellformed" {
		t.Fatalf("FindMatching = %v, want [wellformed]", got)
	}
}
