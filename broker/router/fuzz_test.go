// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"testing"

	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/topics"
)

// FuzzMatchAgreesWithTopicMatch checks the trie against topics.TopicMatch, the
// reference matcher, for any valid filter and topic. It also checks that
// unsubscribing leaves nothing behind for the topic to match.
func FuzzMatchAgreesWithTopicMatch(f *testing.F) {
	seeds := [][2]string{
		{"#", "a/b"},
		{"+", "a"},
		{"a/+/c", "a/b/c"},
		{"a/#", "a"},
		{"+/+", "/x"},
		{"#", "$SYS/uptime"},
		{"$SYS/#", "$SYS/uptime"},
		{"+/x", "$a/x"},
		{"a//b", "a//b"},
		{"/#", "/"},
		{"+", ""},
		{"a/b", "a/b/"},
	}
	for _, s := range seeds {
		f.Add(s[0], s[1])
	}

	f.Fuzz(func(t *testing.T, filter, topic string) {
		if topics.ValidateTopicFilter(filter) != nil || topics.ValidateTopicName(topic) != nil {
			return
		}
		r := NewRouter()
		if err := r.Subscribe("c", filter, 0, storage.SubscribeOptions{}); err != nil {
			t.Fatalf("subscribe %q: %v", filter, err)
		}

		matched, err := r.Match(topic)
		if err != nil {
			t.Fatalf("match %q: %v", topic, err)
		}
		want := topics.TopicMatch(filter, topic)
		if got := len(matched) == 1; got != want {
			t.Fatalf("filter %q topic %q: trie matched=%v, TopicMatch=%v", filter, topic, got, want)
		}

		if err := r.Unsubscribe("c", filter); err != nil {
			t.Fatalf("unsubscribe %q: %v", filter, err)
		}
		if len(r.root.children) != 0 {
			t.Fatalf("filter %q left %d root children after unsubscribe", filter, len(r.root.children))
		}
	})
}
