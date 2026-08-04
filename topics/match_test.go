// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics_test

import (
	"strings"
	"testing"

	"github.com/absmach/fluxmq/topics"
)

const (
	testFooBar           = "foo/bar"
	testFooPlus          = "foo/+"
	testFoo              = "foo"
	testFooBarBaz        = "foo/bar/baz"
	testSysMonitorClient = "$SYS/monitor/Clients"
	testUserPlusCreated  = "user/+/created"
	testEmpty            = "empty"
	testOrdersEuCreated  = "orders.eu.created"
	testOrdersEuMqtt     = "orders/eu/created"
	testOrders           = "orders"
	testMAcmeCTemp       = "m/acme/c/temp"
)

func TestTopicMatch(t *testing.T) {
	tests := []struct {
		filter string
		topic  string
		want   bool
	}{
		{testFooBar, testFooBar, true},
		{testFooPlus, testFooBar, true},
		{testFooPlus, "foo/baz", true},
		{testFooPlus, testFoo, false},
		{testFooPlus, testFooBarBaz, false},
		{"foo/#", testFooBarBaz, true},
		{"foo/#", testFoo, true},
		{"#", testFooBar, true},
		{"#", "anything", true},
		{"+/+", testFooBar, true},
		{"+/+", testFooBarBaz, false},
		{testSysMonitorClient, testSysMonitorClient, true},
		{"$SYS/#", testSysMonitorClient, true},
		{"#", testSysMonitorClient, false},
		{"+/monitor/Clients", testSysMonitorClient, false},
		{testFooBar, "foo/baz", false},
		{"", testFoo, false},
		{testFoo, "", false},
	}

	for _, tt := range tests {
		if got := topics.TopicMatch(tt.filter, tt.topic); got != tt.want {
			t.Errorf("TopicMatch(%q, %q) = %v, want %v", tt.filter, tt.topic, got, tt.want)
		}
	}
}

// referenceTopicMatch is the split-based implementation TopicMatch replaced. It
// is kept here as the oracle for the equivalence test below: the rewrite exists
// to remove allocations, so it must not change a single answer.
func referenceTopicMatch(filter, topic string) bool {
	if filter == "" || topic == "" {
		return false
	}
	if filter == topic {
		return true
	}

	filterLevels := strings.Split(filter, "/")
	topicLevels := strings.Split(topic, "/")

	if strings.HasPrefix(topic, "$") {
		if len(filter) == 0 || filter[0] != '$' {
			return false
		}
		if filterLevels[0] == "+" || filterLevels[0] == "#" {
			return false
		}
	}

	for i, fLevel := range filterLevels {
		if fLevel == "#" {
			return true
		}
		if i >= len(topicLevels) {
			return false
		}
		tLevel := topicLevels[i]
		if fLevel == "+" {
			continue
		}
		if fLevel != tLevel {
			return false
		}
	}

	return len(filterLevels) == len(topicLevels)
}

// TestTopicMatchEquivalence exhaustively compares the allocation-free matcher
// against the implementation it replaced over every filter and topic that can
// be built from a small alphabet, up to three levels. The alphabet carries both
// wildcards, a "$"-prefixed level, and an empty level, so the cases the two
// implementations could plausibly disagree on — a trailing "#", a filter longer
// than its topic, "$" handling — are all covered in combination.
func TestTopicMatchEquivalence(t *testing.T) {
	levels := []string{"a", "b", "+", "#", "$sys", ""}

	var candidates []string
	for _, first := range levels {
		candidates = append(candidates, first)
		for _, second := range levels {
			candidates = append(candidates, first+"/"+second)
			for _, third := range levels {
				candidates = append(candidates, first+"/"+second+"/"+third)
			}
		}
	}

	for _, filter := range candidates {
		for _, topic := range candidates {
			if got, want := topics.TopicMatch(filter, topic), referenceTopicMatch(filter, topic); got != want {
				t.Fatalf("TopicMatch(%q, %q) = %v, reference = %v", filter, topic, got, want)
			}
		}
	}
}

func BenchmarkTopicMatch(b *testing.B) {
	benchmarks := []struct {
		name   string
		filter string
		topic  string
	}{
		{name: "exact", filter: testMAcmeCTemp, topic: testMAcmeCTemp},
		{name: "single_level_wildcard", filter: "m/+/c/+", topic: testMAcmeCTemp},
		{name: "multi_level_wildcard", filter: "m/acme/#", topic: "m/acme/c/temp/reading"},
		{name: "mismatch_first_level", filter: "$queue/orders/#", topic: "m/acme/c/temp/reading"},
		{name: "mismatch_last_level", filter: "m/acme/c/other", topic: testMAcmeCTemp},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				topics.TopicMatch(bm.filter, bm.topic)
			}
		})
	}
}
