// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics_test

import (
	"testing"

	"github.com/absmach/fluxmq/topics"
)

func TestTopicMatch(t *testing.T) {
	tests := []struct {
		filter string
		topic  string
		want   bool
	}{
		{"foo/bar", "foo/bar", true},
		{"foo/+", "foo/bar", true},
		{"foo/+", "foo/baz", true},
		{"foo/+", "foo", false},
		{"foo/+", "foo/bar/baz", false},
		{"foo/#", "foo/bar/baz", true},
		{"foo/#", "foo", true},
		{"#", "foo/bar", true},
		{"#", "anything", true},
		{"+/+", "foo/bar", true},
		{"+/+", "foo/bar/baz", false},
		{"$SYS/monitor/Clients", "$SYS/monitor/Clients", true},
		{"$SYS/#", "$SYS/monitor/Clients", true},
		{"#", "$SYS/monitor/Clients", false},
		{"+/monitor/Clients", "$SYS/monitor/Clients", false},
		{"foo/bar", "foo/baz", false},
		{"", "foo", false},
		{"foo", "", false},
	}

	for _, tt := range tests {
		if got := topics.TopicMatch(tt.filter, tt.topic); got != tt.want {
			t.Errorf("TopicMatch(%q, %q) = %v, want %v", tt.filter, tt.topic, got, tt.want)
		}
	}
}
