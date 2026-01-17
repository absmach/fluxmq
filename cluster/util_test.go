// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopicMatchesFilter_ExactMatch(t *testing.T) {
	tests := []struct {
		name   string
		topic  string
		filter string
		match  bool
	}{
		{"simple exact", "sensor/temp", "sensor/temp", true},
		{"multi-level exact", "sensor/room1/temp", "sensor/room1/temp", true},
		{"no match different", "sensor/temp", "sensor/humidity", false},
		{"no match prefix", "sensor/temp", "sensor/temp/extra", false},
		{"no match missing level", "sensor/temp/extra", "sensor/temp", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topicMatchesFilter(tt.topic, tt.filter)
			assert.Equal(t, tt.match, result)
		})
	}
}

func TestTopicMatchesFilter_SingleLevelWildcard(t *testing.T) {
	tests := []struct {
		name   string
		topic  string
		filter string
		match  bool
	}{
		{"single + at end", "sensor/temp", "sensor/+", true},
		{"single + at start", "sensor/temp", "+/temp", true},
		{"single + in middle", "sensor/room1/temp", "sensor/+/temp", true},
		{"multiple +", "sensor/room1/temp", "+/+/+", true},
		{"+ no match too short", "sensor", "sensor/+", false},
		{"+ no match too long", "sensor/room1/temp", "sensor/+", false},
		{"+ matches empty level", "/temp", "+/temp", true},
		{"all +", "a/b/c", "+/+/+", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topicMatchesFilter(tt.topic, tt.filter)
			assert.Equal(t, tt.match, result)
		})
	}
}

func TestTopicMatchesFilter_MultiLevelWildcard(t *testing.T) {
	tests := []struct {
		name   string
		topic  string
		filter string
		match  bool
	}{
		{"# matches all", "sensor/room1/temp/high", "#", true},
		{"# at end matches rest", "sensor/room1/temp", "sensor/#", true},
		{"# matches single level", "sensor/temp", "sensor/#", true},
		{"# matches zero levels", "sensor", "sensor/#", true},
		{"# no match prefix", "alerts/critical", "sensor/#", false},
		{"# after exact match", "sensor/room1/temp/high", "sensor/room1/#", true},
		{"# matches empty", "", "#", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topicMatchesFilter(tt.topic, tt.filter)
			assert.Equal(t, tt.match, result)
		})
	}
}

func TestTopicMatchesFilter_MixedWildcards(t *testing.T) {
	tests := []struct {
		name   string
		topic  string
		filter string
		match  bool
	}{
		{"+ before #", "sensor/room1/temp/high", "sensor/+/#", true},
		{"multiple + before #", "sensor/room1/floor2/temp", "+/+/#", true},
		{"exact then + then #", "sensor/room1/temp/high", "sensor/+/#", true},
		{"+ and # no match", "alerts/critical", "sensor/+/#", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topicMatchesFilter(tt.topic, tt.filter)
			assert.Equal(t, tt.match, result)
		})
	}
}

func TestTopicMatchesFilter_EdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		topic  string
		filter string
		match  bool
	}{
		{"both empty", "", "", true},
		{"empty topic # filter", "", "#", true},
		{"empty topic exact filter", "", "sensor", false},
		{"topic with empty level", "sensor//temp", "sensor//temp", true},
		{"+ matches empty level", "sensor//temp", "sensor/+/temp", true},
		{"system topic $", "$SYS/broker/stats", "$SYS/broker/stats", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topicMatchesFilter(tt.topic, tt.filter)
			assert.Equal(t, tt.match, result)
		})
	}
}

func TestTopicMatchesFilter_RealWorld(t *testing.T) {
	tests := []struct {
		name   string
		topic  string
		filter string
		match  bool
	}{
		{"sensor reading", "sensor/temperature/room1", "sensor/temperature/+", true},
		{"sensor all types", "sensor/temperature/room1", "sensor/+/room1", true},
		{"sensor all", "sensor/humidity/room2/value", "sensor/#", true},
		{"alert critical", "alerts/critical/fire", "alerts/critical/#", true},
		{"alert all severity", "alerts/warning/temp", "alerts/+/temp", true},
		{"device status", "device/12345/status", "device/+/status", true},
		{"device all", "device/12345/config/network", "device/12345/#", true},
		{"no match different device", "device/67890/status", "device/12345/#", false},
		{"no match different type", "sensor/temp", "alerts/#", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topicMatchesFilter(tt.topic, tt.filter)
			assert.Equal(t, tt.match, result)
		})
	}
}

func TestMatchLevels_DirectCalls(t *testing.T) {
	// Test the internal matchLevels function for edge cases
	tests := []struct {
		name   string
		topic  []string
		filter []string
		ti     int
		fi     int
		match  bool
	}{
		{"both at end", []string{"a"}, []string{"a"}, 1, 1, true},
		{"filter exhausted", []string{"a", "b"}, []string{"a"}, 1, 1, false},
		{"# matches remaining", []string{"a", "b", "c"}, []string{"a", "#"}, 1, 1, true},
		{"+ matches level", []string{"a", "b"}, []string{"a", "+"}, 1, 1, true},
		{"exact matches level", []string{"a", "b"}, []string{"a", "b"}, 1, 1, true},
		{"no match different", []string{"a", "b"}, []string{"a", "c"}, 1, 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchLevels(tt.topic, tt.filter, tt.ti, tt.fi)
			assert.Equal(t, tt.match, result)
		})
	}
}

func BenchmarkTopicMatchesFilter_Exact(b *testing.B) {
	topic := "sensor/room1/temperature/current"
	filter := "sensor/room1/temperature/current"

	for i := 0; i < b.N; i++ {
		topicMatchesFilter(topic, filter)
	}
}

func BenchmarkTopicMatchesFilter_SingleWildcard(b *testing.B) {
	topic := "sensor/room1/temperature/current"
	filter := "sensor/+/temperature/+"

	for i := 0; i < b.N; i++ {
		topicMatchesFilter(topic, filter)
	}
}

func BenchmarkTopicMatchesFilter_MultiWildcard(b *testing.B) {
	topic := "sensor/room1/temperature/current/value/high"
	filter := "sensor/room1/#"

	for i := 0; i < b.N; i++ {
		topicMatchesFilter(topic, filter)
	}
}

func BenchmarkTopicMatchesFilter_AllWildcard(b *testing.B) {
	topic := "sensor/room1/temperature/current/value/high"
	filter := "#"

	for i := 0; i < b.N; i++ {
		topicMatchesFilter(topic, filter)
	}
}
