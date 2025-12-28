// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics

import "testing"

func TestParseShared(t *testing.T) {
	tests := []struct {
		name              string
		filter            string
		expectedShare     string
		expectedTopic     string
		expectedIsShared  bool
	}{
		{
			name:             "Valid shared subscription",
			filter:           "$share/group1/sensors/#",
			expectedShare:    "group1",
			expectedTopic:    "sensors/#",
			expectedIsShared: true,
		},
		{
			name:             "Valid shared with multilevel wildcard",
			filter:           "$share/consumers/home/+/temperature",
			expectedShare:    "consumers",
			expectedTopic:    "home/+/temperature",
			expectedIsShared: true,
		},
		{
			name:             "Non-shared subscription",
			filter:           "sensors/#",
			expectedShare:    "",
			expectedTopic:    "sensors/#",
			expectedIsShared: false,
		},
		{
			name:             "Invalid shared format (no topic)",
			filter:           "$share/group1",
			expectedShare:    "",
			expectedTopic:    "$share/group1",
			expectedIsShared: false,
		},
		{
			name:             "Empty filter",
			filter:           "",
			expectedShare:    "",
			expectedTopic:    "",
			expectedIsShared: false,
		},
		{
			name:             "Share prefix only",
			filter:           "$share/",
			expectedShare:    "",
			expectedTopic:    "$share/",
			expectedIsShared: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shareName, topicFilter, isShared := ParseShared(tt.filter)

			if shareName != tt.expectedShare {
				t.Errorf("Expected share name '%s', got '%s'", tt.expectedShare, shareName)
			}
			if topicFilter != tt.expectedTopic {
				t.Errorf("Expected topic filter '%s', got '%s'", tt.expectedTopic, topicFilter)
			}
			if isShared != tt.expectedIsShared {
				t.Errorf("Expected isShared %v, got %v", tt.expectedIsShared, isShared)
			}
		})
	}
}

func TestIsShared(t *testing.T) {
	tests := []struct {
		filter   string
		expected bool
	}{
		{"$share/group1/topic", true},
		{"$share/", true}, // Starts with prefix (ParseShared will validate format)
		{"sensors/#", false},
		{"$topic", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.filter, func(t *testing.T) {
			result := IsShared(tt.filter)
			if result != tt.expected {
				t.Errorf("IsShared(%s) = %v, expected %v", tt.filter, result, tt.expected)
			}
		})
	}
}

func TestShareGroup_AddSubscriber(t *testing.T) {
	group := &ShareGroup{
		Name:        "group1",
		TopicFilter: "sensors/#",
		Subscribers: []string{},
	}

	// Add first subscriber
	if !group.AddSubscriber("client1") {
		t.Error("Expected AddSubscriber to return true for new subscriber")
	}
	if len(group.Subscribers) != 1 {
		t.Errorf("Expected 1 subscriber, got %d", len(group.Subscribers))
	}

	// Add second subscriber
	group.AddSubscriber("client2")
	if len(group.Subscribers) != 2 {
		t.Errorf("Expected 2 subscribers, got %d", len(group.Subscribers))
	}

	// Add duplicate (should not add)
	if group.AddSubscriber("client1") {
		t.Error("Expected AddSubscriber to return false for duplicate")
	}
	if len(group.Subscribers) != 2 {
		t.Errorf("Expected 2 subscribers after duplicate, got %d", len(group.Subscribers))
	}
}

func TestShareGroup_RemoveSubscriber(t *testing.T) {
	group := &ShareGroup{
		Name:        "group1",
		TopicFilter: "sensors/#",
		Subscribers: []string{"client1", "client2", "client3"},
	}

	// Remove existing subscriber
	if !group.RemoveSubscriber("client2") {
		t.Error("Expected RemoveSubscriber to return true")
	}
	if len(group.Subscribers) != 2 {
		t.Errorf("Expected 2 subscribers, got %d", len(group.Subscribers))
	}

	// Verify client2 is gone
	for _, sub := range group.Subscribers {
		if sub == "client2" {
			t.Error("client2 should have been removed")
		}
	}

	// Remove non-existent subscriber
	if group.RemoveSubscriber("client4") {
		t.Error("Expected RemoveSubscriber to return false for non-existent subscriber")
	}
}

func TestShareGroup_NextSubscriber(t *testing.T) {
	group := &ShareGroup{
		Name:        "group1",
		TopicFilter: "sensors/#",
		Subscribers: []string{"client1", "client2", "client3"},
	}

	// Test round-robin
	expected := []string{"client1", "client2", "client3", "client1", "client2"}
	for i, exp := range expected {
		sub := group.NextSubscriber()
		if sub != exp {
			t.Errorf("Round %d: expected '%s', got '%s'", i, exp, sub)
		}
	}

	// Test empty group
	emptyGroup := &ShareGroup{
		Subscribers: []string{},
	}
	if sub := emptyGroup.NextSubscriber(); sub != "" {
		t.Errorf("Expected empty string for empty group, got '%s'", sub)
	}
}

func TestShareGroup_IsEmpty(t *testing.T) {
	group := &ShareGroup{
		Subscribers: []string{"client1"},
	}

	if group.IsEmpty() {
		t.Error("Expected group to not be empty")
	}

	group.RemoveSubscriber("client1")
	if !group.IsEmpty() {
		t.Error("Expected group to be empty after removing all subscribers")
	}
}
