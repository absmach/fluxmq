// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics_test

import (
	"testing"

	"github.com/absmach/fluxmq/topics"
)

func TestValidateTopicName(t *testing.T) {
	tests := []struct {
		topic   string
		wantErr bool
	}{
		{"valid/topic", false},
		{"invalid/+", true},
		{"invalid/#", true},
		{"", true},
		{string([]byte{0xFF, 0xFE}), true}, // Invalid UTF-8
		{"null\u0000char", true},
	}

	for _, tt := range tests {
		if err := topics.ValidateTopicName(tt.topic); (err != nil) != tt.wantErr {
			t.Errorf("ValidateTopicName(%q) error = %v, wantErr %v", tt.topic, err, tt.wantErr)
		}
	}
}

func TestValidateTopicFilter(t *testing.T) {
	tests := []struct {
		filter  string
		wantErr bool
	}{
		{"valid/topic", false},
		{"sensor/+/temp", false},
		{"sensor/#", false},
		{"#", false},
		{"+", false},
		{"$share/group/sensor/+/temp", false},
		{"", true},
		{"sensor/#/tail", true},
		{"sensor/te+st", true},
		{"sensor/te#st", true},
		{"$share//sensor/#", true},
		{"$share/group/", true},
		{"$share/", true},
		{"$share/gr+oup/sensor/#", true},
		{"null\u0000char", true},
		{string([]byte{0xFF, 0xFE}), true}, // Invalid UTF-8
	}

	for _, tt := range tests {
		if err := topics.ValidateTopicFilter(tt.filter); (err != nil) != tt.wantErr {
			t.Errorf("ValidateTopicFilter(%q) error = %v, wantErr %v", tt.filter, err, tt.wantErr)
		}
	}
}
