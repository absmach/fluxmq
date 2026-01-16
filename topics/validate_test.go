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
