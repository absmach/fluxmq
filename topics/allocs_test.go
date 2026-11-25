// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics

import (
	"testing"

	"github.com/absmach/fluxmq/internal/allocguard"
)

const allocsTopic = "a/b/c"

func TestAllocsTopicMatch(t *testing.T) {
	tests := []struct {
		name, filter, topic string
	}{
		{"exact", allocsTopic, allocsTopic},
		{"single_level", "a/+/c", allocsTopic},
		{"multi_level", "a/#", "a/b/c/d"},
		{"dollar_isolation", "#", "$SYS/uptime"},
		{"mismatch", allocsTopic, "a/b/d"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allocguard.Max(t, 0, func() { _ = TopicMatch(tt.filter, tt.topic) })
		})
	}
}
