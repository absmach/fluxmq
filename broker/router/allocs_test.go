// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"testing"

	"github.com/absmach/fluxmq/internal/allocguard"
	"github.com/absmach/fluxmq/storage"
)

const allocsExactTopic = "a/b/c"

// MatchInto runs once per publish. With a pooled result slice it must not
// allocate, whatever the subscriptions look like.
func TestAllocsMatchInto(t *testing.T) {
	r := NewRouter()
	for _, filter := range []string{allocsExactTopic, "a/+/c", "a/#", "#", "+/b/+", "$SYS/+/+", "x/y/z/w/v/u/t/s"} {
		for _, client := range []string{"c1", "c2", "c3"} {
			if err := r.Subscribe(client+filter, filter, 1, storage.SubscribeOptions{}); err != nil {
				t.Fatal(err)
			}
		}
	}

	tests := []struct {
		name  string
		topic string
	}{
		{"exact_and_wildcards", allocsExactTopic},
		{"multi_level_only", "a/q/r/s"},
		{"no_match", "zzz/yyy"},
		{"dollar_topic", "$SYS/broker/uptime"},
		{"deep", "x/y/z/w/v/u/t/s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allocguard.Max(t, 0, func() {
				matched := AcquireSubscriptionSlice()
				if err := r.MatchInto(tt.topic, matched); err != nil {
					t.Fatal(err)
				}
				ReleaseSubscriptionSlice(matched)
			})
		})
	}
}
