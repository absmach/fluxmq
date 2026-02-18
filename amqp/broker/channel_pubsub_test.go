// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "testing"

func TestConsumerQueueMatchesUsesMQTTCanonicalFilter(t *testing.T) {
	ch := &Channel{}
	cons := &consumer{
		queue:      "user.*.created",
		mqttFilter: "user/+/created",
	}

	if !ch.consumerQueueMatches(cons, "user/eu/created") {
		t.Fatal("expected translated filter to match topic")
	}
	if ch.consumerQueueMatches(cons, "user/eu/deleted") {
		t.Fatal("expected translated filter not to match topic")
	}
}
