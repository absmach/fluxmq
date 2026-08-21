// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResolveRejectsMisplacedControlVerbs closes a hole a reviewer would have
// hit first: the ack verbs were matched as a suffix only, so an address that
// carried an identifier after them — the shape a client without message
// properties reaches for — resolved as an ordinary queue publish and enqueued a
// message into the queue it meant to acknowledge.
func TestResolveRejectsMisplacedControlVerbs(t *testing.T) {
	resolver := NewRoutingResolver()

	for _, topic := range []string{
		"$queue/m/$ack/msg-1",
		"$queue/m/$nack/msg-1/group-1",
		"$queue/m/$reject/msg-1",
		"$queue/m/$commit/42",
	} {
		t.Run(topic, func(t *testing.T) {
			route := resolver.Resolve(topic)
			require.Equal(t, RouteQueueMalformed, route.Kind,
				"a misplaced control verb must not resolve as a publish")
			assert.NotEmpty(t, route.ControlVerb)
		})
	}
}

// TestResolveKeepsWellFormedControlAddresses guards the other direction: the
// verbs still work where they belong, and a queue whose own path merely
// contains a similar word is untouched.
func TestResolveKeepsWellFormedControlAddresses(t *testing.T) {
	resolver := NewRoutingResolver()

	for _, tc := range []struct {
		topic string
		want  RouteKind
	}{
		{topic: "$queue/m/$ack", want: RouteQueueAck},
		{topic: "$queue/m/$nack", want: RouteQueueAck},
		{topic: "$queue/m/$reject", want: RouteQueueAck},
		{topic: "$queue/m/$commit", want: RouteQueueCommit},
		{topic: "$queue/m/acme/temp", want: RouteQueue},
		{topic: "$queue/m/ack/pending", want: RouteQueue},
	} {
		t.Run(tc.topic, func(t *testing.T) {
			assert.Equal(t, tc.want, resolver.Resolve(tc.topic).Kind)
		})
	}
}
