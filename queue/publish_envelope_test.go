// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"testing"

	"github.com/absmach/fluxmq/message"
)

// publishEnvelope builds the envelope a publish or append command borrows.
// The command surface never takes ownership, so the caller releases it — here,
// when the test ends.
func publishEnvelope(tb testing.TB, topic string, payload []byte) *message.Envelope {
	tb.Helper()
	envelope := message.New(topic, payload)
	tb.Cleanup(func() { message.Release(envelope) })
	return envelope
}
