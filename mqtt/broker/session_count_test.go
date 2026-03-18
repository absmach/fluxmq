// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"

	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage/memory"
)

func TestSessionCount(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	if got := b.SessionCount(); got != 0 {
		t.Fatalf("expected 0 sessions, got %d", got)
	}

	for _, id := range []string{"c1", "c2", "c3"} {
		if _, _, err := b.CreateSession(id, 5, session.Options{CleanStart: true}); err != nil {
			t.Fatalf("create session %s: %v", id, err)
		}
	}

	if got := b.SessionCount(); got != 3 {
		t.Fatalf("expected 3 sessions, got %d", got)
	}

	// Remove one session and verify count decreases
	b.sessionsMap.Delete("c2")
	if got := b.SessionCount(); got != 2 {
		t.Fatalf("expected 2 sessions after delete, got %d", got)
	}
}
