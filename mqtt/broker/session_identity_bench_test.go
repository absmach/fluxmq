// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"strconv"
	"testing"

	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage/memory"
)

const benchIdentity = "bench-entity"

// BenchmarkCreateSessionForIdentity covers the CONNECT path that resolves and
// binds a session identity. The publish benchmarks do not reach it: they run
// without an auth engine, so authorization short-circuits before any identity
// is resolved.
func BenchmarkCreateSessionForIdentity(b *testing.B) {
	b.Run("new session", func(b *testing.B) {
		br := NewBroker(memory.New(), nil)
		b.Cleanup(func() { _ = br.Close() })

		b.ReportAllocs()

		// b.Loop decides the iteration count as it goes, so the client ID is
		// built per iteration. That cost is identical on both sides of a
		// comparison and does not hide the identity work being measured.
		i := 0
		for b.Loop() {
			if _, _, err := br.CreateSessionForIdentity("bench-new-"+strconv.Itoa(i), 5, session.Options{
				ExternalID:     benchIdentity,
				ExpiryInterval: 300,
			}, false); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})

	b.Run("resume bound session", func(b *testing.B) {
		br := NewBroker(memory.New(), nil)
		b.Cleanup(func() { _ = br.Close() })

		opts := session.Options{ExternalID: benchIdentity, ExpiryInterval: 300}
		if _, _, err := br.CreateSessionForIdentity("bench-resume", 5, opts, false); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()

		for b.Loop() {
			if _, _, err := br.CreateSessionForIdentity("bench-resume", 5, opts, false); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("resume bound session over mtls", func(b *testing.B) {
		br := NewBroker(memory.New(), nil)
		b.Cleanup(func() { _ = br.Close() })

		opts := session.Options{ExternalID: benchIdentity, ExpiryInterval: 300}
		if _, _, err := br.CreateSessionForIdentity("bench-resume-mtls", 5, opts, true); err != nil {
			b.Fatal(err)
		}

		b.ReportAllocs()

		for b.Loop() {
			if _, _, err := br.CreateSessionForIdentity("bench-resume-mtls", 5, opts, true); err != nil {
				b.Fatal(err)
			}
		}
	})
}
