// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"sync"
	"testing"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/packets"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	identityA = "entity-a"
	identityB = "entity-b"
)

func newIdentitySession(t *testing.T, externalID string) *Session {
	t.Helper()

	return New(
		"identity-client",
		packets.V5,
		Options{ExternalID: externalID},
		messages.NewInflightTracker(16),
		messages.NewMessageQueue(16, true),
		config.SessionConfig{InflightOverflow: config.InflightOverflowBackpressure},
	)
}

func TestIdentityAllows(t *testing.T) {
	tests := []struct {
		name         string
		bound        string
		incoming     string
		requireBound bool
		want         bool
	}{
		{name: "unbound session adopts an identity", incoming: identityA, want: true},
		{name: "unbound session adopts no identity", want: true},
		{name: "same identity resumes", bound: identityA, incoming: identityA, want: true},
		{name: "another identity is refused", bound: identityA, incoming: identityB},
		{name: "an unauthenticated reconnect cannot claim a bound session", bound: identityA},
		{name: "bound connect requires the same identity", bound: identityA, incoming: identityA, requireBound: true, want: true},
		{name: "bound connect refuses an unbound session", incoming: identityA, requireBound: true},
		{name: "bound connect refuses an empty identity", bound: identityA, requireBound: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IdentityAllows(tc.bound, tc.incoming, tc.requireBound))
		})
	}
}

func TestBindExternalIdentity(t *testing.T) {
	t.Run("binding an unbound session records the identity", func(t *testing.T) {
		s := newIdentitySession(t, "")
		require.True(t, s.BindExternalIdentity(identityA, false))
		assert.Equal(t, identityA, s.ExternalIdentity())
	})

	t.Run("a refused identity leaves the binding untouched", func(t *testing.T) {
		s := newIdentitySession(t, identityA)
		require.False(t, s.BindExternalIdentity(identityB, false))
		assert.Equal(t, identityA, s.ExternalIdentity())
	})

	t.Run("only one of two concurrent binds wins", func(t *testing.T) {
		s := newIdentitySession(t, "")

		var (
			wg    sync.WaitGroup
			mu    sync.Mutex
			bound []string
		)
		for _, externalID := range []string{identityA, identityB} {
			wg.Add(1)
			go func() {
				defer wg.Done()

				if s.BindExternalIdentity(externalID, false) {
					mu.Lock()
					bound = append(bound, externalID)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		require.Len(t, bound, 1)
		assert.Equal(t, bound[0], s.ExternalIdentity())
	})
}

func TestMarkOfflineStampsDisconnectOnce(t *testing.T) {
	s := newIdentitySession(t, identityA)
	require.True(t, s.GetDisconnectedAt().IsZero())

	s.MarkOffline()
	stamped := s.GetDisconnectedAt()
	require.False(t, stamped.IsZero())

	s.MarkOffline()
	assert.Equal(t, stamped, s.GetDisconnectedAt(), "a second call must not extend the expiry deadline")
}
