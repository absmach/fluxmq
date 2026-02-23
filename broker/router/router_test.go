// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"testing"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

func TestSubscribe_ReplacesExistingClientFilter(t *testing.T) {
	r := NewRouter()

	require.NoError(t, r.Subscribe("client-a", "sensors/temperature", 0, storage.SubscribeOptions{}))
	require.NoError(t, r.Subscribe("client-a", "sensors/temperature", 2, storage.SubscribeOptions{}))

	matched, err := r.Match("sensors/temperature")
	require.NoError(t, err)
	require.Len(t, matched, 1)
	require.Equal(t, byte(2), matched[0].QoS)
}

func TestSubscribe_DifferentClientsRemainDistinct(t *testing.T) {
	r := NewRouter()

	require.NoError(t, r.Subscribe("client-a", "sensors/temperature", 0, storage.SubscribeOptions{}))
	require.NoError(t, r.Subscribe("client-b", "sensors/temperature", 1, storage.SubscribeOptions{}))

	matched, err := r.Match("sensors/temperature")
	require.NoError(t, err)
	require.Len(t, matched, 2)
}
