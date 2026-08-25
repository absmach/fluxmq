// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Adapters that receive a delivery as a property map resolve the offset once,
// here, rather than parsing a textual identifier back when the settlement
// arrives. Offset 0 is a real offset, so "absent" and "zero" must stay distinct.
func TestQueueOffsetFromProperties(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
		wantOffset uint64
		wantOK     bool
	}{
		{name: "nil map", properties: nil},
		{name: "absent", properties: map[string]string{"other": "1"}},
		{name: "empty value", properties: map[string]string{PropertyOffset: ""}},
		{name: "not a number", properties: map[string]string{PropertyOffset: "seven"}},
		{name: "negative", properties: map[string]string{PropertyOffset: "-1"}},
		{name: "overflows uint64", properties: map[string]string{PropertyOffset: "18446744073709551616"}},
		{name: "composite identifier", properties: map[string]string{PropertyOffset: "orders:42"}},
		{name: "zero", properties: map[string]string{PropertyOffset: "0"}, wantOffset: 0, wantOK: true},
		{name: "small", properties: map[string]string{PropertyOffset: "42"}, wantOffset: 42, wantOK: true},
		{
			name:       "max uint64",
			properties: map[string]string{PropertyOffset: strconv.FormatUint(^uint64(0), 10)},
			wantOffset: ^uint64(0),
			wantOK:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, ok := QueueOffsetFromProperties(tt.properties)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantOffset, offset)
		})
	}
}

// A queue delivery always projects its offset, so the resolver can rely on it
// being present rather than falling back to a parsed message identifier.
func TestQueueDeliveryAlwaysProjectsItsOffset(t *testing.T) {
	for _, offset := range []uint64{0, 1, 42} {
		envelope := New("devices/1", []byte("payload"))
		envelope.Broker.Queue = QueueMetadata{
			MessageID: "orders:" + strconv.FormatUint(offset, 10),
			Name:      "orders",
			GroupID:   "workers",
			Offset:    offset,
		}

		projected := ProjectProperties(envelope, PublicProjection)
		require.NotNil(t, projected)

		resolved, ok := QueueOffsetFromProperties(projected)
		require.True(t, ok, "a queue delivery must carry a resolvable offset")
		assert.Equal(t, offset, resolved)

		Release(envelope)
	}
}

// An ordinary pub/sub message carries no queue metadata, so an adapter must be
// able to tell it apart from a queue delivery and never settle on its behalf.
func TestOrdinaryDeliveryCarriesNoQueueOffset(t *testing.T) {
	envelope := New("devices/1", []byte("payload"))
	defer Release(envelope)

	_, ok := QueueOffsetFromProperties(ProjectProperties(envelope, PublicProjection))
	assert.False(t, ok)
}
