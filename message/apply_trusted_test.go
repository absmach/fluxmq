// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A malformed numeric property used to be discarded and replaced with a zero,
// which is a real value in every one of these fields. A corrupt offset became
// offset 0 — the head of the queue — and nothing upstream could tell the
// difference.
func TestApplyTrustedPropertiesReportsMalformedNumbers(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
	}{
		{
			name:       "queue offset",
			properties: map[string]string{PropertyOffset: "not-a-number"},
		},
		{
			name: "stream offset",
			properties: map[string]string{
				PropertyStreamOffset: "seven",
			},
		},
		{
			name: "stream timestamp",
			properties: map[string]string{
				PropertyStreamOffset:    "1",
				PropertyStreamTimestamp: "yesterday",
			},
		},
		{
			name: "committed offset",
			properties: map[string]string{
				PropertyStreamOffset:  "1",
				PropertyWorkCommitted: "-",
			},
		},
		{
			name: "work acknowledged",
			properties: map[string]string{
				PropertyStreamOffset: "1",
				PropertyWorkAcked:    "maybe",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envelope := New("devices/1", []byte("payload"))
			defer Release(envelope)

			err := ApplyTrustedProperties(envelope, tt.properties)
			require.Error(t, err, "a malformed value must not be silently zeroed")
		})
	}
}

// Everything that does parse is still applied, so a caller that decides to
// continue past a malformed field sees the rest of the message.
func TestApplyTrustedPropertiesAppliesWhatParses(t *testing.T) {
	envelope := New("devices/1", []byte("payload"))
	defer Release(envelope)

	err := ApplyTrustedProperties(envelope, map[string]string{
		PropertyOffset:          "corrupt",
		PropertyQueueName:       "readings",
		PropertyGroupID:         "stream-workers",
		PropertySourceTopic:     "sensors/temperature",
		PropertyStreamOffset:    "9",
		PropertyStreamTimestamp: "1700000000",
		PropertyWorkAcked:       "true",
		PropertyWorkGroup:       "stream-workers",
		"tenant":                "acme",
	})
	require.Error(t, err)

	assert.Equal(t, "readings", envelope.BrokerMeta.Queue.Name)
	assert.Equal(t, "stream-workers", envelope.BrokerMeta.Queue.GroupID)
	assert.Equal(t, "sensors/temperature", envelope.BrokerMeta.Source.Topic)
	assert.Equal(t, uint64(0), envelope.BrokerMeta.Queue.Offset, "the corrupt offset stays unset")
	require.NotNil(t, envelope.BrokerMeta.Queue.Stream)
	assert.Equal(t, uint64(9), envelope.BrokerMeta.Queue.Stream.Offset)
	assert.Equal(t, int64(1700000000), envelope.BrokerMeta.Queue.Stream.Timestamp)
	assert.True(t, envelope.BrokerMeta.Queue.Stream.WorkAcknowledged)
	assert.Equal(t, "acme", envelope.PublisherMeta.Properties["tenant"])
}

func TestApplyTrustedPropertiesAcceptsWellFormedValues(t *testing.T) {
	envelope := New("devices/1", []byte("payload"))
	defer Release(envelope)

	require.NoError(t, ApplyTrustedProperties(envelope, map[string]string{
		PropertyOffset:        "0",
		PropertyStreamOffset:  "0",
		PropertyWorkCommitted: "0",
		PropertyWorkAcked:     "false",
	}))
	assert.Equal(t, uint64(0), envelope.BrokerMeta.Queue.Offset)
	require.NotNil(t, envelope.BrokerMeta.Queue.Stream)
	assert.True(t, envelope.BrokerMeta.Queue.Stream.HasCommittedOffset)
}

func TestApplyTrustedPropertiesIgnoresNilEnvelope(t *testing.T) {
	require.NoError(t, ApplyTrustedProperties(nil, map[string]string{PropertyOffset: "bad"}))
}
