// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/stretchr/testify/assert"
)

func TestSetMQTT5MetadataPreservesCorrelationData(t *testing.T) {
	tests := []struct {
		name            string
		correlationData []byte
	}{
		{
			name:            "ascii text",
			correlationData: []byte("request-123"),
		},
		{
			name:            "binary protobuf-like data",
			correlationData: []byte{0x08, 0x96, 0x01, 0x12, 0x07, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67},
		},
		{
			name:            "data with null bytes",
			correlationData: []byte{0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd},
		},
		{
			name:            "non-UTF-8 bytes",
			correlationData: []byte{0x80, 0x81, 0xfe, 0xff},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envelope := message.New("topic", nil)
			defer message.Release(envelope)
			input := append([]byte(nil), tt.correlationData...)
			setMQTT5Metadata(envelope, nil, time.Time{}, time.Time{}, nil, "", "", input)
			if len(input) > 0 {
				input[0] ^= 0xff
			}
			assert.Equal(t, tt.correlationData, envelope.User.CorrelationData)
		})
	}
}

func TestSetMQTT5MetadataCopiesScalarPointers(t *testing.T) {
	expiry := uint32(30)
	payloadFormat := byte(1)
	envelope := message.New("topic", nil)
	defer message.Release(envelope)

	setMQTT5Metadata(envelope, &expiry, time.Time{}, time.Time{}, &payloadFormat, "", "", nil)
	expiry = 1
	payloadFormat = 0

	assert.Equal(t, uint32(30), *envelope.User.MessageExpiry)
	assert.Equal(t, byte(1), *envelope.User.PayloadFormat)
}

func TestExtractUserPropertiesNil(t *testing.T) {
	result := extractUserProperties(nil)

	if len(result) != 0 {
		t.Errorf("expected empty map for nil props, got %v", result)
	}
}

const (
	testRuleTrace   = `["rule-a"]`
	testTraceVal    = "abc"
	testTraceKey    = "trace"
	testTenantValue = "acme"
	testTenantKey   = "tenant"
)

// A publishing device must not be able to set broker-internal properties: they
// are the channel services use to pass state that authenticates nothing on its
// own, so a forged one would let a client drive service behaviour.
func TestExtractUserPropertiesStripsReservedProperties(t *testing.T) {
	tests := []struct {
		name   string
		user   []v5.User
		want   map[string]string
		absent []string
	}{
		{
			name: "reserved property is dropped",
			user: []v5.User{
				{Key: message.ReservedPropertyPrefix + "re.trace", Value: testRuleTrace},
			},
			want:   map[string]string{},
			absent: []string{message.ReservedPropertyPrefix + "re.trace"},
		},
		{
			name: "ordinary properties are kept",
			user: []v5.User{
				{Key: testTraceKey, Value: testTraceVal},
				{Key: testTenantKey, Value: testTenantValue},
			},
			want: map[string]string{testTraceKey: testTraceVal, testTenantKey: testTenantValue},
		},
		{
			name: "reserved dropped alongside ordinary",
			user: []v5.User{
				{Key: testTraceKey, Value: testTraceVal},
				{Key: message.ReservedPropertyPrefix + "re.trace", Value: testRuleTrace},
			},
			want:   map[string]string{testTraceKey: testTraceVal},
			absent: []string{message.ReservedPropertyPrefix + "re.trace"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractUserProperties(&v5.PublishProperties{User: tc.user})

			assert.Equal(t, tc.want, got)
			for _, key := range tc.absent {
				assert.NotContains(t, got, key)
			}
		})
	}
}
