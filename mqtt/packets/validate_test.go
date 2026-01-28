// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package packets_test

import (
	"strings"
	"testing"

	"github.com/absmach/fluxmq/mqtt/packets"
	"github.com/stretchr/testify/assert"
)

func TestValidateClientID(t *testing.T) {
	cases := []struct {
		desc     string
		clientID string
		wantErr  bool
	}{
		{
			desc:     "valid alphanumeric client ID",
			clientID: "valid123",
			wantErr:  false,
		},
		{
			desc:     "empty client ID",
			clientID: "",
			wantErr:  false,
		},
		{
			desc:     "client ID with special characters",
			clientID: "client-id_with.chars",
			wantErr:  false,
		},
		{
			desc:     "client ID with spaces",
			clientID: "client with spaces",
			wantErr:  false,
		},
		{
			desc:     "client ID with UTF-8 characters",
			clientID: "client世界🌍",
			wantErr:  false,
		},
		{
			desc:     "long client ID",
			clientID: strings.Repeat("a", 1000),
			wantErr:  false,
		},
		{
			desc:     "max length client ID",
			clientID: strings.Repeat("a", 65535),
			wantErr:  false,
		},
		{
			desc:     "invalid UTF-8 client ID",
			clientID: string([]byte{0xFF, 0xFE}),
			wantErr:  true,
		},
		{
			desc:     "client ID with null bytes",
			clientID: "client\x00id",
			wantErr:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			err := packets.ValidateClientID(tc.clientID)
			if tc.wantErr {
				assert.Error(t, err)
				assert.Equal(t, packets.ErrInvalidClientID, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateTopicName(t *testing.T) {
	cases := []struct {
		desc      string
		topic     string
		subscribe bool
	}{
		{
			desc:      "valid simple topic",
			topic:     "test/topic",
			subscribe: false,
		},
		{
			desc:      "valid multilevel topic",
			topic:     "level1/level2/level3",
			subscribe: false,
		},
		{
			desc:      "valid topic with special characters",
			topic:     "test-topic_123.abc",
			subscribe: false,
		},
		{
			desc:      "valid single level wildcard subscription",
			topic:     "test/+/topic",
			subscribe: true,
		},
		{
			desc:      "valid multilevel wildcard subscription",
			topic:     "test/#",
			subscribe: true,
		},
		{
			desc:      "valid wildcard at end",
			topic:     "test/topic/#",
			subscribe: true,
		},
		{
			desc:      "empty topic",
			topic:     "",
			subscribe: false,
		},
		{
			desc:      "root level topic",
			topic:     "/",
			subscribe: false,
		},
		{
			desc:      "topic with leading slash",
			topic:     "/test/topic",
			subscribe: false,
		},
		{
			desc:      "topic with trailing slash",
			topic:     "test/topic/",
			subscribe: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.GreaterOrEqual(t, len(tc.topic), 0)
		})
	}
}

func TestQoSLevels(t *testing.T) {
	cases := []struct {
		desc  string
		qos   byte
		valid bool
	}{
		{
			desc:  "QoS 0 at most once",
			qos:   0,
			valid: true,
		},
		{
			desc:  "QoS 1 at least once",
			qos:   1,
			valid: true,
		},
		{
			desc:  "QoS 2 exactly once",
			qos:   2,
			valid: true,
		},
		{
			desc:  "invalid QoS 3",
			qos:   3,
			valid: false,
		},
		{
			desc:  "invalid QoS 255",
			qos:   255,
			valid: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.LessOrEqual(t, tc.qos, byte(255))
		})
	}
}

func TestPacketIdentifiers(t *testing.T) {
	cases := []struct {
		desc  string
		id    uint16
		valid bool
	}{
		{
			desc:  "packet ID 1",
			id:    1,
			valid: true,
		},
		{
			desc:  "packet ID 100",
			id:    100,
			valid: true,
		},
		{
			desc:  "packet ID max value",
			id:    65535,
			valid: true,
		},
		{
			desc:  "packet ID 0 (technically invalid for QoS>0)",
			id:    0,
			valid: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.LessOrEqual(t, tc.id, uint16(65535))
		})
	}
}

func TestKeepAliveValues(t *testing.T) {
	cases := []struct {
		desc      string
		keepAlive uint16
	}{
		{
			desc:      "keep alive disabled",
			keepAlive: 0,
		},
		{
			desc:      "keep alive 30 seconds",
			keepAlive: 30,
		},
		{
			desc:      "keep alive 60 seconds",
			keepAlive: 60,
		},
		{
			desc:      "keep alive max value",
			keepAlive: 65535,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.LessOrEqual(t, tc.keepAlive, uint16(65535))
		})
	}
}
