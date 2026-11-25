// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"io"
	"testing"

	"github.com/absmach/fluxmq/internal/allocguard"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/mqtt/packets"
)

// Every delivery to a subscriber encodes one PUBLISH from a shared envelope
// and writes it. Neither step may allocate per subscriber.
func TestAllocsEncodePublishDelivery(t *testing.T) {
	plain := message.NewDelivery("devices/1/telemetry", make([]byte, 256), 1, false)
	t.Cleanup(func() { message.Release(plain) })

	rich := message.NewDelivery("devices/1/telemetry", make([]byte, 256), 1, false)
	rich.PublisherMeta.ContentType = "application/json"
	rich.PublisherMeta.Properties = message.NewPropertyMap(map[string]string{"region": "eu"})
	t.Cleanup(func() { message.Release(rich) })

	tests := []struct {
		name    string
		msg     *message.Envelope
		version byte
		qos     byte
		budget  float64
	}{
		{"v3/qos0", plain, packets.V311, 0, 0},
		{"v3/qos1", plain, packets.V311, 1, 0},
		{"v5/qos1", plain, packets.V5, 1, 0},
		// User properties go through message.ProjectProperties, which builds a
		// map per delivery; 3 pins that until the projection stops allocating.
		{"v5/qos1/properties", rich, packets.V5, 1, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allocguard.Max(t, tt.budget, func() {
				pkt := EncodePublishDelivery(tt.msg, 7, tt.version, false, tt.qos, false)
				if err := pkt.Pack(io.Discard); err != nil {
					t.Fatal(err)
				}
				pkt.Release()
			})
		})
	}
}
