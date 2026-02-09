// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"encoding/base64"
	"testing"

	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

func TestExtractAllProperties_CorrelationDataBase64(t *testing.T) {
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
			props := &v5.PublishProperties{
				CorrelationData: tt.correlationData,
			}

			result := extractAllProperties(props)

			encoded := result["correlation-id"]
			if encoded == "" {
				t.Fatal("correlation-id not set in result")
			}

			decoded, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				t.Fatalf("correlation-id is not valid base64: %v", err)
			}

			if string(decoded) != string(tt.correlationData) {
				t.Errorf("round-trip failed: got %v, want %v", decoded, tt.correlationData)
			}
		})
	}
}

func TestExtractAllProperties_NilCorrelationData(t *testing.T) {
	props := &v5.PublishProperties{
		ContentType: "application/json",
	}

	result := extractAllProperties(props)

	if _, ok := result["correlation-id"]; ok {
		t.Error("correlation-id should not be set when CorrelationData is nil")
	}
}

func TestExtractAllProperties_NilProps(t *testing.T) {
	result := extractAllProperties(nil)

	if len(result) != 0 {
		t.Errorf("expected empty map for nil props, got %v", result)
	}
}
