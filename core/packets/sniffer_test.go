package packets_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/absmach/mqtt/core/packets"
)

func TestDetectProtocolVersion(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		wantVersion int
		wantErr     bool
	}{
		{
			name: "MQTT 3.1.1 (v4)",
			data: []byte{
				0x10, 10, // Fixed Header
				0, 4, 'M', 'Q', 'T', 'T', // Protocol Name
				4,       // Version
				0, 0, 0, // Flags + KeepAlive
			},
			wantVersion: 4,
		},
		{
			name: "MQTT 5.0 (v5)",
			data: []byte{
				0x10, 10,
				0, 4, 'M', 'Q', 'T', 'T',
				5,
				0, 0, 0,
			},
			wantVersion: 5,
		},
		{
			name: "Detects NOT Connect",
			data: []byte{
				0x20, 10, // CONNACK type
				0, 0, 0, 0, 0, 0,
			},
			wantVersion: 0,
		},
		{
			name: "Invalid Protocol Name",
			data: []byte{
				0x10, 10,
				0, 4, 'F', 'A', 'K', 'E',
				4,
				0, 0, 0,
			},
			wantVersion: 0,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.data)
			gotVersion, restored, err := packets.DetectProtocolVersion(r)
			if (err != nil) != tt.wantErr {
				t.Errorf("DetectProtocolVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotVersion != tt.wantVersion {
				t.Errorf("DetectProtocolVersion() gotVersion = %v, want %v", gotVersion, tt.wantVersion)
			}

			// Verify data is preserved
			restoredData, _ := io.ReadAll(restored)
			if !bytes.Equal(restoredData, tt.data) {
				t.Errorf("Restored reader did not return original data")
			}
		})
	}
}

func TestDetectPacketType(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		wantType byte
		wantErr  bool
	}{
		{"Connect Packet", []byte{0x10, 0x0C}, 1, false},
		{"Publish Packet", []byte{0x30, 0x05}, 3, false},
		{"PingReq Packet", []byte{0xC0, 0x00}, 12, false},
		{"Empty Stream", []byte{}, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.data)
			gotType, restored, err := packets.DetectPacketType(r)
			if (err != nil) != tt.wantErr {
				t.Errorf("DetectPacketType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotType != tt.wantType {
				t.Errorf("DetectPacketType() gotType = %v, want %v", gotType, tt.wantType)
			}

			// Verify data is preserved
			if len(tt.data) > 0 {
				restoredData, _ := io.ReadAll(restored)
				if !bytes.Equal(restoredData, tt.data) {
					t.Errorf("Restored reader did not return original data")
				}
			}
		})
	}
}
