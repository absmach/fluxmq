package packets_test

import (
	"testing"

	"github.com/dborovcanin/mqtt/packets"
)

func TestValidateClientID(t *testing.T) {
	tests := []struct {
		clientID string
		wantErr  bool
	}{
		{"valid123", false},
		{"", false},                        // Empty allowed (context dependent)
		{"invalid char $", false},          // We decided to allow almost anything UTF-8 valid
		{string([]byte{0xFF, 0xFE}), true}, // Invalid UTF-8
	}

	for _, tt := range tests {
		if err := packets.ValidateClientID(tt.clientID); (err != nil) != tt.wantErr {
			t.Errorf("ValidateClientID(%q) error = %v, wantErr %v", tt.clientID, err, tt.wantErr)
		}
	}
}
