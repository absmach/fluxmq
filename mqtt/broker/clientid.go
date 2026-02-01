// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// GenerateClientID generates a random client ID.
// Format: auto-<16-char-hex>.
func GenerateClientID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random client ID: %w", err)
	}
	return "auto-" + hex.EncodeToString(b), nil
}
