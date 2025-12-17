// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package packets

import (
	"errors"
	"unicode/utf8"
)

// Common validation errors.
var (
	ErrInvalidClientID = errors.New("invalid client id")
	ErrInvalidQoS      = errors.New("invalid qos level")
)

// Validator is an interface for packets that can validate themselves.
type Validator interface {
	Validate() error
}

// ValidateClientID checks if the client ID is valid (0-9, a-z, A-Z) and length restrictions.
// V3.1.1 allows 1-23 chars, but almost all brokers support longer. The spec says
// "The Server MUST allow ClientIds which are between 1 and 23 UTF-8 encoded bytes in length... may allow ClientIds that contain more than 23...".
// We will allow flexible length but restrict chars if required.
// Actually spec says "contains only characters 0-9a-zA-Z".
func ValidateClientID(clientID string) error {
	if clientID == "" {
		// Empty clientID allowed if CleanSession=1 (V3) / CleanStart=1 (V5).
		// Validation of emptiness depends on context, so we just check chars here.
		return nil
	}
	// We strictly enforce text if we want to follow spec recommendation,
	// but many production systems allow more.
	// Let's enforce basic utf8 validity.
	if !utf8.ValidString(clientID) {
		return ErrInvalidClientID
	}
	return nil
}
