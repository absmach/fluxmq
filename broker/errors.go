// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"errors"
	"strings"
)

// ErrClientNotConnected is returned by protocol adapters when a queue delivery
// targets a client that no longer has a live connection.
var ErrClientNotConnected = errors.New("client not connected")

// IsClientNotConnected reports whether err means a queue delivery target is
// gone. The string fallback preserves this signal across RPC boundaries where
// error wrapping is flattened into response text.
func IsClientNotConnected(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrClientNotConnected) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, ErrClientNotConnected.Error()) ||
		strings.Contains(msg, "client not found") ||
		strings.Contains(msg, "session not found")
}
