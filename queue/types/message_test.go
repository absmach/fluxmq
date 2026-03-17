// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessage_IsExpired(t *testing.T) {
	tests := []struct {
		name      string
		expiresAt time.Time
		expected  bool
	}{
		{
			name:      "zero value is non-expiring",
			expiresAt: time.Time{},
			expected:  false,
		},
		{
			name:      "future expiry is not expired",
			expiresAt: time.Now().Add(time.Hour),
			expected:  false,
		},
		{
			name:      "past expiry is expired",
			expiresAt: time.Now().Add(-time.Second),
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{ExpiresAt: tt.expiresAt}
			assert.Equal(t, tt.expected, msg.IsExpired())
		})
	}
}
