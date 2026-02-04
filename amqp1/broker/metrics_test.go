// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetrics(t *testing.T) {
	m, err := NewMetrics()
	require.NoError(t, err)
	assert.NotNil(t, m)
	assert.NotNil(t, m.connectionsTotal)
	assert.NotNil(t, m.messageSize)
}

func TestMetricsRecordMethods(t *testing.T) {
	m, err := NewMetrics()
	require.NoError(t, err)

	// These should not panic
	m.RecordConnection()
	m.RecordDisconnection()
	m.RecordMessageReceived(256)
	m.RecordMessageSent(128)
	m.RecordSessionOpened()
	m.RecordSessionClosed()
	m.RecordLinkAttached()
	m.RecordLinkDetached()
	m.RecordSubscriptionAdded()
	m.RecordSubscriptionRemoved()
	m.RecordError("auth")
	m.RecordError("protocol")
}
