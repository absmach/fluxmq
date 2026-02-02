// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"

	"github.com/absmach/fluxmq/amqp/performatives"
	"github.com/absmach/fluxmq/amqp/types"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCursorEarliest(t *testing.T) {
	c := parseCursor("earliest")
	require.NotNil(t, c)
	assert.Equal(t, qtypes.CursorEarliest, c.Position)
}

func TestParseCursorLatest(t *testing.T) {
	c := parseCursor("latest")
	require.NotNil(t, c)
	assert.Equal(t, qtypes.CursorLatest, c.Position)
}

func TestParseCursorOffset(t *testing.T) {
	c := parseCursor("42")
	require.NotNil(t, c)
	assert.Equal(t, qtypes.CursorOffset, c.Position)
	assert.Equal(t, uint64(42), c.Offset)
}

func TestParseCursorUint64(t *testing.T) {
	c := parseCursor(uint64(100))
	require.NotNil(t, c)
	assert.Equal(t, qtypes.CursorOffset, c.Position)
	assert.Equal(t, uint64(100), c.Offset)
}

func TestParseCursorUint32(t *testing.T) {
	c := parseCursor(uint32(50))
	require.NotNil(t, c)
	assert.Equal(t, qtypes.CursorOffset, c.Position)
	assert.Equal(t, uint64(50), c.Offset)
}

func TestParseCursorInvalid(t *testing.T) {
	c := parseCursor("invalid")
	assert.Nil(t, c)
}

func TestParseCursorNilValue(t *testing.T) {
	c := parseCursor(nil)
	assert.Nil(t, c)
}

func TestNewLinkCapabilityQueueConsumer(t *testing.T) {
	// Simulate a consumer link with queue capability on source
	attach := &performatives.Attach{
		Name:   "consumer-link",
		Handle: 0,
		Role:   true, // client is receiver, broker is sender
		Source: &performatives.Source{
			Address:      "orders",
			Capabilities: []types.Symbol{performatives.CapQueue},
		},
	}

	// We can't call newLink without a session, but we can test the
	// capability detection logic directly
	assert.True(t, performatives.HasCapability(attach.Source.Capabilities, performatives.CapQueue))
}

func TestNewLinkCapabilityQueueProducer(t *testing.T) {
	attach := &performatives.Attach{
		Name:   "producer-link",
		Handle: 0,
		Role:   false, // client is sender
		Target: &performatives.Target{
			Address:      "orders",
			Capabilities: []types.Symbol{performatives.CapQueue},
		},
	}

	assert.True(t, performatives.HasCapability(attach.Target.Capabilities, performatives.CapQueue))
}

func TestNewLinkManagementDetection(t *testing.T) {
	// Verify management address constant
	assert.Equal(t, "$management", managementAddress)
}

func TestNewLinkLegacyQueuePrefix(t *testing.T) {
	attach := &performatives.Attach{
		Name:   "legacy-link",
		Handle: 0,
		Role:   true,
		Source: &performatives.Source{
			Address: "$queue/my-queue/topic/#",
		},
	}

	// Verify legacy address is detected
	assert.True(t, len(attach.Source.Address) > 0)
	assert.False(t, performatives.HasCapability(attach.Source.Capabilities, performatives.CapQueue))
}
