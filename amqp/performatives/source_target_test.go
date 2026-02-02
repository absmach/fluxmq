// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package performatives

import (
	"bytes"
	"testing"

	"github.com/absmach/fluxmq/amqp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourceCapabilitiesSingle(t *testing.T) {
	s := &Source{
		Address:      "my-queue",
		Capabilities: []types.Symbol{CapQueue},
	}

	encoded, err := s.Encode()
	require.NoError(t, err)

	// Decode: strip described type wrapper
	r := bytes.NewReader(encoded)
	val, err := types.ReadType(r)
	require.NoError(t, err)

	desc, ok := val.(*types.Described)
	require.True(t, ok)
	assert.Equal(t, DescriptorSource, desc.Descriptor)

	fields, ok := desc.Value.([]any)
	require.True(t, ok)

	decoded := DecodeSource(fields)
	assert.Equal(t, "my-queue", decoded.Address)
	require.Len(t, decoded.Capabilities, 1)
	assert.Equal(t, CapQueue, decoded.Capabilities[0])
}

func TestSourceCapabilitiesMultiple(t *testing.T) {
	s := &Source{
		Address:      "my-queue",
		Capabilities: []types.Symbol{CapQueue, "topic"},
	}

	encoded, err := s.Encode()
	require.NoError(t, err)

	r := bytes.NewReader(encoded)
	val, err := types.ReadType(r)
	require.NoError(t, err)

	desc := val.(*types.Described)
	fields := desc.Value.([]any)

	decoded := DecodeSource(fields)
	assert.Equal(t, "my-queue", decoded.Address)
	require.Len(t, decoded.Capabilities, 2)
	assert.Equal(t, CapQueue, decoded.Capabilities[0])
	assert.Equal(t, types.Symbol("topic"), decoded.Capabilities[1])
}

func TestSourceNoCapabilities(t *testing.T) {
	s := &Source{Address: "test/topic"}

	encoded, err := s.Encode()
	require.NoError(t, err)

	r := bytes.NewReader(encoded)
	val, err := types.ReadType(r)
	require.NoError(t, err)

	desc := val.(*types.Described)
	fields := desc.Value.([]any)

	decoded := DecodeSource(fields)
	assert.Equal(t, "test/topic", decoded.Address)
	assert.Nil(t, decoded.Capabilities)
}

func TestTargetCapabilities(t *testing.T) {
	tgt := &Target{
		Address:      "my-queue",
		Capabilities: []types.Symbol{CapQueue},
	}

	encoded, err := tgt.Encode()
	require.NoError(t, err)

	r := bytes.NewReader(encoded)
	val, err := types.ReadType(r)
	require.NoError(t, err)

	desc := val.(*types.Described)
	fields := desc.Value.([]any)

	decoded := DecodeTarget(fields)
	assert.Equal(t, "my-queue", decoded.Address)
	require.Len(t, decoded.Capabilities, 1)
	assert.Equal(t, CapQueue, decoded.Capabilities[0])
}

func TestTargetNoCapabilities(t *testing.T) {
	tgt := &Target{Address: "test/topic"}

	encoded, err := tgt.Encode()
	require.NoError(t, err)

	r := bytes.NewReader(encoded)
	val, err := types.ReadType(r)
	require.NoError(t, err)

	desc := val.(*types.Described)
	fields := desc.Value.([]any)

	decoded := DecodeTarget(fields)
	assert.Equal(t, "test/topic", decoded.Address)
	assert.Nil(t, decoded.Capabilities)
}

func TestHasCapability(t *testing.T) {
	caps := []types.Symbol{CapQueue, "topic"}
	assert.True(t, HasCapability(caps, CapQueue))
	assert.True(t, HasCapability(caps, "topic"))
	assert.False(t, HasCapability(caps, "other"))
	assert.False(t, HasCapability(nil, CapQueue))
}

func TestDecodeCapabilitiesSingleSymbol(t *testing.T) {
	caps := decodeCapabilities(types.Symbol("queue"))
	require.Len(t, caps, 1)
	assert.Equal(t, types.Symbol("queue"), caps[0])
}

func TestDecodeCapabilitiesArray(t *testing.T) {
	caps := decodeCapabilities([]any{types.Symbol("queue"), types.Symbol("topic")})
	require.Len(t, caps, 2)
	assert.Equal(t, types.Symbol("queue"), caps[0])
	assert.Equal(t, types.Symbol("topic"), caps[1])
}

func TestDecodeCapabilitiesNil(t *testing.T) {
	caps := decodeCapabilities(nil)
	assert.Nil(t, caps)
}
