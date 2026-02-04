// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package sasl

import (
	"testing"

	"github.com/absmach/fluxmq/amqp1/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMechanisms(t *testing.T) {
	m := &Mechanisms{Mechanisms: []types.Symbol{MechPLAIN, MechANONYMOUS}}
	body, err := m.Encode()
	require.NoError(t, err)

	desc, val, err := DecodeSASL(body)
	require.NoError(t, err)
	assert.Equal(t, DescriptorMechanisms, desc)

	decoded := val.(*Mechanisms)
	assert.Len(t, decoded.Mechanisms, 2)
	assert.Equal(t, MechPLAIN, decoded.Mechanisms[0])
	assert.Equal(t, MechANONYMOUS, decoded.Mechanisms[1])
}

func TestInit(t *testing.T) {
	i := &Init{
		Mechanism:       MechPLAIN,
		InitialResponse: []byte("\x00user\x00pass"),
		Hostname:        "localhost",
	}
	body, err := i.Encode()
	require.NoError(t, err)

	desc, val, err := DecodeSASL(body)
	require.NoError(t, err)
	assert.Equal(t, DescriptorInit, desc)

	decoded := val.(*Init)
	assert.Equal(t, MechPLAIN, decoded.Mechanism)
	assert.Equal(t, []byte("\x00user\x00pass"), decoded.InitialResponse)
	assert.Equal(t, "localhost", decoded.Hostname)
}

func TestOutcome(t *testing.T) {
	o := &Outcome{Code: CodeOK}
	body, err := o.Encode()
	require.NoError(t, err)

	desc, val, err := DecodeSASL(body)
	require.NoError(t, err)
	assert.Equal(t, DescriptorOutcome, desc)

	decoded := val.(*Outcome)
	assert.Equal(t, CodeOK, decoded.Code)
}

func TestOutcomeFailed(t *testing.T) {
	o := &Outcome{Code: CodeAuth}
	body, err := o.Encode()
	require.NoError(t, err)

	_, val, err := DecodeSASL(body)
	require.NoError(t, err)
	assert.Equal(t, CodeAuth, val.(*Outcome).Code)
}

func TestParsePLAIN(t *testing.T) {
	authz, user, pass, err := ParsePLAIN([]byte("\x00user\x00pass"))
	require.NoError(t, err)
	assert.Equal(t, "", authz)
	assert.Equal(t, "user", user)
	assert.Equal(t, "pass", pass)
}

func TestParsePLAINEmpty(t *testing.T) {
	_, _, _, err := ParsePLAIN(nil)
	assert.Error(t, err)
}
