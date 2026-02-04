// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package frames

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFrameRoundTrip(t *testing.T) {
	body := []byte{0x01, 0x02, 0x03, 0x04}
	var buf bytes.Buffer
	require.NoError(t, WriteFrame(&buf, FrameTypeAMQP, 5, body))

	f, err := ReadFrame(&buf)
	require.NoError(t, err)
	assert.Equal(t, FrameTypeAMQP, f.Type)
	assert.Equal(t, uint16(5), f.Channel)
	assert.Equal(t, body, f.Body)
}

func TestEmptyFrame(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteEmptyFrame(&buf))

	f, err := ReadFrame(&buf)
	require.NoError(t, err)
	assert.True(t, f.IsEmpty())
	assert.Equal(t, FrameTypeAMQP, f.Type)
	assert.Equal(t, uint16(0), f.Channel)
}

func TestSASLFrame(t *testing.T) {
	body := []byte{0xAA, 0xBB}
	var buf bytes.Buffer
	require.NoError(t, WriteFrame(&buf, FrameTypeSASL, 0, body))

	f, err := ReadFrame(&buf)
	require.NoError(t, err)
	assert.Equal(t, FrameTypeSASL, f.Type)
	assert.Equal(t, body, f.Body)
}

func TestProtocolHeaderAMQP(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteProtocolHeader(&buf, ProtoIDAMQP))

	protoID, err := ReadProtocolHeader(&buf)
	require.NoError(t, err)
	assert.Equal(t, ProtoIDAMQP, protoID)
}

func TestProtocolHeaderSASL(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteProtocolHeader(&buf, ProtoIDSASL))

	protoID, err := ReadProtocolHeader(&buf)
	require.NoError(t, err)
	assert.Equal(t, ProtoIDSASL, protoID)
}

func TestProtocolHeaderInvalid(t *testing.T) {
	buf := bytes.NewBuffer([]byte("MQTT0100"))
	_, err := ReadProtocolHeader(buf)
	assert.Error(t, err)
}

func TestDetectAMQP(t *testing.T) {
	assert.True(t, DetectAMQP([]byte("AMQP\x00\x01\x00\x00")))
	assert.False(t, DetectAMQP([]byte("MQTT")))
	assert.False(t, DetectAMQP([]byte("AM")))
}

func TestFrameTooSmall(t *testing.T) {
	// Frame with size < 8
	buf := bytes.NewBuffer([]byte{0, 0, 0, 4, 2, 0, 0, 0})
	_, err := ReadFrame(buf)
	assert.Error(t, err)
}
