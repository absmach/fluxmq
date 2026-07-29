// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"testing"

	"github.com/absmach/fluxmq/amqp/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// encodeContentHeader builds the payload of a content header frame advertising
// bodySize bytes of body.
func encodeContentHeader(t *testing.T, bodySize uint64) []byte {
	t.Helper()

	header := &codec.ContentHeader{
		ClassID:  codec.ClassBasic,
		BodySize: bodySize,
	}
	var buf bytes.Buffer
	require.NoError(t, header.WriteContentHeader(&buf))
	return buf.Bytes()
}

// TestContentHeaderDoesNotReserveAdvertisedBody guards the memory an
// unauthenticated publisher can tie up: a content header is a promise, not a
// delivery, so accepting one must not reserve the whole advertised body before
// any of it arrives.
func TestContentHeaderDoesNotReserveAdvertisedBody(t *testing.T) {
	const advertised = 8 * 1024 * 1024

	conn, _ := newPolicyTestConnection(t, nil)
	ch := newChannel(conn, 1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: "q"}

	ch.handleHeaderFrame(&codec.Frame{Payload: encodeContentHeader(t, advertised)})

	require.NotNil(t, ch.pendingHeader, "a well-formed header must be accepted")
	assert.Equal(t, uint64(advertised), ch.pendingBodySize)
	assert.Empty(t, ch.pendingBody)
	assert.LessOrEqual(t, cap(ch.pendingBody), maxInitialBodyCapacity,
		"reservation must track bytes received, not bytes promised")
}

func TestContentHeaderReservesSmallBodiesExactly(t *testing.T) {
	const advertised = 512

	conn, _ := newPolicyTestConnection(t, nil)
	ch := newChannel(conn, 1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: "q"}

	ch.handleHeaderFrame(&codec.Frame{Payload: encodeContentHeader(t, advertised)})

	assert.Equal(t, advertised, cap(ch.pendingBody),
		"bodies within the cap keep their single up-front allocation")
}

// TestBodyFramesGrowIncrementally checks the assembled message is still exactly
// what was sent once the frames do arrive.
func TestBodyFramesGrowIncrementally(t *testing.T) {
	const chunk = 32 * 1024
	const chunks = 4

	conn, _ := newPolicyTestConnection(t, nil)
	ch := newChannel(conn, 1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: "q"}
	ch.handleHeaderFrame(&codec.Frame{Payload: encodeContentHeader(t, chunk*chunks)})

	for i := range chunks {
		payload := make([]byte, chunk)
		for j := range payload {
			payload[j] = byte('a' + i)
		}
		ch.handleBodyFrame(&codec.Frame{Payload: payload})

		if i < chunks-1 {
			require.Len(t, ch.pendingBody, chunk*(i+1), "body must accumulate frame by frame")
		}
	}

	// The final frame completes the publish, which resets the pending state.
	assert.Nil(t, ch.pendingBody, "a completed publish must release its buffer")
	assert.Nil(t, ch.pendingHeader)
}

// TestMalformedContentHeaderClosesChannel covers the other half: a header that
// cannot be parsed leaves the channel unable to frame what follows, so it must
// be closed rather than logged and ignored.
func TestMalformedContentHeaderClosesChannel(t *testing.T) {
	conn, buf := newPolicyTestConnection(t, nil)
	ch := newChannel(conn, 1)
	ch.pendingMethod = &codec.BasicPublish{RoutingKey: "q"}

	ch.handleHeaderFrame(&codec.Frame{Payload: []byte{0x00}})

	assert.Nil(t, ch.pendingHeader, "a malformed header must not be accepted")
	assert.Nil(t, ch.pendingMethod, "pending publish state must be reset")

	require.NoError(t, conn.writer.Flush())
	closeMethod := decodeSingleChannelClose(t, buf)
	assert.Equal(t, uint16(codec.FrameError), closeMethod.ReplyCode)
}
