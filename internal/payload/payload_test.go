// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package payload_test

import (
	"testing"

	"github.com/absmach/fluxmq/internal/payload"
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGet_PrefersBuffer(t *testing.T) {
	buf := core.GetBufferWithData([]byte("buffered"))
	defer buf.Release()

	assert.Equal(t, "buffered", string(payload.Get([]byte("legacy"), buf)))
	assert.Equal(t, "legacy", string(payload.Get([]byte("legacy"), nil)))
	assert.Nil(t, payload.Get(nil, nil))
}

func TestStable_CopiesBufferSurvivesRelease(t *testing.T) {
	pool := core.NewBufferPoolWithCapacity(1, 0, 0)
	buf := pool.GetWithData([]byte("remote-payload"))

	stable := payload.Stable(nil, buf)
	buf.Release()
	reused := pool.GetWithData([]byte("overwritten!!!"))
	defer reused.Release()

	assert.Equal(t, "remote-payload", string(stable), "buffer-backed payload must be copied")
}

func TestStable_PlainPayloadNotCopied(t *testing.T) {
	legacy := []byte("plain")
	got := payload.Stable(legacy, nil)
	require.NotEmpty(t, got)
	assert.Equal(t, &legacy[0], &got[0], "plain payload must not allocate a copy")
}

func TestFromBuffer_ReleasesPreviousAndTransfers(t *testing.T) {
	pool := core.NewBufferPoolWithCapacity(2, 0, 0)
	prev := pool.GetWithData([]byte("old"))
	next := pool.GetWithData([]byte("new"))

	legacy, buf := payload.FromBuffer(prev, next)
	assert.Nil(t, legacy, "legacy slice cleared")
	assert.Same(t, next, buf, "buffer ownership transferred")
	assert.Equal(t, int32(0), prev.RefCount(), "previous buffer released")

	buf.Release()
}

func TestFromBytes_EmptyReturnsNilBuffer(t *testing.T) {
	legacy, buf := payload.FromBytes(nil, nil)
	assert.Nil(t, legacy)
	assert.Nil(t, buf)

	legacy, buf = payload.FromBytes(nil, []byte("data"))
	require.NotNil(t, buf)
	assert.Nil(t, legacy)
	assert.Equal(t, "data", string(buf.Bytes()))
	buf.Release()
}

func TestReleaseBuffer_HandlesNil(t *testing.T) {
	assert.Nil(t, payload.ReleaseBuffer(nil))

	buf := core.GetBufferWithData([]byte("x"))
	assert.Nil(t, payload.ReleaseBuffer(buf))
	assert.Equal(t, int32(0), buf.RefCount())
}

func TestRetain_IncrementsRefCount(t *testing.T) {
	buf := core.GetBufferWithData([]byte("x"))
	payload.Retain(buf)
	assert.Equal(t, int32(2), buf.RefCount())
	buf.Release()
	buf.Release()

	assert.NotPanics(t, func() { payload.Retain(nil) })
}
