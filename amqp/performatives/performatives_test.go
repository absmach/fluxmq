// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package performatives

import (
	"testing"

	"github.com/absmach/fluxmq/amqp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func encodeDecodePerf(t *testing.T, encode func() ([]byte, error), expectedDesc uint64) any {
	t.Helper()
	body, err := encode()
	require.NoError(t, err)
	desc, perf, err := DecodePerformative(body)
	require.NoError(t, err)
	assert.Equal(t, expectedDesc, desc)
	return perf
}

func TestOpen(t *testing.T) {
	o := &Open{
		ContainerID:  "test-container",
		Hostname:     "localhost",
		MaxFrameSize: 32768,
		ChannelMax:   255,
		IdleTimeOut:  30000,
	}

	got := encodeDecodePerf(t, o.Encode, DescriptorOpen).(*Open)
	assert.Equal(t, "test-container", got.ContainerID)
	assert.Equal(t, "localhost", got.Hostname)
	assert.Equal(t, uint32(32768), got.MaxFrameSize)
	assert.Equal(t, uint16(255), got.ChannelMax)
	assert.Equal(t, uint32(30000), got.IdleTimeOut)
}

func TestOpenDefaults(t *testing.T) {
	o := &Open{ContainerID: "c1"}
	got := encodeDecodePerf(t, o.Encode, DescriptorOpen).(*Open)
	assert.Equal(t, "c1", got.ContainerID)
	assert.Equal(t, uint32(DefaultMaxFrameSize), got.MaxFrameSize)
	assert.Equal(t, uint16(65535), got.ChannelMax)
}

func TestBegin(t *testing.T) {
	ch := uint16(0)
	b := &Begin{
		RemoteChannel:  &ch,
		NextOutgoingID: 1,
		IncomingWindow: 1000,
		OutgoingWindow: 1000,
		HandleMax:      255,
	}

	got := encodeDecodePerf(t, b.Encode, DescriptorBegin).(*Begin)
	require.NotNil(t, got.RemoteChannel)
	assert.Equal(t, uint16(0), *got.RemoteChannel)
	assert.Equal(t, uint32(1), got.NextOutgoingID)
	assert.Equal(t, uint32(1000), got.IncomingWindow)
	assert.Equal(t, uint32(1000), got.OutgoingWindow)
	assert.Equal(t, uint32(255), got.HandleMax)
}

func TestBeginNoRemoteChannel(t *testing.T) {
	b := &Begin{
		NextOutgoingID: 0,
		IncomingWindow: 100,
		OutgoingWindow: 100,
	}
	got := encodeDecodePerf(t, b.Encode, DescriptorBegin).(*Begin)
	assert.Nil(t, got.RemoteChannel)
}

func TestAttachSender(t *testing.T) {
	a := &Attach{
		Name:   "link-1",
		Handle: 0,
		Role:   RoleSender,
		Source: &Source{Address: "test/topic"},
		Target: &Target{Address: "test/topic"},
	}

	got := encodeDecodePerf(t, a.Encode, DescriptorAttach).(*Attach)
	assert.Equal(t, "link-1", got.Name)
	assert.Equal(t, uint32(0), got.Handle)
	assert.Equal(t, RoleSender, got.Role)
	require.NotNil(t, got.Source)
	assert.Equal(t, "test/topic", got.Source.Address)
	require.NotNil(t, got.Target)
	assert.Equal(t, "test/topic", got.Target.Address)
}

func TestAttachReceiver(t *testing.T) {
	a := &Attach{
		Name:   "link-2",
		Handle: 1,
		Role:   RoleReceiver,
		Source: &Source{Address: "$queue/orders"},
	}

	got := encodeDecodePerf(t, a.Encode, DescriptorAttach).(*Attach)
	assert.Equal(t, RoleReceiver, got.Role)
	assert.Equal(t, "$queue/orders", got.Source.Address)
}

func TestFlow(t *testing.T) {
	h := uint32(0)
	dc := uint32(0)
	lc := uint32(100)
	f := &Flow{
		IncomingWindow: 1000,
		NextOutgoingID: 1,
		OutgoingWindow: 1000,
		Handle:         &h,
		DeliveryCount:  &dc,
		LinkCredit:     &lc,
	}

	got := encodeDecodePerf(t, f.Encode, DescriptorFlow).(*Flow)
	require.NotNil(t, got.Handle)
	assert.Equal(t, uint32(0), *got.Handle)
	require.NotNil(t, got.LinkCredit)
	assert.Equal(t, uint32(100), *got.LinkCredit)
}

func TestTransfer(t *testing.T) {
	did := uint32(0)
	mf := uint32(0)
	tr := &Transfer{
		Handle:        0,
		DeliveryID:    &did,
		DeliveryTag:   []byte{0x01},
		MessageFormat: &mf,
		Settled:       false,
	}

	got := encodeDecodePerf(t, tr.Encode, DescriptorTransfer).(*Transfer)
	assert.Equal(t, uint32(0), got.Handle)
	require.NotNil(t, got.DeliveryID)
	assert.Equal(t, uint32(0), *got.DeliveryID)
	assert.Equal(t, []byte{0x01}, got.DeliveryTag)
	assert.False(t, got.Settled)
}

func TestDisposition(t *testing.T) {
	d := &Disposition{
		Role:    RoleReceiver,
		First:   0,
		Settled: true,
		State:   &Accepted{},
	}

	got := encodeDecodePerf(t, d.Encode, DescriptorDisposition).(*Disposition)
	assert.Equal(t, RoleReceiver, got.Role)
	assert.Equal(t, uint32(0), got.First)
	assert.True(t, got.Settled)
	_, ok := got.State.(*Accepted)
	assert.True(t, ok)
}

func TestDetach(t *testing.T) {
	d := &Detach{
		Handle: 0,
		Closed: true,
		Error: &Error{
			Condition:   ErrDetachForced,
			Description: "shutting down",
		},
	}

	got := encodeDecodePerf(t, d.Encode, DescriptorDetach).(*Detach)
	assert.Equal(t, uint32(0), got.Handle)
	assert.True(t, got.Closed)
	require.NotNil(t, got.Error)
	assert.Equal(t, ErrDetachForced, got.Error.Condition)
	assert.Equal(t, "shutting down", got.Error.Description)
}

func TestEnd(t *testing.T) {
	e := &End{}
	got := encodeDecodePerf(t, e.Encode, DescriptorEnd).(*End)
	assert.Nil(t, got.Error)
}

func TestEndWithError(t *testing.T) {
	e := &End{Error: &Error{Condition: ErrInternalError}}
	got := encodeDecodePerf(t, e.Encode, DescriptorEnd).(*End)
	require.NotNil(t, got.Error)
	assert.Equal(t, ErrInternalError, got.Error.Condition)
}

func TestClose(t *testing.T) {
	c := &Close{}
	got := encodeDecodePerf(t, c.Encode, DescriptorClose).(*Close)
	assert.Nil(t, got.Error)
}

func TestCloseWithError(t *testing.T) {
	c := &Close{Error: &Error{
		Condition:   ErrConnectionForced,
		Description: "server shutdown",
	}}
	got := encodeDecodePerf(t, c.Encode, DescriptorClose).(*Close)
	require.NotNil(t, got.Error)
	assert.Equal(t, types.Symbol("amqp:connection:forced"), got.Error.Condition)
}

func TestOutcomes(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		d := &Disposition{Role: RoleReceiver, First: 0, Settled: true, State: &Accepted{}}
		got := encodeDecodePerf(t, d.Encode, DescriptorDisposition).(*Disposition)
		_, ok := got.State.(*Accepted)
		assert.True(t, ok)
	})

	t.Run("rejected", func(t *testing.T) {
		d := &Disposition{Role: RoleReceiver, First: 0, Settled: true, State: &Rejected{}}
		got := encodeDecodePerf(t, d.Encode, DescriptorDisposition).(*Disposition)
		_, ok := got.State.(*Rejected)
		assert.True(t, ok)
	})

	t.Run("released", func(t *testing.T) {
		d := &Disposition{Role: RoleReceiver, First: 0, Settled: true, State: &Released{}}
		got := encodeDecodePerf(t, d.Encode, DescriptorDisposition).(*Disposition)
		_, ok := got.State.(*Released)
		assert.True(t, ok)
	})

	t.Run("modified", func(t *testing.T) {
		d := &Disposition{Role: RoleReceiver, First: 0, State: &Modified{DeliveryFailed: true}}
		got := encodeDecodePerf(t, d.Encode, DescriptorDisposition).(*Disposition)
		m, ok := got.State.(*Modified)
		require.True(t, ok)
		assert.True(t, m.DeliveryFailed)
	})
}
