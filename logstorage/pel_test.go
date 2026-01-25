// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPEL_NewAndClose(t *testing.T) {
	dir := t.TempDir()

	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	require.NotNil(t, pel)

	assert.Equal(t, 0, pel.Count())

	err = pel.Close()
	assert.NoError(t, err)
}

func TestPEL_AddAndGet(t *testing.T) {
	dir := t.TempDir()
	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	defer pel.Close()

	entry := PELEntry{
		Offset:        100,
		PartitionID:   0,
		ConsumerID:    "consumer-1",
		ClaimedAt:     time.Now().UnixMilli(),
		DeliveryCount: 1,
	}

	err = pel.Add(entry)
	require.NoError(t, err)

	assert.Equal(t, 1, pel.Count())

	retrieved, found := pel.Get(100)
	require.True(t, found)
	assert.Equal(t, entry.Offset, retrieved.Offset)
	assert.Equal(t, entry.ConsumerID, retrieved.ConsumerID)
	assert.Equal(t, entry.PartitionID, retrieved.PartitionID)

	_, found = pel.Get(200)
	assert.False(t, found)
}

func TestPEL_Ack(t *testing.T) {
	dir := t.TempDir()
	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	defer pel.Close()

	entry := PELEntry{
		Offset:        100,
		PartitionID:   0,
		ConsumerID:    "consumer-1",
		ClaimedAt:     time.Now().UnixMilli(),
		DeliveryCount: 1,
	}
	err = pel.Add(entry)
	require.NoError(t, err)

	err = pel.Ack(100)
	require.NoError(t, err)

	assert.Equal(t, 0, pel.Count())
	_, found := pel.Get(100)
	assert.False(t, found)

	err = pel.Ack(100)
	assert.ErrorIs(t, err, ErrPELEntryNotFound)
}

func TestPEL_Claim(t *testing.T) {
	dir := t.TempDir()
	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	defer pel.Close()

	entry := PELEntry{
		Offset:        100,
		PartitionID:   0,
		ConsumerID:    "consumer-1",
		ClaimedAt:     time.Now().UnixMilli(),
		DeliveryCount: 1,
	}
	err = pel.Add(entry)
	require.NoError(t, err)

	err = pel.Claim(100, "consumer-2")
	require.NoError(t, err)

	retrieved, found := pel.Get(100)
	require.True(t, found)
	assert.Equal(t, "consumer-2", retrieved.ConsumerID)
	assert.Equal(t, uint16(2), retrieved.DeliveryCount)

	err = pel.Claim(200, "consumer-2")
	assert.ErrorIs(t, err, ErrPELEntryNotFound)
}

func TestPEL_IncrementDelivery(t *testing.T) {
	dir := t.TempDir()
	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	defer pel.Close()

	entry := PELEntry{
		Offset:        100,
		PartitionID:   0,
		ConsumerID:    "consumer-1",
		ClaimedAt:     time.Now().UnixMilli(),
		DeliveryCount: 1,
	}
	err = pel.Add(entry)
	require.NoError(t, err)

	err = pel.IncrementDelivery(100)
	require.NoError(t, err)

	retrieved, found := pel.Get(100)
	require.True(t, found)
	assert.Equal(t, uint16(2), retrieved.DeliveryCount)

	err = pel.IncrementDelivery(200)
	assert.ErrorIs(t, err, ErrPELEntryNotFound)
}

func TestPEL_GetByConsumer(t *testing.T) {
	dir := t.TempDir()
	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	defer pel.Close()

	for i := uint64(0); i < 5; i++ {
		err = pel.Add(PELEntry{
			Offset:        i,
			ConsumerID:    "consumer-1",
			ClaimedAt:     time.Now().UnixMilli(),
			DeliveryCount: 1,
		})
		require.NoError(t, err)
	}

	for i := uint64(5); i < 8; i++ {
		err = pel.Add(PELEntry{
			Offset:        i,
			ConsumerID:    "consumer-2",
			ClaimedAt:     time.Now().UnixMilli(),
			DeliveryCount: 1,
		})
		require.NoError(t, err)
	}

	c1Entries := pel.GetByConsumer("consumer-1")
	assert.Len(t, c1Entries, 5)

	c2Entries := pel.GetByConsumer("consumer-2")
	assert.Len(t, c2Entries, 3)

	c3Entries := pel.GetByConsumer("consumer-3")
	assert.Len(t, c3Entries, 0)
}

func TestPEL_GetStealable(t *testing.T) {
	dir := t.TempDir()
	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	defer pel.Close()

	oldTime := time.Now().Add(-2 * time.Second).UnixMilli()
	for i := uint64(0); i < 3; i++ {
		err = pel.Add(PELEntry{
			Offset:        i,
			ConsumerID:    "consumer-1",
			ClaimedAt:     oldTime,
			DeliveryCount: 1,
		})
		require.NoError(t, err)
	}

	for i := uint64(3); i < 5; i++ {
		err = pel.Add(PELEntry{
			Offset:        i,
			ConsumerID:    "consumer-2",
			ClaimedAt:     time.Now().UnixMilli(),
			DeliveryCount: 1,
		})
		require.NoError(t, err)
	}

	stealable := pel.GetStealable(1*time.Second, "")
	assert.Len(t, stealable, 3)

	stealable = pel.GetStealable(1*time.Second, "consumer-1")
	assert.Len(t, stealable, 0)

	stealable = pel.GetStealable(1*time.Second, "consumer-2")
	assert.Len(t, stealable, 3)
}

func TestPEL_CountByConsumer(t *testing.T) {
	dir := t.TempDir()
	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	defer pel.Close()

	for i := uint64(0); i < 5; i++ {
		err = pel.Add(PELEntry{
			Offset:        i,
			ConsumerID:    "consumer-1",
			ClaimedAt:     time.Now().UnixMilli(),
			DeliveryCount: 1,
		})
		require.NoError(t, err)
	}

	for i := uint64(5); i < 8; i++ {
		err = pel.Add(PELEntry{
			Offset:        i,
			ConsumerID:    "consumer-2",
			ClaimedAt:     time.Now().UnixMilli(),
			DeliveryCount: 1,
		})
		require.NoError(t, err)
	}

	counts := pel.CountByConsumer()
	assert.Len(t, counts, 2)
	assert.Equal(t, 5, counts["consumer-1"])
	assert.Equal(t, 3, counts["consumer-2"])
}

func TestPEL_GetAll(t *testing.T) {
	dir := t.TempDir()
	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	defer pel.Close()

	for i := uint64(0); i < 3; i++ {
		err = pel.Add(PELEntry{
			Offset:        i,
			ConsumerID:    "consumer-1",
			ClaimedAt:     time.Now().UnixMilli(),
			DeliveryCount: 1,
		})
		require.NoError(t, err)
	}

	all := pel.GetAll()
	assert.Len(t, all, 1)
	assert.Len(t, all["consumer-1"], 3)
}

func TestPEL_MinOffset(t *testing.T) {
	dir := t.TempDir()
	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	defer pel.Close()

	assert.Equal(t, uint64(0), pel.MinOffset())

	for _, offset := range []uint64{100, 50, 200, 75} {
		err = pel.Add(PELEntry{
			Offset:        offset,
			ConsumerID:    "consumer-1",
			ClaimedAt:     time.Now().UnixMilli(),
			DeliveryCount: 1,
		})
		require.NoError(t, err)
	}

	assert.Equal(t, uint64(50), pel.MinOffset())
}

func TestPEL_Persistence(t *testing.T) {
	dir := t.TempDir()

	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)

	for i := uint64(0); i < 5; i++ {
		err = pel.Add(PELEntry{
			Offset:        i,
			ConsumerID:    "consumer-1",
			ClaimedAt:     time.Now().UnixMilli(),
			DeliveryCount: 1,
		})
		require.NoError(t, err)
	}

	err = pel.Sync()
	require.NoError(t, err)
	err = pel.Close()
	require.NoError(t, err)

	pel2, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	defer pel2.Close()

	assert.Equal(t, 5, pel2.Count())

	for i := uint64(0); i < 5; i++ {
		entry, found := pel2.Get(i)
		require.True(t, found)
		assert.Equal(t, i, entry.Offset)
		assert.Equal(t, "consumer-1", entry.ConsumerID)
	}
}

func TestPEL_Compact(t *testing.T) {
	dir := t.TempDir()

	pel, err := NewPEL(dir, "test-group")
	require.NoError(t, err)
	defer pel.Close()

	pel.compactThreshold = 100000

	for i := uint64(0); i < 10; i++ {
		err = pel.Add(PELEntry{
			Offset:        i,
			ConsumerID:    "consumer-1",
			ClaimedAt:     time.Now().UnixMilli(),
			DeliveryCount: 1,
		})
		require.NoError(t, err)
	}

	for i := uint64(0); i < 5; i++ {
		err = pel.Ack(i)
		require.NoError(t, err)
	}

	err = pel.Compact()
	require.NoError(t, err)

	assert.Equal(t, 5, pel.Count())
}

func TestConsumerGroupPELs_NewAndClose(t *testing.T) {
	dir := t.TempDir()

	cgp, err := NewConsumerGroupPELs(dir)
	require.NoError(t, err)
	require.NotNil(t, cgp)

	err = cgp.Close()
	assert.NoError(t, err)
}

func TestConsumerGroupPELs_GetOrCreate(t *testing.T) {
	dir := t.TempDir()
	cgp, err := NewConsumerGroupPELs(dir)
	require.NoError(t, err)
	defer cgp.Close()

	pel1, err := cgp.GetOrCreate("group-1")
	require.NoError(t, err)
	require.NotNil(t, pel1)

	pel2, err := cgp.GetOrCreate("group-1")
	require.NoError(t, err)
	assert.Equal(t, pel1, pel2)

	pel3, err := cgp.GetOrCreate("group-2")
	require.NoError(t, err)
	assert.NotEqual(t, pel1, pel3)
}

func TestConsumerGroupPELs_Get(t *testing.T) {
	dir := t.TempDir()
	cgp, err := NewConsumerGroupPELs(dir)
	require.NoError(t, err)
	defer cgp.Close()

	pel := cgp.Get("group-1")
	assert.Nil(t, pel)

	_, err = cgp.GetOrCreate("group-1")
	require.NoError(t, err)

	pel = cgp.Get("group-1")
	assert.NotNil(t, pel)
}

func TestConsumerGroupPELs_Delete(t *testing.T) {
	dir := t.TempDir()
	cgp, err := NewConsumerGroupPELs(dir)
	require.NoError(t, err)
	defer cgp.Close()

	pel, err := cgp.GetOrCreate("group-1")
	require.NoError(t, err)

	err = pel.Add(PELEntry{
		Offset:        1,
		ConsumerID:    "c1",
		ClaimedAt:     time.Now().UnixMilli(),
		DeliveryCount: 1,
	})
	require.NoError(t, err)

	err = cgp.Delete("group-1")
	require.NoError(t, err)

	assert.Nil(t, cgp.Get("group-1"))
}
