// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPartition(t *testing.T) {
	partition := NewPartition(5)

	assert.NotNil(t, partition)
	assert.Equal(t, 5, partition.ID())
	assert.False(t, partition.IsAssigned())
	assert.Equal(t, "", partition.AssignedTo())
}

func TestPartition_AssignTo(t *testing.T) {
	partition := NewPartition(0)

	partition.AssignTo("consumer-1")

	assert.True(t, partition.IsAssigned())
	assert.Equal(t, "consumer-1", partition.AssignedTo())
}

func TestPartition_Unassign(t *testing.T) {
	partition := NewPartition(0)
	partition.AssignTo("consumer-1")

	assert.True(t, partition.IsAssigned())

	partition.Unassign()

	assert.False(t, partition.IsAssigned())
	assert.Equal(t, "", partition.AssignedTo())
}

func TestPartition_ReassignToDifferentConsumer(t *testing.T) {
	partition := NewPartition(0)

	partition.AssignTo("consumer-1")
	assert.Equal(t, "consumer-1", partition.AssignedTo())

	partition.AssignTo("consumer-2")
	assert.Equal(t, "consumer-2", partition.AssignedTo())
	assert.True(t, partition.IsAssigned())
}

func TestPartition_ID(t *testing.T) {
	tests := []int{0, 1, 5, 10, 100, 999}

	for _, id := range tests {
		partition := NewPartition(id)
		assert.Equal(t, id, partition.ID())
	}
}

func TestPartition_IsAssigned(t *testing.T) {
	partition := NewPartition(0)

	// Initially not assigned
	assert.False(t, partition.IsAssigned())

	// After assignment
	partition.AssignTo("consumer-1")
	assert.True(t, partition.IsAssigned())

	// After unassignment
	partition.Unassign()
	assert.False(t, partition.IsAssigned())

	// After reassignment
	partition.AssignTo("consumer-2")
	assert.True(t, partition.IsAssigned())
}
