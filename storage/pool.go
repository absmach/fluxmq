// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"sync"
	"time"
)

// Message pool to reduce allocations during message distribution.
var messagePool = sync.Pool{
	New: func() interface{} {
		return &Message{}
	},
}

// AcquireMessage gets a Message from the pool.
// The returned message must be returned via ReleaseMessage when done.
func AcquireMessage() *Message {
	return messagePool.Get().(*Message)
}

// ReleaseMessage returns a Message to the pool after resetting it.
// The message must not be used after calling this function.
func ReleaseMessage(m *Message) {
	if m == nil {
		return
	}
	m.Reset()
	messagePool.Put(m)
}

// Reset clears all fields in the message for reuse.
// This is critical for pool safety - ensures no data leaks between uses.
func (m *Message) Reset() {
	// Release payload buffer if exists
	if m.PayloadBuf != nil {
		m.PayloadBuf.Release()
		m.PayloadBuf = nil
	}

	// Clear time fields
	m.Expiry = time.Time{}
	m.PublishTime = time.Time{}

	// Clear byte slices (nil them, don't reuse to avoid keeping large buffers)
	m.Payload = nil
	m.CorrelationData = nil
	m.SubscriptionIDs = nil

	// Clear string fields
	m.Topic = ""
	m.ContentType = ""
	m.ResponseTopic = ""

	// Clear maps (nil them to allow GC of map memory)
	m.Properties = nil
	m.UserProperties = nil

	// Clear pointers
	m.MessageExpiry = nil
	m.PayloadFormat = nil

	// Clear primitive fields
	m.PacketID = 0
	m.QoS = 0
	m.Retain = false
}
