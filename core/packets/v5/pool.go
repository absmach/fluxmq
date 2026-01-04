// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import (
	"sync"
)

// Publish packet pool to reduce allocations during message delivery
var publishPool = sync.Pool{
	New: func() interface{} {
		return &Publish{
			Properties: &PublishProperties{},
		}
	},
}

// AcquirePublish gets a Publish packet from the pool.
// The returned packet must be returned via ReleasePublish when done.
func AcquirePublish() *Publish {
	p := publishPool.Get().(*Publish)
	// Properties are pre-allocated, restore if nil
	if p.Properties == nil {
		p.Properties = &PublishProperties{}
	}
	return p
}

// ReleasePublish returns a Publish packet to the pool after resetting it.
// The packet must not be used after calling this function.
func ReleasePublish(p *Publish) {
	if p == nil {
		return
	}
	// Keep the Properties pointer to avoid reallocation
	props := p.Properties

	// Reset the packet (this will nil out Properties)
	p.Reset()

	// Reset and restore properties without allocating
	if props != nil {
		ResetPublishProperties(props)
		p.Properties = props
	} else {
		// First time through pool, allocate once
		p.Properties = &PublishProperties{}
	}

	publishPool.Put(p)
}

// ResetPublishProperties clears all fields in PublishProperties for reuse.
func ResetPublishProperties(pp *PublishProperties) {
	pp.PayloadFormat = nil
	pp.MessageExpiry = nil
	pp.TopicAlias = nil
	pp.ResponseTopic = ""
	pp.CorrelationData = nil
	pp.User = nil
	pp.SubscriptionID = nil
	pp.ContentType = ""
}
