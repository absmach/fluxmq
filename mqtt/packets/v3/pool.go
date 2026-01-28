// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3

import (
	"sync"
)

// Publish packet pool to reduce allocations during message delivery.
var publishPool = sync.Pool{
	New: func() interface{} {
		return &Publish{}
	},
}

// AcquirePublish gets a Publish packet from the pool.
// The returned packet must be returned via ReleasePublish when done.
func AcquirePublish() *Publish {
	return publishPool.Get().(*Publish)
}

// ReleasePublish returns a Publish packet to the pool after resetting it.
// The packet must not be used after calling this function.
func ReleasePublish(p *Publish) {
	if p == nil {
		return
	}
	p.Reset()
	publishPool.Put(p)
}
