// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"sync"

	"github.com/absmach/fluxmq/storage"
)

// Pool for subscription slices to reduce allocations in Match().
var subscriptionSlicePool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate with reasonable capacity for most use cases
		// This avoids reallocation for topics with moderate subscriber counts
		s := make([]*storage.Subscription, 0, 64)
		return &s
	},
}

// AcquireSubscriptionSlice gets a subscription slice from the pool.
// The returned slice must be returned via ReleaseSubscriptionSlice when done.
func AcquireSubscriptionSlice() *[]*storage.Subscription {
	return subscriptionSlicePool.Get().(*[]*storage.Subscription)
}

// ReleaseSubscriptionSlice returns a subscription slice to the pool after resetting it.
// The slice must not be used after calling this function.
func ReleaseSubscriptionSlice(s *[]*storage.Subscription) {
	if s == nil {
		return
	}
	// Clear the slice but keep capacity for reuse
	*s = (*s)[:0]
	subscriptionSlicePool.Put(s)
}
