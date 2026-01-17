// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"sync"

	"github.com/absmach/fluxmq/queue/types"
)

var (
	messagePool = sync.Pool{
		New: func() interface{} {
			return &types.Message{
				Properties: make(map[string]string, 8),
			}
		},
	}

	propertyMapPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]string, 8)
		},
	}
)

// getMessageFromPool retrieves a QueueMessage from the pool.
func getMessageFromPool() *types.Message {
	return messagePool.Get().(*types.Message)
}

// putMessageToPool returns a QueueMessage to the pool.
func putMessageToPool(msg *types.Message) {
	if msg == nil {
		return
	}
	// Release buffer reference if set
	msg.ReleasePayload()
	// Clear map but keep allocated capacity
	for k := range msg.Properties {
		delete(msg.Properties, k)
	}
	messagePool.Put(msg)
}

// getPropertyMap retrieves a property map from the pool.
func getPropertyMap() map[string]string {
	return propertyMapPool.Get().(map[string]string)
}

// putPropertyMap returns a property map to the pool after clearing it.
func putPropertyMap(m map[string]string) {
	if m == nil {
		return
	}
	// Clear before returning to pool
	for k := range m {
		delete(m, k)
	}
	propertyMapPool.Put(m)
}

// copyProperties copies properties from src to dst.
func copyProperties(dst, src map[string]string) {
	for k, v := range src {
		dst[k] = v
	}
}
