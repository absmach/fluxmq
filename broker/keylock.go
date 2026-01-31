// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"hash/fnv"
	"sync"
)

const numKeyShards = 128

// keyLock provides per-key locking using a fixed number of sharded mutexes.
// Operations on different keys are unlikely to contend.
type keyLock struct {
	shards [numKeyShards]sync.Mutex
}

func (kl *keyLock) Lock(key string) {
	kl.shards[kl.index(key)].Lock()
}

func (kl *keyLock) Unlock(key string) {
	kl.shards[kl.index(key)].Unlock()
}

func (kl *keyLock) index(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % numKeyShards
}
