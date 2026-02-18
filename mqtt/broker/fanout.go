// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"runtime"
	"sync"
)

// fanOutPool is a bounded goroutine pool for async subscriber fan-out.
// Each task is a func() that calls distribute() for one published message.
// The pool owns its goroutines and stops them cleanly via Close().
type fanOutPool struct {
	tasks     chan func()
	wg        sync.WaitGroup
	mu        sync.RWMutex
	closeOnce sync.Once
	closed    bool
}

func newFanOutPool(workers int) *fanOutPool {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	p := &fanOutPool{
		// Buffer one task per worker so submitters rarely block.
		tasks: make(chan func(), workers),
	}
	p.wg.Add(workers)
	for range workers {
		go p.run()
	}
	return p
}

func (p *fanOutPool) run() {
	defer p.wg.Done()
	for fn := range p.tasks {
		fn()
	}
}

// Submit enqueues a fan-out task.
// Returns false when the pool is closed.
func (p *fanOutPool) Submit(fn func()) bool {
	if fn == nil {
		return true
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return false
	}
	p.tasks <- fn
	return true
}

// Close drains queued tasks and waits for all workers to finish.
func (p *fanOutPool) Close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.tasks)
		p.mu.Unlock()
		p.wg.Wait()
	})
}
