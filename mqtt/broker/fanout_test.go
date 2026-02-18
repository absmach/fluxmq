// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFanOutPoolSubmitAfterClose(t *testing.T) {
	p := newFanOutPool(1)
	var ran atomic.Bool

	require.True(t, p.Submit(func() {
		ran.Store(true)
	}))

	require.Eventually(t, ran.Load, 500*time.Millisecond, 10*time.Millisecond)

	p.Close()
	require.False(t, p.Submit(func() {}))
}

func TestFanOutPoolCloseWithConcurrentSubmitDoesNotPanic(t *testing.T) {
	p := newFanOutPool(4)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				if !p.Submit(func() {}) {
					return
				}
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	p.Close()
	wg.Wait()
}

func TestFanOutPoolCloseDrainsQueuedTasks(t *testing.T) {
	p := newFanOutPool(1)

	started := make(chan struct{})
	release := make(chan struct{})
	var secondRan atomic.Bool

	require.True(t, p.Submit(func() {
		close(started)
		<-release
	}))
	require.True(t, p.Submit(func() {
		secondRan.Store(true)
	}))

	<-started

	closed := make(chan struct{})
	go func() {
		p.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned before in-flight task completion")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	require.Eventually(t, func() bool {
		select {
		case <-closed:
			return true
		default:
			return false
		}
	}, 500*time.Millisecond, 10*time.Millisecond)
	require.True(t, secondRan.Load())
}
