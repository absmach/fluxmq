// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"strconv"
	"testing"

	"github.com/absmach/fluxmq/queue/types"
)

// BenchmarkQueueConfigStoreSave measures one queue update against catalogs of
// growing size.
func BenchmarkQueueConfigStoreSave(b *testing.B) {
	for _, existing := range []int{10, 1000} {
		b.Run(strconv.Itoa(existing)+"queues", func(b *testing.B) {
			store, err := NewQueueConfigStore(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			for i := range existing {
				if err := store.Save(types.DefaultQueueConfig("q"+strconv.Itoa(i), "t/#")); err != nil {
					b.Fatal(err)
				}
			}

			cfg := types.DefaultQueueConfig("q0", "t/#")
			b.ReportAllocs()
			for b.Loop() {
				cfg.MaxDepth++
				if err := store.Save(cfg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
