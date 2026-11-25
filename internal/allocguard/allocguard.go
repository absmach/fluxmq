// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package allocguard pins the allocation budget of hot paths in tests, so a
// change that adds a heap allocation to one fails a test instead of showing up
// later as a benchmark regression.
//
// The race detector changes allocation behaviour (sync.Pool drops entries at
// random under it), so guards skip in race builds. `make test-allocs` runs them
// without it.
package allocguard

import "testing"

// runs is how many times AllocsPerRun repeats fn. It reports the average,
// truncated, so an occasional pool miss does not fail a budget.
const runs = 1000

// Max fails t if fn allocates more than budget times per call on average.
// fn runs once before measuring, so lazily initialised state and warm pools do
// not count against it.
func Max(t testing.TB, budget float64, fn func()) {
	t.Helper()

	if raceEnabled {
		t.Skip("allocation counts are not meaningful under the race detector")
	}
	if got := testing.AllocsPerRun(runs, fn); got > budget {
		t.Errorf("%v allocations per call, budget is %v", got, budget)
	}
}
