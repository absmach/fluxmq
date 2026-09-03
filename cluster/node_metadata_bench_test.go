// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster_test

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/testutil"
	"github.com/stretchr/testify/require"
)

// BenchmarkNodes measures the readiness-probe path end to end. Reporting a
// build version and an uptime per member is served from the watch-fed cache,
// so it costs a map lookup rather than the range read per call it started as.
//
// The number this reports is dominated by something else: Nodes() asks
// IsLeader, which is a linearizable election read, and that alone measured
// 286.7us/480 allocs of a 302.5us/494 alloc call on a local 3-node cluster.
// Read a change here as a change to that round trip unless the metadata path
// is what moved.
func BenchmarkNodes(b *testing.B) {
	tc := testutil.NewTestCluster(b, 3)
	b.Cleanup(tc.Stop)

	require.NoError(b, tc.Start())
	require.NoError(b, tc.WaitForClusterReady(30*time.Second))

	node := tc.Nodes[0]

	// Measure the warm path: every peer registered and its metadata watched in.
	require.Eventually(b, func() bool {
		nodes := node.Cluster.Nodes()
		if len(nodes) != 3 {
			return false
		}
		for _, n := range nodes {
			if n.Version == "" {
				return false
			}
		}

		return true
	}, 15*time.Second, 200*time.Millisecond)

	b.ReportAllocs()

	for b.Loop() {
		_ = node.Cluster.Nodes()
	}
}
