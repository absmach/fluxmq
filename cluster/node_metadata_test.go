// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster_test

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeMetadata_ReportedForEveryPeer covers the round trip: each node
// registers its own build and start time under /nodes/, and every other node
// reads them back through Nodes(). A node reporting only itself would still
// satisfy a single-node test, so this asserts on the peers.
func TestNodeMetadata_ReportedForEveryPeer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tc := testutil.NewTestCluster(t, 3)
	t.Cleanup(tc.Stop)

	require.NoError(t, tc.Start())
	require.NoError(t, tc.WaitForClusterReady(30*time.Second))

	// The metadata read is serializable, so a follower may briefly serve a view
	// that predates a peer's registration.
	for _, node := range tc.Nodes {
		require.Eventuallyf(t, func() bool {
			nodes := node.Cluster.Nodes()
			if len(nodes) != 3 {
				return false
			}
			for _, n := range nodes {
				if n.Version != fluxmq.Version || n.Uptime <= 0 {
					return false
				}
			}
			return true
		}, 15*time.Second, 200*time.Millisecond,
			"node %s never saw a version and uptime for all 3 members: %+v", node.ID, node.Cluster.Nodes())
	}
}

// TestNoopClusterNodes_ReportsVersion keeps the single-node path reporting the
// same fields as the clustered one, since /cluster and the health endpoint
// render both through NodeInfo.
func TestNoopClusterNodes_ReportsVersion(t *testing.T) {
	nodes := cluster.NewNoopCluster("node-1").Nodes()

	require.Len(t, nodes, 1)
	assert.Equal(t, fluxmq.Version, nodes[0].Version)
	assert.Positive(t, nodes[0].Uptime)
}
