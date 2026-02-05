// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import "strings"

// WritePolicy controls how non-leader nodes handle queue writes when Raft is enabled.
type WritePolicy string

const (
	WritePolicyLocal   WritePolicy = "local"   // Append locally (no Raft redirect)
	WritePolicyReject  WritePolicy = "reject"  // Reject writes on non-leaders
	WritePolicyForward WritePolicy = "forward" // Forward writes to the Raft leader
)

// DistributionMode controls how queue messages reach consumers across nodes.
type DistributionMode string

const (
	DistributionForward   DistributionMode = "forward"   // Forward publishes to nodes with consumers
	DistributionReplicate DistributionMode = "replicate" // Rely on Raft to replicate queue logs
)

func normalizeWritePolicy(policy WritePolicy) WritePolicy {
	switch WritePolicy(strings.ToLower(string(policy))) {
	case WritePolicyLocal, WritePolicyReject, WritePolicyForward:
		return WritePolicy(strings.ToLower(string(policy)))
	default:
		return WritePolicyLocal
	}
}

func normalizeDistributionMode(mode DistributionMode) DistributionMode {
	switch DistributionMode(strings.ToLower(string(mode))) {
	case DistributionForward, DistributionReplicate:
		return DistributionMode(strings.ToLower(string(mode)))
	default:
		return DistributionForward
	}
}
