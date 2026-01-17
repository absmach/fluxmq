// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"

	"github.com/absmach/fluxmq/queue/types"
)

// QueueInfo provides queue metadata needed by lifecycle managers.
type QueueInfo interface {
	Name() string
	Config() types.QueueConfig
}

// RaftCoordinator provides Raft operations needed by retention management.
type RaftCoordinator interface {
	IsLeader(partitionID int) bool
	ApplyRetentionDelete(ctx context.Context, partitionID int, messageIDs []string) error
}
