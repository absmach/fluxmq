// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"time"
)

// TimeOffsetProvider exposes time-based offset lookups.
type TimeOffsetProvider interface {
	OffsetByTime(ctx context.Context, queueName string, ts time.Time) (uint64, error)
}

// SizeOffsetProvider exposes size-based retention offsets.
type SizeOffsetProvider interface {
	OffsetBySize(ctx context.Context, queueName string, retentionBytes int64) (uint64, error)
}
