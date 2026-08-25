// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import "context"

// v1CommandProcessor is intentionally duplicated so additions and removals
// both fail compilation. Changes require an explicit v1 compatibility review.
type v1CommandProcessor interface {
	Append(context.Context, AppendCommand) (AppendOutcome, error)
	Consume(context.Context, ConsumeCommand) (ConsumeOutcome, error)
	CommitConsume(context.Context, CommitConsumeCommand) error
	CommitOffset(context.Context, CommitOffsetCommand) error
	Ack(context.Context, AckCommand) (SettlementOutcome, error)
	Nack(context.Context, NackCommand) (SettlementOutcome, error)
	Reject(context.Context, RejectCommand) (SettlementOutcome, error)
	Claim(context.Context, ClaimCommand) (ClaimOutcome, error)
	Seek(context.Context, SeekCommand) (SeekOutcome, error)
}

var (
	_ v1CommandProcessor = CommandProcessor(nil)
	_ CommandProcessor   = v1CommandProcessor(nil)
)
