// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"errors"
	"fmt"

	"github.com/absmach/fluxmq/message"
)

// errEmptyEnvelope reports a wire message with no envelope in it.
var errEmptyEnvelope = errors.New("cluster message carries no envelope")

// encodeEnvelope renders an envelope for the cluster wire.
//
// Peers exchange the whole envelope rather than a payload beside a flattened
// property map. The map was lossy in both directions: it never carried
// QueueMetadata state, timestamps or retry count, nor most of TransferMetadata,
// and reading it back parsed integers with the errors discarded, so a malformed
// peer value became a zero offset indistinguishable from a real one.
func encodeEnvelope(envelope *message.Envelope) ([]byte, error) {
	if envelope == nil {
		return nil, errEmptyEnvelope
	}
	encoded, err := message.MarshalBinary(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode cluster envelope: %w", err)
	}
	return encoded, nil
}

// decodeEnvelope reads an envelope from the cluster wire. The caller owns the
// result and must release it.
func decodeEnvelope(encoded []byte) (*message.Envelope, error) {
	if len(encoded) == 0 {
		return nil, errEmptyEnvelope
	}
	envelope, err := message.UnmarshalBinary(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode cluster envelope: %w", err)
	}
	return envelope, nil
}

// QueueDelivery pairs a queue message envelope with its target local client.
// Used for batched cross-node queue delivery.
type QueueDelivery struct {
	ClientID string
	Message  *message.Envelope
}
