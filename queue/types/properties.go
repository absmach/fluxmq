// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"errors"
	"fmt"
	"strconv"
)

// Inbound queue command properties: fields a client sets on a message it sends
// to the broker to say what the command should do. The client owns their values
// and the broker reads them.
//
// The outbound direction lives in message/properties.go, which holds the fields
// the broker stamps on a message it delivers. The two are deliberately separate
// namespaces with different literals: consolidating them would conflate a
// client-supplied request field with a broker-owned delivery field, and a
// client could then forge the latter.
const (
	// Queue commit headers/properties.
	PropCommitGroupID = "x-group-id"
	PropCommitOffset  = "x-offset"

	// Queue reject metadata.
	//
	// Unprefixed, unlike its siblings above. That inconsistency is deliberate
	// now: the name is documented for clients (docs/content/docs/clients/mqtt.md)
	// and renaming it silently drops the reason from every publisher that
	// followed those docs. Changing it needs a documentation change and a
	// migration note, not a tidy-up.
	PropRejectReason = "reason"
)

// Settlement names the delivery a client is acknowledging.
type Settlement struct {
	GroupID string
	Offset  uint64
}

var (
	// ErrSettlementGroupRequired reports a settlement that names no consumer
	// group. A queue fans out to every group independently, so there is no
	// default to fall back on.
	ErrSettlementGroupRequired = errors.New("queue settlement requires " + PropCommitGroupID)

	// ErrSettlementOffsetRequired reports a settlement that names no offset.
	ErrSettlementOffsetRequired = errors.New("queue settlement requires " + PropCommitOffset)
)

// SettlementFromProperties reads the delivery a settlement command names.
//
// It reads the inbound command namespace rather than the broker's outbound
// delivery properties. Those are reserved names: a protocol boundary strips
// them from client input precisely so a publisher cannot forge broker-owned
// state, which also means a client can never send them back.
//
// Offset 0 is a real offset, so an absent offset is an error rather than a
// zero, and a malformed one is reported rather than settling the head of the
// queue.
func SettlementFromProperties(properties map[string]string) (Settlement, error) {
	groupID := properties[PropCommitGroupID]
	if groupID == "" {
		return Settlement{}, ErrSettlementGroupRequired
	}

	raw, present := properties[PropCommitOffset]
	if !present || raw == "" {
		return Settlement{}, ErrSettlementOffsetRequired
	}
	offset, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return Settlement{}, fmt.Errorf("queue settlement %s %q: %w", PropCommitOffset, raw, err)
	}

	return Settlement{GroupID: groupID, Offset: offset}, nil
}
