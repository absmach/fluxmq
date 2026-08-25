// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

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
	PropRejectReason = "x-reject-reason"
)
