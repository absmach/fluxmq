// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// These names are what a client puts on a queue command message. Renaming one
// changes what every publisher must send, with no compile error and nothing in
// the compatibility guards to notice.
//
// That happened: the reject reason was renamed from "reason" to
// "x-reject-reason" while the published client documentation still said
// "reason", so a publisher following those docs sent a reject with no reason
// recorded. The literals are duplicated here deliberately — comparing a
// constant to itself would pass through any rename.
func TestCommandPropertyNamesAreFrozen(t *testing.T) {
	assert.Equal(t, map[string]string{
		"PropCommitGroupID": "x-group-id",
		"PropCommitOffset":  "x-offset",
		"PropRejectReason":  "reason",
	}, map[string]string{
		"PropCommitGroupID": PropCommitGroupID,
		"PropCommitOffset":  PropCommitOffset,
		"PropRejectReason":  PropRejectReason,
	}, "a client-supplied command property changed; the documented name is what "+
		"publishers send, so a rename silently drops what they set")
}
