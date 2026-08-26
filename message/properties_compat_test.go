// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Property names are protocol surface: a client sets them on a message it sends
// and reads them on a message it receives. Renaming one silently changes what
// every publisher and subscriber must write, with no compile error anywhere and
// nothing in the compatibility guards to catch it.
//
// That happened: the reject reason was renamed from "reason" to
// "x-reject-reason" while the published client documentation still said
// "reason", so a doc-following publisher's reject carried no reason at all.
//
// The literals are duplicated here on purpose. A test that compared the
// constant to itself would pass through any rename.
func TestProtocolPropertyNamesAreFrozen(t *testing.T) {
	outbound := map[string]string{
		"PropertyClientID":            "client_id",
		"PropertyExternalID":          "external_id",
		"PropertyProtocol":            "protocol",
		"PropertyMessageID":           "message-id",
		"PropertyGroupID":             "group-id",
		"PropertyQueueName":           "queue", //nolint:goconst // the literal is the contract; using the constant would compare it to itself
		"PropertyOffset":              "offset",
		"PropertySourceTopic":         "x-source-topic",
		"PropertyStreamOffset":        "x-stream-offset",
		"PropertyStreamTimestamp":     "x-stream-timestamp",
		"PropertyWorkCommitted":       "x-work-committed-offset",
		"PropertyWorkAcked":           "x-work-acked",
		"PropertyWorkGroup":           "x-work-group",
		"PropertyTransferID":          "x-dlq-transfer-id",
		"PropertyDLQReason":           "x-dlq-reason",
		"PropertyForwardTargetQueues": "x-queue-forward-targets",
		"PropertyTraceParent":         "_flux.traceparent",
		"PropertyTraceState":          "_flux.tracestate",
		"PropertyTraceID":             "_flux.trace_id",
		"ReservedPropertyPrefix":      "_flux.",
	}
	actual := map[string]string{
		"PropertyClientID":            PropertyClientID,
		"PropertyExternalID":          PropertyExternalID,
		"PropertyProtocol":            PropertyProtocol,
		"PropertyMessageID":           PropertyMessageID,
		"PropertyGroupID":             PropertyGroupID,
		"PropertyQueueName":           PropertyQueueName,
		"PropertyOffset":              PropertyOffset,
		"PropertySourceTopic":         PropertySourceTopic,
		"PropertyStreamOffset":        PropertyStreamOffset,
		"PropertyStreamTimestamp":     PropertyStreamTimestamp,
		"PropertyWorkCommitted":       PropertyWorkCommitted,
		"PropertyWorkAcked":           PropertyWorkAcked,
		"PropertyWorkGroup":           PropertyWorkGroup,
		"PropertyTransferID":          PropertyTransferID,
		"PropertyDLQReason":           PropertyDLQReason,
		"PropertyForwardTargetQueues": PropertyForwardTargetQueues,
		"PropertyTraceParent":         PropertyTraceParent,
		"PropertyTraceState":          PropertyTraceState,
		"PropertyTraceID":             PropertyTraceID,
		"ReservedPropertyPrefix":      ReservedPropertyPrefix,
	}

	assert.Equal(t, outbound, actual,
		"a broker-owned property name changed; clients read these, so a rename "+
			"is a protocol change and needs a documentation change with it")
}
