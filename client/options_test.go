// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import "testing"

func TestWithProtocolPublish(t *testing.T) {
	po := buildPublishOptions([]Option{
		WithProtocol(ProtocolAMQP),
	})

	if po.Protocol != ProtocolAMQP {
		t.Fatalf("publish protocol = %q, want %q", po.Protocol, ProtocolAMQP)
	}
}

func TestWithProtocolSubscribe(t *testing.T) {
	so := buildSubscribeOptions([]Option{
		WithProtocol(ProtocolMQTT),
	})

	if so.Protocol != ProtocolMQTT {
		t.Fatalf("subscribe protocol = %q, want %q", so.Protocol, ProtocolMQTT)
	}
}
