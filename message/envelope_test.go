// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/absmach/fluxmq/payload"
)

const testPropertyValue = "value"

func TestEnvelopeJSONRequiresVersion1(t *testing.T) {
	original := New("devices/1", []byte("payload"))
	original.Broker.Source = SourceMetadata{ClientID: "client", Protocol: ProtocolMQTT}
	original.User.Properties = map[string]string{"content": "json"}
	defer Release(original)

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded Envelope
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	defer Release(&decoded)
	if decoded.Version != Version1 || decoded.Topic != original.Topic || string(decoded.PayloadBytes()) != "payload" {
		t.Fatalf("unexpected decoded envelope: %#v", decoded)
	}

	for _, raw := range []string{
		`{"topic":"devices/1","payload":"cGF5bG9hZA=="}`,
		`{"version":2,"topic":"devices/1"}`,
	} {
		var unsupported Envelope
		if err := json.Unmarshal([]byte(raw), &unsupported); !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("unmarshal %s error = %v", raw, err)
		}
	}
}

func TestEnvelopeCloneSharesOnlyImmutablePayload(t *testing.T) {
	original := New("devices/1", []byte("payload"))
	original.User.Headers = map[string][]byte{"trace": []byte("one")}
	original.User.Properties = map[string]string{"key": testPropertyValue}
	original.Broker.Queue.Stream = &StreamMetadata{Offset: 7}
	clone := original.Clone()

	if refs := original.Payload.RefCount(); refs != 2 {
		t.Fatalf("payload references = %d, want 2", refs)
	}
	clone.User.Headers["trace"][0] = 'X'
	clone.User.Properties["key"] = "changed"
	clone.Broker.Queue.Stream.Offset = 8
	if string(original.User.Headers["trace"]) != "one" || original.User.Properties["key"] != testPropertyValue || original.Broker.Queue.Stream.Offset != 7 {
		t.Fatal("clone aliases mutable metadata")
	}

	Release(clone)
	if refs := original.Payload.RefCount(); refs != 1 {
		t.Fatalf("payload references after clone release = %d, want 1", refs)
	}
	Release(original)
}

func TestEnvelopePoolReset(t *testing.T) {
	envelope := Acquire()
	envelope.Topic = "devices/1"
	envelope.Payload = payload.FromBytes([]byte("payload"))
	envelope.User.Properties = map[string]string{"key": testPropertyValue}
	Release(envelope)

	reused := Acquire()
	defer Release(reused)
	if reused.Version != Version1 || reused.Topic != "" || reused.Payload != nil || reused.User.Properties != nil {
		t.Fatalf("pooled envelope was not reset: %#v", reused)
	}
}

func TestPropertyProjectionTrustBoundary(t *testing.T) {
	envelope := New("devices/1", nil)
	defer Release(envelope)
	envelope.User.Properties = map[string]string{
		"user":             "visible",
		PropertyExternalID: "forged",
		PropertyTraceID:    "forged",
	}
	envelope.Broker.Source = SourceMetadata{ClientID: "client", ExternalID: "subject", Protocol: ProtocolMQTT}
	envelope.Broker.Queue = QueueMetadata{MessageID: "message", Name: "queue", Offset: 3}
	envelope.Broker.Trace.TraceID = "trusted"

	public := ProjectProperties(envelope, PublicProjection)
	if public["user"] != "visible" || public[PropertyMessageID] != "message" {
		t.Fatalf("public projection lost delivery metadata: %#v", public)
	}
	if _, ok := public[PropertyExternalID]; ok {
		t.Fatalf("public projection leaked source metadata: %#v", public)
	}
	if _, ok := public[PropertyTraceID]; ok {
		t.Fatalf("public projection leaked trace metadata: %#v", public)
	}

	trusted := ProjectProperties(envelope, TrustedServiceProjection)
	if trusted[PropertyExternalID] != "subject" || trusted[PropertyTraceID] != "trusted" {
		t.Fatalf("trusted projection = %#v", trusted)
	}
}

func BenchmarkEnvelopeClone(b *testing.B) {
	envelope := New("devices/1", make([]byte, 1024))
	defer Release(envelope)
	b.ReportAllocs()
	for range b.N {
		clone := envelope.Clone()
		Release(clone)
	}
}
