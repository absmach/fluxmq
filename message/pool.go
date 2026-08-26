// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import "sync"

var envelopePool = sync.Pool{
	New: func() any {
		return &Envelope{Version: Version1}
	},
}

// Acquire gets a clean Version1 envelope from the broker pool.
func Acquire() *Envelope {
	envelope := envelopePool.Get().(*Envelope)
	envelope.Version = Version1
	return envelope
}

// Release drops the payload reference, clears all metadata, and returns the
// envelope to the broker pool. The envelope must not be used afterwards.
func Release(envelope *Envelope) {
	if envelope == nil {
		return
	}
	envelope.reset()
	envelopePool.Put(envelope)
}

// reset clears an envelope while preserving the current schema version.
func (e *Envelope) reset() {
	if e == nil {
		return
	}
	e.ReleasePayload()
	e.Version = Version1
	e.Topic = ""
	e.PublisherMeta = PublisherMetadata{}
	e.BrokerMeta = BrokerMetadata{}
}
