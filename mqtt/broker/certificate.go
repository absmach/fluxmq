// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	corebroker "github.com/absmach/fluxmq/broker"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

// DisconnectCertificateSessions revokes matching certificate bindings and
// closes their live MQTT connections. Persistent session data is retained, but
// a client must present and resolve a valid certificate before reconnecting.
func (b *Broker) DisconnectCertificateSessions(match func(corebroker.CertificateIdentity) bool) int {
	if b.auth == nil {
		return 0
	}
	clientIDs := b.auth.InvalidateCertificateSessions(match)
	disconnected := 0
	for _, clientID := range clientIDs {
		s := b.Get(clientID)
		if s == nil || !s.IsConnected() {
			continue
		}
		// Treat lifecycle revocation as graceful for Will purposes: a revoked
		// credential must not publish one final message while being removed.
		if err := s.Disconnect(true, v5.DisconnectAdministrativeAction); err == nil {
			disconnected++
		}
	}
	return disconnected
}
