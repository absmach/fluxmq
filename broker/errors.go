// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "errors"

// ErrClientNotConnected is returned by protocol adapters when a queue delivery
// targets a client that no longer has a live connection.
var ErrClientNotConnected = errors.New("client not connected")

// ErrCertificateSessionCapacity prevents certificate-authenticated session
// state from growing without a configured bound.
var ErrCertificateSessionCapacity = errors.New("certificate session capacity reached")

// ErrCertificateClientIdentityConflict prevents a live certificate-bound
// client ID from being taken over by a different Atom entity.
var ErrCertificateClientIdentityConflict = errors.New("certificate client ID is already bound to another entity")

// IsErrClientNotConnected reports whether err means a queue delivery target is
// gone. Across the cluster RPC the signal is carried structurally (the
// client_not_connected proto field, re-wrapped into ErrClientNotConnected by
// the sender), so errors.Is matches it end to end.
func IsErrClientNotConnected(err error) bool {
	return errors.Is(err, ErrClientNotConnected)
}
