// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "net"

// Session represents the state of an AMQP 0.9.1 connection.
type Session struct {
	Conn net.Conn
	// TODO: Add fields for channels, etc.
}
