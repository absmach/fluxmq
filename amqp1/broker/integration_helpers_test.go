// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker_test

import (
	"testing"

	amqpconn "github.com/absmach/fluxmq/amqp1"
)

// readDescriptor reads a performative and returns only the descriptor and error,
// discarding channel, performative value, and payload.
func readDescriptor(t *testing.T, c *amqpconn.Connection) (uint64, error) {
	t.Helper()
	_, desc, _, _, err := c.ReadPerformative() //nolint:dogsled
	return desc, err
}

// drainPerformative reads and discards a full performative frame, returning only the error.
func drainPerformative(t *testing.T, c *amqpconn.Connection) error {
	t.Helper()
	_, _, _, _, err := c.ReadPerformative() //nolint:dogsled
	return err
}
