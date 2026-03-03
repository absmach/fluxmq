// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"time"

	"github.com/absmach/fluxmq/broker/events"
)

// Notifier sends webhook notifications asynchronously.
type Notifier interface {
	// Notify sends an event asynchronously (non-blocking)
	Notify(ctx context.Context, event events.Event) error

	// Close gracefully shuts down, flushing pending events
	Close() error
}

// Sender is the protocol-specific sender interface (HTTP, gRPC, etc.).
type Sender interface {
	// Send sends a webhook payload to the specified URL.
	// Returns error if the send fails.
	Send(ctx context.Context, url string, headers map[string]string, payload []byte, timeout time.Duration) error
}
