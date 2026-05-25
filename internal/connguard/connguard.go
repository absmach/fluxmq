// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package connguard provides shared panic-recovery helpers for per-connection
// goroutines. Each transport spawns one goroutine per accepted connection;
// a panic in any downstream codec, router, or hook would otherwise crash the
// entire broker. Recover() converts panics into structured error logs and
// increments a process-wide counter for monitoring.
package connguard

import (
	"log/slog"
	"runtime/debug"
	"sync/atomic"
)

var panicCount atomic.Uint64

// PanicCount returns the number of recovered panics observed since process
// start. Exposed for metrics scraping; a non-zero value indicates a bug
// somewhere on the connection-handling path that should be investigated.
func PanicCount() uint64 { return panicCount.Load() }

// Recover is intended to be deferred at the top of any per-connection
// goroutine. On a panic it logs the recovered value, the remote address,
// and the stack trace, then returns so the deferred connection close runs
// normally. The transport must NOT call Recover inline; defer is required
// so that the surrounding cleanup still executes.
//
// transport identifies the protocol ("mqtt-tcp", "mqtt-ws", "amqp091",
// "amqp10", "coap") for log filtering. remote is the client's address; pass
// an empty string when it is not available.
func Recover(logger *slog.Logger, transport, remote string) {
	r := recover()
	if r == nil {
		return
	}
	panicCount.Add(1)
	if logger == nil {
		logger = slog.Default()
	}
	logger.Error("connection_handler_panic_recovered",
		slog.String("transport", transport),
		slog.String("remote", remote),
		slog.Any("recover", r),
		slog.String("stack", string(debug.Stack())))
}
