// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package connguard

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecoverCatchesPanicAndCounts(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelError}))

	before := PanicCount()

	assert.NotPanics(t, func() {
		defer Recover(logger, "mqtt-tcp", "1.2.3.4:1883")
		panic(errors.New("boom"))
	})

	assert.Equal(t, before+1, PanicCount())
	logged := buf.String()
	assert.True(t, strings.Contains(logged, "connection_handler_panic_recovered"))
	assert.True(t, strings.Contains(logged, "mqtt-tcp"))
	assert.True(t, strings.Contains(logged, "1.2.3.4:1883"))
}

func TestRecoverNoPanicIsNoOp(t *testing.T) {
	before := PanicCount()
	func() { defer Recover(nil, "mqtt-tcp", "") }()
	assert.Equal(t, before, PanicCount())
}

func TestRecoverEmptyRemoteDoesNotPanic(t *testing.T) {
	assert.NotPanics(t, func() {
		defer Recover(nil, "amqp10", "")
		panic("oops")
	})
}
