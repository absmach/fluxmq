// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"log/slog"
	"strings"
)

type raftLogWriter struct {
	logger *slog.Logger
}

func (w *raftLogWriter) Write(p []byte) (int, error) {
	if detail := strings.TrimSpace(string(p)); detail != "" {
		w.logger.Warn("queue raft diagnostic", slog.String("detail", detail))
	}
	return len(p), nil
}
