// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/absmach/fluxmq/reload"
)

func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.reloadManager == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(reloadErrorResponse{ //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
			Error: "reload not configured",
		})
		return
	}

	result, err := s.reloadManager.Reload(r.Context())
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, reload.ErrShuttingDown) {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(reloadErrorResponse{ //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
			Error:  err.Error(),
			Result: result,
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result) //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
}

type reloadErrorResponse struct {
	Error  string               `json:"error"`
	Result *reload.ReloadResult `json:"result,omitempty"`
}
