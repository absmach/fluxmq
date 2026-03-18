// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/reload"
)

func TestHandleReloadMethodNotAllowed(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/reload", nil)
	s.handleReload(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestHandleReloadNoManager(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/reload", nil)
	s.handleReload(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}

func TestHandleReloadSuccess(t *testing.T) {
	dir := t.TempDir()
	yamlContent := `log:
  level: debug
`
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	rm := reload.New(path, cfg,
		reload.WithLogSetup(func(_ config.LogConfig) {}),
	)

	s := &Server{reloadManager: rm}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/reload", nil)
	s.handleReload(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var result reload.ReloadResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Version != 2 {
		t.Errorf("expected version 2, got %d", result.Version)
	}
	if len(result.Applied) == 0 {
		t.Error("expected applied changes")
	}
}

func TestHandleReloadInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	yamlContent := `broker:
  max_message_size: 100
`
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	rm := reload.New(path, cfg)

	s := &Server{reloadManager: rm}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/reload", nil)
	s.handleReload(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestHandleReloadShuttingDown(t *testing.T) {
	cfg := config.Default()
	rm := reload.New("", cfg)
	rm.Shutdown()

	s := &Server{reloadManager: rm}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/reload", nil)
	s.handleReload(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", rec.Code)
	}
}
