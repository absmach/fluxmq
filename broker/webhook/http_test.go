// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPSender_Send(t *testing.T) {
	tests := []struct {
		name           string
		serverResponse int
		serverDelay    time.Duration
		timeout        time.Duration
		wantErr        bool
		errContains    string
	}{
		{
			name:           "successful request",
			serverResponse: http.StatusOK,
			serverDelay:    0,
			timeout:        5 * time.Second,
			wantErr:        false,
		},
		{
			name:           "successful request with 201",
			serverResponse: http.StatusCreated,
			serverDelay:    0,
			timeout:        5 * time.Second,
			wantErr:        false,
		},
		{
			name:           "server returns 400",
			serverResponse: http.StatusBadRequest,
			serverDelay:    0,
			timeout:        5 * time.Second,
			wantErr:        true,
			errContains:    "non-2xx status: 400",
		},
		{
			name:           "server returns 500",
			serverResponse: http.StatusInternalServerError,
			serverDelay:    0,
			timeout:        5 * time.Second,
			wantErr:        true,
			errContains:    "non-2xx status: 500",
		},
		{
			name:           "timeout exceeded",
			serverResponse: http.StatusOK,
			serverDelay:    2 * time.Second,
			timeout:        100 * time.Millisecond,
			wantErr:        true,
			errContains:    "context deadline exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Verify request
				if r.Method != http.MethodPost {
					t.Errorf("expected POST method, got %s", r.Method)
				}
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
				}
				if r.Header.Get("User-Agent") != "Absmach-MQTT-Broker/1.0" {
					t.Errorf("expected User-Agent Absmach-MQTT-Broker/1.0, got %s", r.Header.Get("User-Agent"))
				}

				// Check custom headers
				if auth := r.Header.Get("Authorization"); auth != "Bearer test-token" {
					t.Errorf("expected Authorization header, got %s", auth)
				}

				// Verify body
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatalf("failed to read body: %v", err)
				}
				expected := `{"test":"payload"}`
				if string(body) != expected {
					t.Errorf("expected body %s, got %s", expected, string(body))
				}

				// Simulate delay
				if tt.serverDelay > 0 {
					time.Sleep(tt.serverDelay)
				}

				// Send response
				w.WriteHeader(tt.serverResponse)
			}))
			defer server.Close()

			// Create sender
			sender := NewHTTPSender()

			// Create context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()

			// Send request
			headers := map[string]string{
				"Authorization": "Bearer test-token",
			}
			payload := []byte(`{"test":"payload"}`)

			err := sender.Send(ctx, server.URL, headers, payload, tt.timeout)

			// Check result
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing '%s', got '%s'", tt.errContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestHTTPSender_Send_InvalidURL(t *testing.T) {
	sender := NewHTTPSender()
	ctx := context.Background()

	// Invalid URL scheme
	err := sender.Send(ctx, "invalid://url", nil, []byte("test"), 5*time.Second)
	if err == nil {
		t.Error("expected error for invalid URL, got nil")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsHelper(s, substr)))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
