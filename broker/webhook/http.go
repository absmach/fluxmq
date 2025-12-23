// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"
)

// HTTPSender implements the Sender interface for HTTP webhooks.
type HTTPSender struct {
	client *http.Client
}

// NewHTTPSender creates a new HTTP webhook sender.
func NewHTTPSender() *HTTPSender {
	return &HTTPSender{
		client: &http.Client{
			Timeout: 30 * time.Second, // Default max timeout
		},
	}
}

// Send sends an HTTP POST request with the webhook payload.
func (s *HTTPSender) Send(ctx context.Context, url string, headers map[string]string, payload []byte, timeout time.Duration) error {
	// Create request with context
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Absmach-MQTT-Broker/1.0")
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	// Send request
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned non-2xx status: %d", resp.StatusCode)
	}

	return nil
}
