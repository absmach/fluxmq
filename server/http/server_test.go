// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package http

import (
	"net/http/httptest"
	"testing"
)

func TestParseAuthorizationToken(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   string
	}{
		{name: "empty", header: "", want: ""},
		{name: "bare token", header: "secret", want: "secret"},
		{name: "bearer token", header: "Bearer secret", want: "secret"},
		{name: "client token", header: "Client secret", want: "secret"},
		{name: "basic token", header: "Basic dXNlcjpwYXNz", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseAuthorizationToken(tc.header)
			if got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

func TestAuthFromRequestBasicAuth(t *testing.T) {
	req := httptest.NewRequest("POST", "/publish", nil)
	req.SetBasicAuth("client-id", "client-secret")
	req.Header.Set("X-FluxMQ-Username", "header-user")

	clientID, username, password, ok := authFromRequest(req)
	if !ok {
		t.Fatal("expected basic auth to be accepted")
	}
	if username != "client-id" || password != "client-secret" {
		t.Fatalf("expected basic credentials, got username=%q password=%q", username, password)
	}
	if clientID == "" {
		t.Fatal("expected non-empty clientID")
	}
}

func TestAuthFromRequestHeaderPair(t *testing.T) {
	req := httptest.NewRequest("POST", "/publish", nil)
	req.Header.Set("X-FluxMQ-Username", "client-id")
	req.Header.Set("Authorization", "Client client-secret")
	req.Header.Set("X-FluxMQ-Client-ID", "http-client")

	clientID, username, password, ok := authFromRequest(req)
	if !ok {
		t.Fatal("expected header pair auth to be accepted")
	}
	if username != "client-id" || password != "client-secret" {
		t.Fatalf("unexpected credentials username=%q password=%q", username, password)
	}
	if clientID != "http-client" {
		t.Fatalf("expected clientID http-client, got %q", clientID)
	}
}

func TestAuthFromRequestHeaderPairMissingValues(t *testing.T) {
	cases := []struct {
		name   string
		header map[string]string
	}{
		{
			name: "missing username",
			header: map[string]string{
				"Authorization": "Client client-secret",
			},
		},
		{
			name: "missing authorization",
			header: map[string]string{
				"X-FluxMQ-Username": "client-id",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/publish", nil)
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}

			_, _, _, ok := authFromRequest(req)
			if ok {
				t.Fatal("expected credentials to be rejected")
			}
		})
	}
}

func TestParseQoS(t *testing.T) {
	req := httptest.NewRequest("POST", "/m/domain/c/channel?qos=2", nil)
	qos, err := parseQoS(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if qos != 2 {
		t.Fatalf("expected qos=2, got %d", qos)
	}

	req = httptest.NewRequest("POST", "/m/domain/c/channel?qos=3", nil)
	if _, err := parseQoS(req); err == nil {
		t.Fatal("expected error for invalid qos")
	}
}

func TestParseRetain(t *testing.T) {
	req := httptest.NewRequest("POST", "/m/domain/c/channel?retain=true", nil)
	retain, err := parseRetain(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !retain {
		t.Fatal("expected retain=true")
	}

	req = httptest.NewRequest("POST", "/m/domain/c/channel?retain=invalid", nil)
	if _, err := parseRetain(req); err == nil {
		t.Fatal("expected error for invalid retain value")
	}
}

func TestHandleLegacyPublishPathGuards(t *testing.T) {
	srv := &Server{}

	// Unsupported path should be 404.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/invalid/path", nil)
	srv.handleLegacyPublish(rec, req)
	if rec.Code != 404 {
		t.Fatalf("expected 404 for invalid path, got %d", rec.Code)
	}

	// Non-POST method should be 405.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/m/domain/c/channel", nil)
	srv.handleLegacyPublish(rec, req)
	if rec.Code != 405 {
		t.Fatalf("expected 405 for invalid method, got %d", rec.Code)
	}
}

func TestExtractDomainIDFromTopic(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		ok    bool
		want  string
	}{
		{name: "valid topic", topic: "m/domain-1/c/channel-1/test", ok: true, want: "domain-1"},
		{name: "valid topic leading slash", topic: "/m/domain-2/c/channel-2", ok: true, want: "domain-2"},
		{name: "missing channel prefix", topic: "m/domain-1/x/channel-1", ok: false},
		{name: "missing domain", topic: "m//c/channel-1", ok: false},
		{name: "missing channel", topic: "m/domain-1/c/", ok: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractDomainIDFromTopic(tc.topic)
			if ok != tc.ok {
				t.Fatalf("expected ok=%v, got %v", tc.ok, ok)
			}
			if got != tc.want {
				t.Fatalf("expected domain=%q, got %q", tc.want, got)
			}
		})
	}
}

func TestAuthForTopicAuthorizationOnlyFallback(t *testing.T) {
	req := httptest.NewRequest("POST", "/m/domain-1/c/channel-1/test", nil)
	req.Header.Set("Authorization", "Client secret")

	clientID, username, password, ok := authForTopic(req, "m/domain-1/c/channel-1/test")
	if !ok {
		t.Fatal("expected authorization-only fallback to be accepted")
	}
	if username != "domain-1" || password != "secret" {
		t.Fatalf("unexpected credentials username=%q password=%q", username, password)
	}
	if clientID == "" {
		t.Fatal("expected non-empty clientID")
	}
}
