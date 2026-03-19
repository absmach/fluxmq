// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestSubscriptionsListEndpointDefaultsToConnected(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp listSubscriptionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Subscriptions) != 2 {
		t.Fatalf("expected 2 connected subscriptions, got %d", len(resp.Subscriptions))
	}
	if resp.Subscriptions[0].Filter != "alpha/one" {
		t.Fatalf("expected first filter alpha/one, got %q", resp.Subscriptions[0].Filter)
	}
	if resp.Subscriptions[1].Filter != "devices/+/events" {
		t.Fatalf("expected second filter devices/+/events, got %q", resp.Subscriptions[1].Filter)
	}
}

func TestSubscriptionsListEndpointStateAllIncludesDisconnected(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?state=all", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp listSubscriptionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(resp.Subscriptions) != 3 {
		t.Fatalf("expected 3 subscriptions, got %d", len(resp.Subscriptions))
	}
}

func TestSubscriptionsListEndpointPagination(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?state=all&limit=2", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("page 1: expected 200, got %d", rec.Code)
	}

	var page1 listSubscriptionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&page1); err != nil {
		t.Fatalf("page 1: decode: %v", err)
	}
	if len(page1.Subscriptions) != 2 {
		t.Fatalf("page 1: expected 2 subscriptions, got %d", len(page1.Subscriptions))
	}
	if page1.NextPageToken == "" {
		t.Fatal("page 1: expected next_page_token")
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?state=all&limit=2&page_token="+url.QueryEscape(page1.NextPageToken), nil)
	rec = httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("page 2: expected 200, got %d", rec.Code)
	}

	var page2 listSubscriptionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&page2); err != nil {
		t.Fatalf("page 2: decode: %v", err)
	}
	if len(page2.Subscriptions) != 1 {
		t.Fatalf("page 2: expected 1 subscription, got %d", len(page2.Subscriptions))
	}
}

func TestSubscriptionClientsEndpointSupportsEscapedFilter(t *testing.T) {
	srv := newTestAPIServer(t)

	filter := "devices/+/events"
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/subscriptions/"+url.PathEscape(filter)+"/clients",
		nil,
	)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp listSubscriptionClientsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if resp.Filter != filter {
		t.Fatalf("expected filter %q, got %q", filter, resp.Filter)
	}
	if len(resp.Clients) != 1 {
		t.Fatalf("expected 1 client, got %d", len(resp.Clients))
	}
	if resp.Clients[0].ClientID != "tenant/a/client-1" {
		t.Fatalf("expected tenant/a/client-1, got %q", resp.Clients[0].ClientID)
	}
}

func TestSubscriptionClientsEndpointDefaultsToConnected(t *testing.T) {
	srv := newTestAPIServer(t)

	filter := "bravo/one"
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/subscriptions/"+url.PathEscape(filter)+"/clients",
		nil,
	)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp listSubscriptionClientsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Clients) != 0 {
		t.Fatalf("expected 0 connected clients, got %d", len(resp.Clients))
	}
}

func TestSubscriptionsListEndpointPrefix(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?state=all&prefix=alpha", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp listSubscriptionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Subscriptions) != 1 {
		t.Fatalf("expected 1 subscription with prefix alpha, got %d", len(resp.Subscriptions))
	}
	if resp.Subscriptions[0].Filter != "alpha/one" {
		t.Fatalf("expected alpha/one, got %q", resp.Subscriptions[0].Filter)
	}
}

func TestSubscriptionClientsEndpointPrefix(t *testing.T) {
	srv := newTestAPIServer(t)

	filter := "devices/+/events"
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/subscriptions/"+url.PathEscape(filter)+"/clients?prefix=tenant",
		nil,
	)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp listSubscriptionClientsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Clients) != 1 {
		t.Fatalf("expected 1 client with prefix tenant, got %d", len(resp.Clients))
	}
}

func TestSubscriptionClientsEndpointStateAll(t *testing.T) {
	srv := newTestAPIServer(t)

	filter := "bravo/one"
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/subscriptions/"+url.PathEscape(filter)+"/clients?state=all",
		nil,
	)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp listSubscriptionClientsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Clients) != 1 {
		t.Fatalf("expected 1 client with state=all, got %d", len(resp.Clients))
	}
	if resp.Clients[0].ClientID != "bravo-offline" {
		t.Fatalf("expected bravo-offline, got %q", resp.Clients[0].ClientID)
	}
}

func TestSubscriptionsEndpointRejectsInvalidState(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?state=broken", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestSubscriptionsEndpointRejectsInvalidLimit(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions?limit=abc", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestSubscriptionsEndpointRejectsPost(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/subscriptions", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestSubscriptionsDetailPathWithoutClientsReturnsNotFound(t *testing.T) {
	srv := newTestAPIServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/subscriptions/alpha%2Fone", nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestSubscriptionsNilBrokerReturnsServiceUnavailable(t *testing.T) {
	srv := New(Config{}, nil, nil, nil, nil, nil, nil, nil)

	tests := []string{
		"/api/v1/subscriptions",
		"/api/v1/subscriptions/alpha%2Fone/clients",
	}

	for _, path := range tests {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		srv.httpServer.Handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s: expected 503, got %d", path, rec.Code)
		}
	}
}
