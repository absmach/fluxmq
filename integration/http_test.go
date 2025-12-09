package integration

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/dborovcanin/mqtt/adapter"
	"github.com/dborovcanin/mqtt/broker"
	"github.com/dborovcanin/mqtt/client"
	"github.com/dborovcanin/mqtt/transport"
)

func TestHTTPAdapter_Publish(t *testing.T) {
	// 1. Start Broker
	srv := broker.NewServer()

	// 2. Start TCP Frontend for subscriber
	tcpFe, err := transport.NewTCPFrontend("localhost:0")
	if err != nil {
		t.Fatalf("Failed to create TCP frontend: %v", err)
	}
	defer tcpFe.Close()
	if err := srv.AddFrontend(tcpFe); err != nil {
		t.Fatalf("Failed to add TCP frontend: %v", err)
	}

	// 3. Start HTTP Adapter
	httpFe := adapter.NewHTTPAdapter("localhost:8889")
	defer httpFe.Close()
	// Start in background (srv.AddFrontend does this, but Serve is blocking, wrapper handles it?)
	// srv.AddFrontend calls Serve in goroutine.
	if err := srv.AddFrontend(httpFe); err != nil {
		t.Fatalf("Failed to add HTTP frontend: %v", err)
	}

	// Allow server to start
	time.Sleep(100 * time.Millisecond)

	// 4. Create a subscriber client (using v3.1.1 = version 4)
	subOpts := client.Options{
		ClientID:   "http-subscriber",
		BrokerAddr: tcpFe.Addr().String(),
		Version:    4,
	}
	subClient := client.NewClient(subOpts)

	received := make(chan string, 1)
	subClient.SetMessageHandler(func(topic string, payload []byte) {
		if topic == "http/ingest" {
			received <- string(payload)
		}
	})

	if err := subClient.Connect(); err != nil {
		t.Fatalf("Subscriber failed to connect: %v", err)
	}

	if err := subClient.Subscribe("http/ingest"); err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// 5. Send HTTP POST
	msg := "Hello from HTTP"
	resp, err := http.Post("http://localhost:8889/publish?topic=http/ingest", "text/plain", bytes.NewBufferString(msg))
	if err != nil {
		t.Fatalf("HTTP Post failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", resp.StatusCode)
	}

	// 6. Verify receipt
	select {
	case payload := <-received:
		if payload != msg {
			t.Errorf("Expected payload %q, got %q", msg, payload)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for message")
	}
}
