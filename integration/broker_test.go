package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/dborovcanin/mqtt/broker"
	"github.com/dborovcanin/mqtt/client"
	"github.com/dborovcanin/mqtt/transport"
)

func TestPubSub_V5(t *testing.T) {
	// 1. Start Broker
	srv := broker.NewServer()
	// Use a random port
	fe, err := transport.NewTCPFrontend("localhost:0")
	if err != nil {
		t.Fatalf("Failed to create frontend: %v", err)
	}

	// Get actual port
	addr := fe.Addr().String()

	if err := srv.AddFrontend(fe); err != nil {
		t.Fatalf("Failed to add frontend: %v", err)
	}
	defer fe.Close()

	// 2. Start Client (using v3.1.1 = version 4)
	opts := client.Options{
		ClientID:   "client-v3",
		BrokerAddr: addr,
		Version:    4,
	}
	c := client.NewClient(opts)

	received := make(chan string, 1)
	c.SetMessageHandler(func(topic string, payload []byte) {
		fmt.Printf("Received: %s -> %s\n", topic, payload)
		received <- string(payload)
	})

	if err := c.Connect(); err != nil {
		t.Fatalf("Client connect error: %v", err)
	}

	// 3. Subscribe
	if err := c.Subscribe("test/topic"); err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	// Allow time for subscription propagation
	time.Sleep(100 * time.Millisecond)

	// 4. Publish
	if err := c.Publish("test/topic", []byte("hello world"), 0, false); err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	// 5. Verify
	select {
	case msg := <-received:
		if msg != "hello world" {
			t.Errorf("Expected 'hello world', got '%s'", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}
