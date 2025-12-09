package integration

import (
	"testing"
	"time"

	"github.com/dborovcanin/mqtt/broker"
	"github.com/dborovcanin/mqtt/client"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/transport"
)

func HelperStartBroker(t *testing.T) (*broker.Server, string) {
	srv := broker.NewServer()
	fe, err := transport.NewTCPFrontend("localhost:0")
	if err != nil {
		t.Fatalf("Failed to create frontend: %v", err)
	}
	addr := fe.Addr().String()
	if err := srv.AddFrontend(fe); err != nil {
		t.Fatalf("Failed to add frontend: %v", err)
	}
	return srv, addr
}

func TestRetainedMessages(t *testing.T) {
	srv, addr := HelperStartBroker(t)
	defer srv.Close()

	// 1. Publisher publishes retained message
	pub := client.NewClient(client.Options{
		ClientID:   "pub",
		BrokerAddr: addr,
		Version:    4,
	})
	if err := pub.Connect(); err != nil {
		t.Fatalf("Pub connect failed: %v", err)
	}

	topic := "test/retained"
	payload := []byte("persistent data")
	if err := pub.Publish(topic, payload, 1, true); err != nil {
		t.Fatalf("Pub publish failed: %v", err)
	}
	// Give time to process
	time.Sleep(100 * time.Millisecond)

	// 2. Subscriber connects and subscribes
	sub := client.NewClient(client.Options{
		ClientID:   "sub",
		BrokerAddr: addr,
		Version:    4,
	})

	received := make(chan string, 1)
	sub.SetMessageHandler(func(t string, p []byte) {
		received <- string(p)
	})

	if err := sub.Connect(); err != nil {
		t.Fatalf("Sub connect failed: %v", err)
	}

	if err := sub.Subscribe(topic); err != nil {
		t.Fatalf("Sub subscribe failed: %v", err)
	}

	// 3. Verify message received
	select {
	case msg := <-received:
		if msg != string(payload) {
			t.Errorf("Expected '%s', got '%s'", string(payload), msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for retained message")
	}

	// 4. Clear retained message
	if err := pub.Publish(topic, []byte{}, 1, true); err != nil {
		t.Fatalf("Pub clear retained failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// 5. New Subscriber should NOT receive it
	sub2 := client.NewClient(client.Options{
		ClientID:   "sub2",
		BrokerAddr: addr,
		Version:    4,
	})
	received2 := make(chan string, 1)
	sub2.SetMessageHandler(func(t string, p []byte) {
		received2 <- string(p)
	})

	if err := sub2.Connect(); err != nil {
		t.Fatalf("Sub2 connect failed: %v", err)
	}
	if err := sub2.Subscribe(topic); err != nil {
		t.Fatalf("Sub2 subscribe failed: %v", err)
	}

	select {
	case msg := <-received2:
		t.Fatalf("Received cleared retained message: %s", msg)
	case <-time.After(500 * time.Millisecond):
		// Expected
	}
}

func TestWillMessage(t *testing.T) {
	srv, addr := HelperStartBroker(t)
	defer srv.Close()

	topic := "test/will"
	willPayload := []byte("I died")

	// 1. Subscriber monitoring will topic
	sub := client.NewClient(client.Options{
		ClientID:   "observer",
		BrokerAddr: addr,
		Version:    4,
	})
	received := make(chan string, 1)
	sub.SetMessageHandler(func(t string, p []byte) {
		if t == topic {
			received <- string(p)
		}
	})
	if err := sub.Connect(); err != nil {
		t.Fatal(err)
	}
	if err := sub.Subscribe(topic); err != nil {
		t.Fatal(err)
	}

	// 2. Client connects with Will
	victim := client.NewClient(client.Options{
		ClientID:   "victim",
		BrokerAddr: addr,
		Version:    4,
		Will: &store.WillMessage{
			Topic:   topic,
			Payload: willPayload,
			QoS:     1,
			Retain:  false,
		},
	})

	// Manually connect to access conn and close it
	// But NewClient Connect stores conn in private field.
	// However, we can use Connect() then we need a way to close it forcefully.
	// The client helper doesn't expose Close() either.
	// But if we let the variable go out of scope / garbage collected... no that's not prompt.
	// We need to close the underlying connection.
	// Or we can modify client to expose Close() or Conn().
	// Wait, verification plan said "close connection without DISCONNECT packet".
	// If I modify client to have Close(), it might send DISCONNECT if implemented politely.
	// But client.go doesn't implement Disconnect packet sending in any method currently.
	// The `readLoop` handles errors and exits.
	// So if I just close the TCP connection, it's an ungraceful disconnect from Broker's perspective (Read error).

	// I'll add Close() to client which simply closes the net.Conn.
	// Since client helper doesn't have Disconnect packet logic yet, this is perfect for Will testing.

	if err := victim.Connect(); err != nil {
		t.Fatal(err)
	}

	// 3. Simulating crash (ungraceful disconnect)
	victim.Close()

	// 4. Verify Will message received
	select {
	case msg := <-received:
		if msg != string(willPayload) {
			t.Errorf("Expected '%s', got '%s'", string(willPayload), msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for Will message")
	}
}
