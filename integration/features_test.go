package integration

import (
	"context"
	"testing"
	"time"

	"github.com/dborovcanin/mqtt/broker"
	"github.com/dborovcanin/mqtt/client"
	"github.com/dborovcanin/mqtt/pkg/server/tcp"
	"github.com/dborovcanin/mqtt/store"
)

func TestRetainedMessages(t *testing.T) {
	// Setup broker and server
	b := broker.NewBroker()
	defer b.Close()

	serverCfg := tcp.Config{
		Address:         "localhost:0",
		ShutdownTimeout: 1 * time.Second,
	}
	server := tcp.New(serverCfg, b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Listen(ctx)
	time.Sleep(100 * time.Millisecond)

	addr := server.Addr().String()

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
	// Setup broker and server
	b := broker.NewBroker()
	defer b.Close()

	serverCfg := tcp.Config{
		Address:         "localhost:0",
		ShutdownTimeout: 1 * time.Second,
	}
	server := tcp.New(serverCfg, b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Listen(ctx)
	time.Sleep(100 * time.Millisecond)

	addr := server.Addr().String()

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
