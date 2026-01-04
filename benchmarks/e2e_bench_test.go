// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package benchmarks

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// BenchmarkConnectionEstablishment measures connection throughput
func BenchmarkConnectionEstablishment(b *testing.B) {
	server := startTestBroker(b)
	defer server.Stop()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		clientID := fmt.Sprintf("bench-client-%d", i)
		client := createMQTTClient(b, server.Addr(), clientID)

		if token := client.Connect(); token.Wait() && token.Error() != nil {
			b.Fatalf("Failed to connect: %v", token.Error())
		}

		client.Disconnect(0)
	}
}

// BenchmarkConnectionEstablishment_Parallel measures concurrent connection throughput
func BenchmarkConnectionEstablishment_Parallel(b *testing.B) {
	server := startTestBroker(b)
	defer server.Stop()

	var counter atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := counter.Add(1)
			clientID := fmt.Sprintf("bench-client-%d", id)
			client := createMQTTClient(b, server.Addr(), clientID)

			if token := client.Connect(); token.Wait() && token.Error() != nil {
				b.Errorf("Failed to connect: %v", token.Error())
				continue
			}

			client.Disconnect(0)
		}
	})
}

// BenchmarkConcurrentClients measures steady-state performance with N clients
func BenchmarkConcurrentClients(b *testing.B) {
	clientCounts := []int{100, 1000, 5000, 10000}

	for _, count := range clientCounts {
		b.Run(fmt.Sprintf("%d_clients", count), func(b *testing.B) {
			server := startTestBroker(b)
			defer server.Stop()

			// Connect N clients
			clients := make([]paho.Client, count)
			for i := 0; i < count; i++ {
				clientID := fmt.Sprintf("bench-client-%d", i)
				clients[i] = createMQTTClient(b, server.Addr(), clientID)

				if token := clients[i].Connect(); token.Wait() && token.Error() != nil {
					b.Fatalf("Failed to connect client %d: %v", i, token.Error())
				}
			}

			// Subscribe all clients to test topic
			for i := 0; i < count; i++ {
				if token := clients[i].Subscribe("bench/test", 0, nil); token.Wait() && token.Error() != nil {
					b.Fatalf("Failed to subscribe: %v", token.Error())
				}
			}

			payload := make([]byte, 1024)

			b.ResetTimer()
			b.ReportAllocs()

			// Publish messages
			for i := 0; i < b.N; i++ {
				if token := clients[0].Publish("bench/test", 0, false, payload); token.Wait() && token.Error() != nil {
					b.Fatalf("Failed to publish: %v", token.Error())
				}
			}

			b.StopTimer()

			// Cleanup
			for _, client := range clients {
				client.Disconnect(0)
			}
		})
	}
}

// BenchmarkMessageThroughput_EndToEnd measures end-to-end message throughput
func BenchmarkMessageThroughput_EndToEnd(b *testing.B) {
	payloadSizes := []int{100, 1024, 10240, 65536}

	for _, size := range payloadSizes {
		b.Run(fmt.Sprintf("%d_bytes", size), func(b *testing.B) {
			server := startTestBroker(b)
			defer server.Stop()

			publisher := createMQTTClient(b, server.Addr(), "publisher")
			subscriber := createMQTTClient(b, server.Addr(), "subscriber")

			if token := publisher.Connect(); token.Wait() && token.Error() != nil {
				b.Fatalf("Failed to connect publisher: %v", token.Error())
			}
			defer publisher.Disconnect(0)

			if token := subscriber.Connect(); token.Wait() && token.Error() != nil {
				b.Fatalf("Failed to connect subscriber: %v", token.Error())
			}
			defer subscriber.Disconnect(0)

			receivedCh := make(chan struct{}, b.N)
			subscriber.Subscribe("bench/test", 0, func(client paho.Client, msg paho.Message) {
				receivedCh <- struct{}{}
			})

			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			go func() {
				for i := 0; i < b.N; i++ {
					<-receivedCh
				}
			}()

			for i := 0; i < b.N; i++ {
				if token := publisher.Publish("bench/test", 0, false, payload); token.Wait() && token.Error() != nil {
					b.Fatalf("Failed to publish: %v", token.Error())
				}
			}
		})
	}
}

// BenchmarkMessageThroughput_QoS measures throughput at different QoS levels
func BenchmarkMessageThroughput_QoS(b *testing.B) {
	qosLevels := []byte{0, 1, 2}

	for _, qos := range qosLevels {
		b.Run(fmt.Sprintf("QoS%d", qos), func(b *testing.B) {
			server := startTestBroker(b)
			defer server.Stop()

			publisher := createMQTTClient(b, server.Addr(), "publisher")
			subscriber := createMQTTClient(b, server.Addr(), "subscriber")

			if token := publisher.Connect(); token.Wait() && token.Error() != nil {
				b.Fatalf("Failed to connect publisher: %v", token.Error())
			}
			defer publisher.Disconnect(0)

			if token := subscriber.Connect(); token.Wait() && token.Error() != nil {
				b.Fatalf("Failed to connect subscriber: %v", token.Error())
			}
			defer subscriber.Disconnect(0)

			receivedCh := make(chan struct{}, b.N)
			subscriber.Subscribe("bench/test", qos, func(client paho.Client, msg paho.Message) {
				receivedCh <- struct{}{}
			})

			payload := make([]byte, 1024)

			b.ResetTimer()
			b.ReportAllocs()

			go func() {
				for i := 0; i < b.N; i++ {
					<-receivedCh
				}
			}()

			for i := 0; i < b.N; i++ {
				if token := publisher.Publish("bench/test", qos, false, payload); token.Wait() && token.Error() != nil {
					b.Fatalf("Failed to publish: %v", token.Error())
				}
			}
		})
	}
}

// BenchmarkFanOut measures 1:N message distribution
func BenchmarkFanOut(b *testing.B) {
	fanoutCounts := []int{10, 100, 500, 1000}

	for _, count := range fanoutCounts {
		b.Run(fmt.Sprintf("1_to_%d", count), func(b *testing.B) {
			server := startTestBroker(b)
			defer server.Stop()

			publisher := createMQTTClient(b, server.Addr(), "publisher")
			if token := publisher.Connect(); token.Wait() && token.Error() != nil {
				b.Fatalf("Failed to connect publisher: %v", token.Error())
			}
			defer publisher.Disconnect(0)

			// Create N subscribers
			subscribers := make([]paho.Client, count)
			var wg sync.WaitGroup
			wg.Add(count * b.N)

			for i := 0; i < count; i++ {
				clientID := fmt.Sprintf("subscriber-%d", i)
				subscribers[i] = createMQTTClient(b, server.Addr(), clientID)

				if token := subscribers[i].Connect(); token.Wait() && token.Error() != nil {
					b.Fatalf("Failed to connect subscriber %d: %v", i, token.Error())
				}

				subscribers[i].Subscribe("bench/fanout", 0, func(client paho.Client, msg paho.Message) {
					wg.Done()
				})
			}

			payload := make([]byte, 256)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if token := publisher.Publish("bench/fanout", 0, false, payload); token.Wait() && token.Error() != nil {
					b.Fatalf("Failed to publish: %v", token.Error())
				}
			}

			// Wait for all subscribers to receive
			wg.Wait()

			b.StopTimer()

			for _, sub := range subscribers {
				sub.Disconnect(0)
			}
		})
	}
}

// BenchmarkRetainedMessages measures retained message performance
func BenchmarkRetainedMessages(b *testing.B) {
	server := startTestBroker(b)
	defer server.Stop()

	publisher := createMQTTClient(b, server.Addr(), "publisher")
	if token := publisher.Connect(); token.Wait() && token.Error() != nil {
		b.Fatalf("Failed to connect publisher: %v", token.Error())
	}
	defer publisher.Disconnect(0)

	payload := make([]byte, 512)

	b.ResetTimer()
	b.ReportAllocs()

	// Publish retained messages to different topics
	for i := 0; i < b.N; i++ {
		topic := fmt.Sprintf("bench/retained/%d", i%100)
		if token := publisher.Publish(topic, 0, true, payload); token.Wait() && token.Error() != nil {
			b.Fatalf("Failed to publish: %v", token.Error())
		}
	}
}

// BenchmarkWildcardSubscriptions measures wildcard routing performance
func BenchmarkWildcardSubscriptions(b *testing.B) {
	server := startTestBroker(b)
	defer server.Stop()

	publisher := createMQTTClient(b, server.Addr(), "publisher")
	subscriber := createMQTTClient(b, server.Addr(), "subscriber")

	if token := publisher.Connect(); token.Wait() && token.Error() != nil {
		b.Fatalf("Failed to connect publisher: %v", token.Error())
	}
	defer publisher.Disconnect(0)

	if token := subscriber.Connect(); token.Wait() && token.Error() != nil {
		b.Fatalf("Failed to connect subscriber: %v", token.Error())
	}
	defer subscriber.Disconnect(0)

	// Subscribe to wildcards
	wildcards := []string{
		"sensor/+/temperature",
		"sensor/#",
		"device/+/status",
	}

	for _, pattern := range wildcards {
		if token := subscriber.Subscribe(pattern, 0, nil); token.Wait() && token.Error() != nil {
			b.Fatalf("Failed to subscribe to %s: %v", pattern, token.Error())
		}
	}

	payload := make([]byte, 256)
	topics := []string{
		"sensor/room1/temperature",
		"sensor/room2/humidity",
		"device/dev1/status",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		topic := topics[i%len(topics)]
		if token := publisher.Publish(topic, 0, false, payload); token.Wait() && token.Error() != nil {
			b.Fatalf("Failed to publish: %v", token.Error())
		}
	}
}

// BenchmarkSessionPersistence measures persistent session overhead
func BenchmarkSessionPersistence(b *testing.B) {
	server := startTestBroker(b)
	defer server.Stop()

	clientID := "persistent-client"
	payload := make([]byte, 1024)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Connect with clean session = false
		opts := paho.NewClientOptions()
		opts.AddBroker(fmt.Sprintf("tcp://%s", server.Addr()))
		opts.SetClientID(clientID)
		opts.SetCleanSession(false)
		opts.SetAutoReconnect(false)

		client := paho.NewClient(opts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			b.Fatalf("Failed to connect: %v", token.Error())
		}

		// Publish some messages
		for j := 0; j < 10; j++ {
			if token := client.Publish("bench/test", 1, false, payload); token.Wait() && token.Error() != nil {
				b.Fatalf("Failed to publish: %v", token.Error())
			}
		}

		client.Disconnect(250)
	}
}

// BenchmarkKeepAlive measures keep-alive ping/pong overhead
func BenchmarkKeepAlive(b *testing.B) {
	server := startTestBroker(b)
	defer server.Stop()

	// Connect with short keep-alive
	opts := paho.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s", server.Addr()))
	opts.SetClientID("keepalive-bench")
	opts.SetKeepAlive(1 * time.Second)
	opts.SetAutoReconnect(false)

	client := paho.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		b.Fatalf("Failed to connect: %v", token.Error())
	}
	defer client.Disconnect(0)

	b.ResetTimer()
	b.ReportAllocs()

	// Keep connection alive for duration
	time.Sleep(time.Duration(b.N) * time.Second)
}

// Helper functions

func startTestBroker(tb testing.TB) *TestServer {
	tb.Helper()

	// This would start an actual broker instance
	// For now, returning a mock that needs implementation
	server := &TestServer{}
	// TODO: Implement actual broker startup
	// server.Start()

	return server
}

func createMQTTClient(tb testing.TB, addr, clientID string) paho.Client {
	tb.Helper()

	opts := paho.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s", addr))
	opts.SetClientID(clientID)
	opts.SetAutoReconnect(false)
	opts.SetConnectTimeout(5 * time.Second)
	opts.SetWriteTimeout(5 * time.Second)

	return paho.NewClient(opts)
}

// TestServer wraps broker for testing
type TestServer struct {
	// TODO: Add actual broker instance
	addr string
}

func (s *TestServer) Addr() string {
	// TODO: Return actual address
	return "localhost:1883"
}

func (s *TestServer) Stop() {
	// TODO: Implement shutdown
}
