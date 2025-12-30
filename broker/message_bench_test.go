// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/core/packets"
	"github.com/absmach/mqtt/session"
	"github.com/absmach/mqtt/storage"
)

// BenchmarkMessagePublish_SingleSubscriber benchmarks publishing a message to a single subscriber
func BenchmarkMessagePublish_SingleSubscriber(b *testing.B) {
	sizes := []int{
		100,    // Small message
		1024,   // 1KB
		10240,  // 10KB
		65536,  // 64KB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_bytes", size), func(b *testing.B) {
			broker := createBenchBroker(b)
			defer broker.Close()

			// Create a subscriber
			sub := createBenchSession(b, broker, "subscriber")
			broker.subscribe(sub, "test/topic", 0, storage.SubscribeOptions{})

			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				msg := &storage.Message{
					Topic: "test/topic",
					QoS:   0,
				}
				msg.SetPayloadFromBytes(payload)
				broker.Publish(msg)
			}
		})
	}
}

// BenchmarkMessagePublish_MultipleSubscribers benchmarks publishing to multiple subscribers
func BenchmarkMessagePublish_MultipleSubscribers(b *testing.B) {
	subscriberCounts := []int{1, 10, 100, 1000}

	for _, count := range subscriberCounts {
		b.Run(fmt.Sprintf("%d_subscribers", count), func(b *testing.B) {
			broker := createBenchBroker(b)
			defer broker.Close()

			// Create subscribers
			for i := 0; i < count; i++ {
				sub := createBenchSession(b, broker, fmt.Sprintf("sub-%d", i))
				broker.subscribe(sub, "test/topic", 0, storage.SubscribeOptions{})
			}

			payload := make([]byte, 1024)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				msg := &storage.Message{
					Topic: "test/topic",
					QoS:   0,
				}
				msg.SetPayloadFromBytes(payload)
				broker.Publish(msg)
			}
		})
	}
}

// BenchmarkMessagePublish_QoS1 benchmarks QoS 1 message publishing
func BenchmarkMessagePublish_QoS1(b *testing.B) {
	broker := createBenchBroker(b)
	defer broker.Close()

	sub := createBenchSession(b, broker, "subscriber")
	broker.subscribe(sub, "test/topic", 1, storage.SubscribeOptions{})

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := &storage.Message{
			Topic: "test/topic",
			QoS:   1,
		}
		msg.SetPayloadFromBytes(payload)
		broker.Publish(msg)
	}
}

// BenchmarkMessageDistribute benchmarks the distribute function directly
func BenchmarkMessageDistribute(b *testing.B) {
	broker := createBenchBroker(b)
	defer broker.Close()

	// Create 10 subscribers
	for i := 0; i < 10; i++ {
		sub := createBenchSession(b, broker, fmt.Sprintf("sub-%d", i))
		broker.subscribe(sub, "test/topic", 0, storage.SubscribeOptions{})
	}

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := &storage.Message{
			Topic: "test/topic",
			QoS:   0,
		}
		msg.SetPayloadFromBytes(payload)
		broker.distribute(msg)
		msg.ReleasePayload()
	}
}

// BenchmarkBufferPooling benchmarks the buffer pool performance
func BenchmarkBufferPooling(b *testing.B) {
	pool := core.NewBufferPool()
	payload := make([]byte, 1024)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf := pool.GetWithData(payload)
		buf.Release()
	}
}

// BenchmarkBufferPooling_Parallel benchmarks parallel buffer pool usage
func BenchmarkBufferPooling_Parallel(b *testing.B) {
	pool := core.NewBufferPool()
	payload := make([]byte, 1024)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.GetWithData(payload)
			buf.Release()
		}
	})
}

// BenchmarkMessageCopy_Legacy simulates the old copy-based approach
func BenchmarkMessageCopy_Legacy(b *testing.B) {
	sizes := []int{100, 1024, 10240, 65536}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_bytes", size), func(b *testing.B) {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Simulate old approach: copy payload for each operation
				msg1 := make([]byte, len(payload))
				copy(msg1, payload)
				msg2 := make([]byte, len(payload))
				copy(msg2, payload)
				msg3 := make([]byte, len(payload))
				copy(msg3, payload)

				// Prevent optimization
				_ = msg1
				_ = msg2
				_ = msg3
			}
		})
	}
}

// BenchmarkMessageCopy_ZeroCopy benchmarks the new zero-copy approach
func BenchmarkMessageCopy_ZeroCopy(b *testing.B) {
	sizes := []int{100, 1024, 10240, 65536}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_bytes", size), func(b *testing.B) {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			pool := core.NewBufferPool()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Simulate zero-copy: create buffer, retain for sharing
				buf := pool.GetWithData(payload)
				buf.Retain() // Share with subscriber 1
				buf.Retain() // Share with subscriber 2
				buf.Release() // Original release
				buf.Release() // Subscriber 1 release
				buf.Release() // Subscriber 2 release
			}
		})
	}
}

// Helper functions

func createBenchBroker(b *testing.B) *Broker {
	b.Helper()

	broker := NewBroker(nil, nil, nil, nil, nil, nil, nil)
	return broker
}

func createBenchSession(b *testing.B, broker *Broker, clientID string) *session.Session {
	b.Helper()

	opts := session.Options{
		CleanStart:     true,
		KeepAlive:      60 * time.Second,
		ReceiveMaximum: 65535,
	}

	s, _, err := broker.CreateSession(clientID, 5, opts)
	if err != nil {
		b.Fatalf("Failed to create session: %v", err)
	}

	// Create a mock connection
	conn := &mockBenchConn{clientID: clientID}
	if err := s.Connect(conn); err != nil {
		b.Fatalf("Failed to connect session: %v", err)
	}

	return s
}

// benchAddr implements net.Addr for benchmarks
type benchAddr struct{}

func (b *benchAddr) Network() string { return "tcp" }
func (b *benchAddr) String() string  { return "127.0.0.1:1883" }

// mockBenchConn is a minimal mock connection for benchmarks
type mockBenchConn struct {
	net.Conn
	clientID string
}

func (m *mockBenchConn) ReadPacket() (packets.ControlPacket, error) {
	return nil, nil
}

func (m *mockBenchConn) WritePacket(pkt packets.ControlPacket) error {
	return nil
}

func (m *mockBenchConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (m *mockBenchConn) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func (m *mockBenchConn) Close() error {
	return nil
}

func (m *mockBenchConn) RemoteAddr() net.Addr {
	return &benchAddr{}
}

func (m *mockBenchConn) LocalAddr() net.Addr {
	return &benchAddr{}
}

func (m *mockBenchConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockBenchConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockBenchConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (m *mockBenchConn) SetKeepAlive(d time.Duration) error {
	return nil
}

func (m *mockBenchConn) SetOnDisconnect(fn func(graceful bool)) {}

func (m *mockBenchConn) Touch() {}
