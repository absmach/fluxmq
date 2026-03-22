// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"fmt"
	"net"
	"testing"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
)

// BenchmarkMessagePublish_SingleSubscriber benchmarks publishing a message to a single subscriber.
func BenchmarkMessagePublish_SingleSubscriber(b *testing.B) {
	sizes := []int{
		100,   // Small message
		1024,  // 1KB
		10240, // 10KB
		65536, // 64KB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_bytes", size), func(b *testing.B) {
			broker := createBenchBroker(b)
			defer broker.Close()

			// Create a subscriber
			sub := createBenchSession(b, broker, "subscriber")
			broker.subscribe(sub, "test/topic", 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup

			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				msg := &storage.Message{
					Topic: "test/topic",
					QoS:   0,
				}
				msg.SetPayloadFromBytes(payload)
				broker.Publish(msg) //nolint:errcheck // best-effort
			}
		})
	}
}

// BenchmarkMessagePublish_MultipleSubscribers benchmarks publishing to multiple subscribers.
func BenchmarkMessagePublish_MultipleSubscribers(b *testing.B) {
	subscriberCounts := []int{1, 10, 100, 1000}

	for _, count := range subscriberCounts {
		b.Run(fmt.Sprintf("%d_subscribers", count), func(b *testing.B) {
			broker := createBenchBroker(b)
			defer broker.Close()

			// Create subscribers
			for i := 0; i < count; i++ {
				sub := createBenchSession(b, broker, fmt.Sprintf("sub-%d", i))
				broker.subscribe(sub, "test/topic", 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
			}

			payload := make([]byte, 1024)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				msg := &storage.Message{
					Topic: "test/topic",
					QoS:   0,
				}
				msg.SetPayloadFromBytes(payload)
				broker.Publish(msg) //nolint:errcheck // best-effort
			}
		})
	}
}

// BenchmarkMessagePublish_QoS1 benchmarks QoS 1 message publishing.
func BenchmarkMessagePublish_QoS1(b *testing.B) {
	broker := createBenchBroker(b)
	defer broker.Close()

	sub := createBenchSession(b, broker, "subscriber")
	broker.subscribe(sub, "test/topic", 1, storage.SubscribeOptions{}) //nolint:errcheck // test setup

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ReportAllocs()

	for b.Loop() {
		msg := &storage.Message{
			Topic: "test/topic",
			QoS:   1,
		}
		msg.SetPayloadFromBytes(payload)
		broker.Publish(msg) //nolint:errcheck // best-effort
	}
}

// BenchmarkMessagePublish_QoS2 benchmarks QoS 2 message publishing.
func BenchmarkMessagePublish_QoS2(b *testing.B) {
	broker := createBenchBroker(b)
	defer broker.Close()

	sub := createBenchSession(b, broker, "subscriber")
	broker.subscribe(sub, "test/topic", 2, storage.SubscribeOptions{}) //nolint:errcheck // test setup

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ReportAllocs()

	for b.Loop() {
		msg := &storage.Message{
			Topic: "test/topic",
			QoS:   2,
		}
		msg.SetPayloadFromBytes(payload)
		broker.Publish(msg) //nolint:errcheck // best-effort
	}
}

// BenchmarkMessagePublish_SharedSubscription benchmarks shared subscription routing.
func BenchmarkMessagePublish_SharedSubscription(b *testing.B) {
	subscriberCounts := []int{2, 5, 10}

	for _, count := range subscriberCounts {
		b.Run(fmt.Sprintf("%d_subscribers", count), func(b *testing.B) {
			broker := createBenchBroker(b)
			defer broker.Close()

			// Create shared subscribers
			for i := 0; i < count; i++ {
				sub := createBenchSession(b, broker, fmt.Sprintf("subscriber-%d", i))
				broker.subscribe(sub, "$share/group1/test/topic", 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
			}

			payload := make([]byte, 1024)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				msg := &storage.Message{
					Topic: "test/topic",
					QoS:   0,
				}
				msg.SetPayloadFromBytes(payload)
				broker.Publish(msg) //nolint:errcheck // best-effort
			}
		})
	}
}

// BenchmarkMessagePublish_MixedSizes benchmarks realistic mixed message sizes.
func BenchmarkMessagePublish_MixedSizes(b *testing.B) {
	broker := createBenchBroker(b)
	defer broker.Close()

	// Create 10 subscribers
	for i := 0; i < 10; i++ {
		sub := createBenchSession(b, broker, fmt.Sprintf("sub-%d", i))
		broker.subscribe(sub, "test/topic", 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
	}

	// Mix of message sizes representing realistic workload:
	// 70% small (100-500 bytes), 20% medium (1-5KB), 10% large (10-64KB)
	sizes := []int{
		100, 200, 300, 400, 500, 100, 200, // 70% small
		1024, 2048, // 20% medium
		10240, // 10% large
	}

	payloads := make([][]byte, len(sizes))
	for i, size := range sizes {
		payloads[i] = make([]byte, size)
		for j := range payloads[i] {
			payloads[i][j] = byte(j % 256)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		payload := payloads[i%len(payloads)]
		msg := &storage.Message{
			Topic: "test/topic",
			QoS:   0,
		}
		msg.SetPayloadFromBytes(payload)
		broker.Publish(msg) //nolint:errcheck // best-effort
	}
}

// BenchmarkMessagePublish_FanOut benchmarks 1:N fanout pattern.
func BenchmarkMessagePublish_FanOut(b *testing.B) {
	fanoutSizes := []int{10, 100, 500, 1000}

	for _, count := range fanoutSizes {
		b.Run(fmt.Sprintf("1_to_%d", count), func(b *testing.B) {
			broker := createBenchBroker(b)
			defer broker.Close()

			// Create N subscribers
			for i := 0; i < count; i++ {
				sub := createBenchSession(b, broker, fmt.Sprintf("sub-%d", i))
				broker.subscribe(sub, "sensor/data", 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
			}

			payload := make([]byte, 256) // Typical sensor data size
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				msg := &storage.Message{
					Topic: "sensor/data",
					QoS:   0,
				}
				msg.SetPayloadFromBytes(payload)
				broker.Publish(msg) //nolint:errcheck // best-effort
			}
		})
	}
}

// BenchmarkMessagePublish_TopicVariety benchmarks with different topics.
func BenchmarkMessagePublish_TopicVariety(b *testing.B) {
	broker := createBenchBroker(b)
	defer broker.Close()

	// Create subscribers for different topics
	topics := []string{
		"sensor/temperature",
		"sensor/humidity",
		"device/status",
		"alerts/critical",
		"metrics/cpu",
	}

	for _, topic := range topics {
		for i := 0; i < 5; i++ {
			sub := createBenchSession(b, broker, fmt.Sprintf("sub-%s-%d", topic, i))
			broker.subscribe(sub, topic, 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
		}
	}

	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		topic := topics[i%len(topics)]
		msg := &storage.Message{
			Topic: topic,
			QoS:   0,
		}
		msg.SetPayloadFromBytes(payload)
		broker.Publish(msg) //nolint:errcheck // best-effort
	}
}

// BenchmarkMessagePublish_WildcardHeavy stresses wildcard matching and shared subscription dedup.
func BenchmarkMessagePublish_WildcardHeavy(b *testing.B) {
	broker := createBenchBroker(b)
	defer broker.Close()

	const (
		exactSubs    = 1000
		sharedSubs   = 20
		wildcardSubs = 10
	)

	// Exact subscribers.
	topics := make([]string, exactSubs)
	for i := 0; i < exactSubs; i++ {
		topic := fmt.Sprintf("sensors/room%d/temperature", i)
		topics[i] = topic
		sub := createBenchSession(b, broker, fmt.Sprintf("sub-%d", i))
		broker.subscribe(sub, topic, 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
	}

	// Wildcard subscribers.
	for i := 0; i < wildcardSubs; i++ {
		sub := createBenchSession(b, broker, fmt.Sprintf("wild-%d", i))
		switch i % 4 {
		case 0:
			broker.subscribe(sub, "sensors/+/temperature", 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
		case 1:
			broker.subscribe(sub, "sensors/#", 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
		case 2:
			broker.subscribe(sub, "+/room+/temperature", 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
		default:
			broker.subscribe(sub, "sensors/+/+", 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
		}
	}

	// Shared subscription group (forces dedup per publish).
	for i := 0; i < sharedSubs; i++ {
		sub := createBenchSession(b, broker, fmt.Sprintf("shared-%d", i))
		broker.subscribe(sub, "$share/group1/sensors/+/temperature", 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
	}

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := &storage.Message{
			Topic: topics[i%len(topics)],
			QoS:   0,
		}
		msg.SetPayloadFromBytes(payload)
		broker.Publish(msg) //nolint:errcheck // best-effort
	}
}

// BenchmarkMessagePublish_WithChurn mixes publish with subscribe/unsubscribe churn.
func BenchmarkMessagePublish_WithChurn(b *testing.B) {
	broker := createBenchBroker(b)
	defer broker.Close()

	const (
		baseSubs  = 200
		churnSubs = 50
	)

	// Base subscribers.
	topics := make([]string, baseSubs)
	for i := 0; i < baseSubs; i++ {
		topic := fmt.Sprintf("devices/%d/state", i)
		topics[i] = topic
		sub := createBenchSession(b, broker, fmt.Sprintf("base-%d", i))
		broker.subscribe(sub, topic, 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
	}

	// Churn sessions reused across subscribe/unsubscribe.
	churnSessions := make([]*session.Session, churnSubs)
	for i := 0; i < churnSubs; i++ {
		churnSessions[i] = createBenchSession(b, broker, fmt.Sprintf("churn-%d", i))
	}

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if i%20 == 0 {
			// 5% churn: subscribe + unsubscribe on a rotating topic.
			idx := (i / 20) % churnSubs
			topic := topics[idx%len(topics)]
			_ = broker.subscribe(churnSessions[idx], topic, 0, storage.SubscribeOptions{})
			_ = broker.Unsubscribe(churnSessions[idx].ID, topic)
			continue
		}

		msg := &storage.Message{
			Topic: topics[i%len(topics)],
			QoS:   0,
		}
		msg.SetPayloadFromBytes(payload)
		broker.Publish(msg) //nolint:errcheck // best-effort
	}
}

// BenchmarkMessageDistribute benchmarks the distribute function directly.
func BenchmarkMessageDistribute(b *testing.B) {
	broker := createBenchBroker(b)
	defer broker.Close()

	// Create 10 subscribers
	for i := 0; i < 10; i++ {
		sub := createBenchSession(b, broker, fmt.Sprintf("sub-%d", i))
		broker.subscribe(sub, "test/topic", 0, storage.SubscribeOptions{}) //nolint:errcheck // test setup
	}

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ReportAllocs()

	for b.Loop() {
		msg := &storage.Message{
			Topic: "test/topic",
			QoS:   0,
		}
		msg.SetPayloadFromBytes(payload)
		broker.distribute(msg) //nolint:errcheck // best-effort
		msg.ReleasePayload()
	}
}

// BenchmarkBufferPooling benchmarks the buffer pool performance.
func BenchmarkBufferPooling(b *testing.B) {
	pool := core.NewBufferPool()
	payload := make([]byte, 1024)

	b.ReportAllocs()

	for b.Loop() {
		buf := pool.GetWithData(payload)
		buf.Release()
	}
}

// BenchmarkBufferPooling_Parallel benchmarks parallel buffer pool usage.
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

// BenchmarkMessageCopy_Legacy simulates the old copy-based approach.
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
			for b.Loop() {
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

// BenchmarkMessageCopy_ZeroCopy benchmarks the new zero-copy approach.
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

			for b.Loop() {
				// Simulate zero-copy: create buffer, retain for sharing
				buf := pool.GetWithData(payload)
				buf.Retain()  // Share with subscriber 1
				buf.Retain()  // Share with subscriber 2
				buf.Release() // Original release
				buf.Release() // Subscriber 1 release
				buf.Release() // Subscriber 2 release
			}
		})
	}
}

// Helper functions

func createBenchBroker(tb testing.TB) *Broker {
	tb.Helper()

	broker := NewBroker(nil, nil)
	return broker
}

func createBenchSession(tb testing.TB, broker *Broker, clientID string) *session.Session {
	tb.Helper()

	opts := session.Options{
		CleanStart:     true,
		KeepAlive:      60 * time.Second,
		ReceiveMaximum: 65535,
	}

	s, _, err := broker.CreateSession(clientID, 5, opts)
	if err != nil {
		tb.Fatalf("Failed to create session: %v", err)
	}

	// Create a mock connection
	conn := &mockBenchConn{clientID: clientID}
	if err := s.Connect(conn); err != nil {
		tb.Fatalf("Failed to connect session: %v", err)
	}

	return s
}

// benchAddr implements net.Addr for benchmarks.
type benchAddr struct{}

func (b *benchAddr) Network() string { return "tcp" }
func (b *benchAddr) String() string  { return "127.0.0.1:1883" }

// mockBenchConn is a minimal mock connection for benchmarks.
type mockBenchConn struct {
	net.Conn
	clientID string
}

func (m *mockBenchConn) ReadPacket() (packets.ControlPacket, error) {
	return nil, nil
}

func (m *mockBenchConn) WritePacket(pkt packets.ControlPacket) error {
	return m.WriteControlPacket(pkt, nil)
}

func (m *mockBenchConn) WriteControlPacket(pkt packets.ControlPacket, onSent func()) error {
	pkt.Release()
	if onSent != nil {
		onSent()
	}
	return nil
}

func (m *mockBenchConn) WriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	pkt.Release()
	if onSent != nil {
		onSent()
	}
	return nil
}

func (m *mockBenchConn) TryWriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	return m.WriteDataPacket(pkt, onSent)
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
