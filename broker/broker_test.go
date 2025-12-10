package broker

import (
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
)

func TestNewBroker(t *testing.T) {
	b := NewBroker(nil)
	if b == nil {
		t.Fatal("NewBroker returned nil")
	}
	if b.logger == nil {
		t.Error("logger should not be nil")
	}
	if b.router == nil {
		t.Error("router should not be nil")
	}
	if b.sessionMgr == nil {
		t.Error("sessionMgr should not be nil")
	}

	// Test with custom logger
	logger := slog.Default()
	b2 := NewBroker(logger)
	if b2.logger != logger {
		t.Error("custom logger not set")
	}
}

func TestBrokerSubscribe(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	// Subscribe to topic
	err := b.Subscribe("client1", "home/temperature", 1, store.SubscribeOptions{})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Verify subscription via Match
	subs, err := b.Match("home/temperature")
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}
	if len(subs) != 1 {
		t.Fatalf("Expected 1 subscription, got %d", len(subs))
	}
	if subs[0].ClientID != "client1" {
		t.Errorf("ClientID: got %s, want client1", subs[0].ClientID)
	}
	if subs[0].QoS != 1 {
		t.Errorf("QoS: got %d, want 1", subs[0].QoS)
	}
}

func TestBrokerSubscribeWithWildcards(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	// Subscribe to wildcard topics
	b.Subscribe("client1", "home/+/temperature", 0, store.SubscribeOptions{})
	b.Subscribe("client2", "home/#", 1, store.SubscribeOptions{})

	// Test single-level wildcard
	subs, _ := b.Match("home/living/temperature")
	if len(subs) != 2 {
		t.Errorf("Expected 2 subscriptions for home/living/temperature, got %d", len(subs))
	}

	// Test multi-level wildcard
	subs, _ = b.Match("home/bedroom/humidity")
	if len(subs) != 1 {
		t.Errorf("Expected 1 subscription for home/bedroom/humidity, got %d", len(subs))
	}
	if subs[0].ClientID != "client2" {
		t.Errorf("Expected client2 for multi-level wildcard")
	}
}

func TestBrokerUnsubscribe(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	// Subscribe first
	b.Subscribe("client1", "test/topic", 0, store.SubscribeOptions{})
	b.Subscribe("client2", "test/topic", 1, store.SubscribeOptions{})

	// Verify both subscriptions exist
	subs, _ := b.Match("test/topic")
	if len(subs) != 2 {
		t.Fatalf("Expected 2 subscriptions before unsubscribe, got %d", len(subs))
	}

	// Unsubscribe client1
	err := b.Unsubscribe("client1", "test/topic")
	if err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

	// Verify only client2 remains
	subs, _ = b.Match("test/topic")
	if len(subs) != 1 {
		t.Fatalf("Expected 1 subscription after unsubscribe, got %d", len(subs))
	}
	if subs[0].ClientID != "client2" {
		t.Errorf("Expected client2 to remain subscribed")
	}
}

func TestBrokerRetainedMessages(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	// Publish retained message
	err := b.Distribute("sensors/temp", []byte("23.5"), 0, true, nil)
	if err != nil {
		t.Fatalf("Distribute failed: %v", err)
	}

	// Verify retained message is stored
	retained, err := b.retained.Get("sensors/temp")
	if err != nil {
		t.Fatalf("Failed to get retained message: %v", err)
	}
	if string(retained.Payload) != "23.5" {
		t.Errorf("Retained payload: got %s, want 23.5", string(retained.Payload))
	}
	if retained.Retain != true {
		t.Error("Retain flag should be true")
	}

	// Delete retained message (empty payload)
	err = b.Distribute("sensors/temp", []byte(""), 0, true, nil)
	if err != nil {
		t.Fatalf("Distribute delete failed: %v", err)
	}

	// Verify retained message is deleted
	_, err = b.retained.Get("sensors/temp")
	if err != store.ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}
}

func TestBrokerRetainedMessagesOnSubscribe(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	// Publish retained message first
	b.Distribute("home/kitchen/temp", []byte("21.0"), 1, true, nil)

	// Create and connect a session
	s, _, err := b.sessionMgr.GetOrCreate("client1", 4, testSessionOptions())
	if err != nil {
		t.Fatalf("GetOrCreate failed: %v", err)
	}

	conn := newMockBrokerConnection()
	s.Connect(conn)

	// Subscribe - should receive retained message
	err = b.Subscribe("client1", "home/kitchen/temp", 1, store.SubscribeOptions{})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Verify retained message was delivered
	if len(conn.packets) == 0 {
		t.Fatal("Expected retained message to be delivered")
	}

	// Should be a PUBLISH packet
	if conn.packets[0].Type() != packets.PublishType {
		t.Errorf("Expected PUBLISH packet, got type %v", conn.packets[0].Type())
	}
}

func TestBrokerMatch(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	// Add multiple subscriptions
	b.Subscribe("client1", "home/+/temp", 0, store.SubscribeOptions{})
	b.Subscribe("client2", "home/#", 1, store.SubscribeOptions{})
	b.Subscribe("client3", "home/bedroom/temp", 2, store.SubscribeOptions{})

	subs, err := b.Match("home/bedroom/temp")
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	// Should match all three subscriptions
	if len(subs) != 3 {
		t.Errorf("Expected 3 matching subscriptions, got %d", len(subs))
	}

	// Verify all clients are present
	clientIDs := make(map[string]bool)
	for _, sub := range subs {
		clientIDs[sub.ClientID] = true
	}

	if !clientIDs["client1"] || !clientIDs["client2"] || !clientIDs["client3"] {
		t.Error("Not all expected clients found in match results")
	}
}

func TestBrokerDistribute(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	// Create two sessions
	s1, _, _ := b.sessionMgr.GetOrCreate("client1", 4, testSessionOptions())
	s2, _, _ := b.sessionMgr.GetOrCreate("client2", 4, testSessionOptions())

	conn1 := newMockBrokerConnection()
	conn2 := newMockBrokerConnection()
	s1.Connect(conn1)
	s2.Connect(conn2)

	// Subscribe both to the topic
	b.Subscribe("client1", "test/topic", 1, store.SubscribeOptions{})
	b.Subscribe("client2", "test/topic", 0, store.SubscribeOptions{})

	// Distribute message
	err := b.Distribute("test/topic", []byte("hello"), 1, false, nil)
	if err != nil {
		t.Fatalf("Distribute failed: %v", err)
	}

	// Both connections should receive the message
	if len(conn1.packets) == 0 {
		t.Error("client1 should have received message")
	}
	if len(conn2.packets) == 0 {
		t.Error("client2 should have received message")
	}
}

func TestBrokerDistributeQoSDowngrade(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	s, _, _ := b.sessionMgr.GetOrCreate("client1", 4, testSessionOptions())
	conn := newMockBrokerConnection()
	s.Connect(conn)

	// Subscribe with QoS 0
	b.Subscribe("client1", "test/topic", 0, store.SubscribeOptions{})

	// Publish with QoS 1 - should be downgraded to QoS 0
	b.Distribute("test/topic", []byte("data"), 1, false, nil)

	if len(conn.packets) == 0 {
		t.Fatal("Expected message to be delivered")
	}

	// Check that QoS was downgraded
	pub, ok := conn.packets[0].(*v3.Publish)
	if !ok {
		t.Fatal("Packet is not a Publish packet")
	}
	if pub.QoS != 0 {
		t.Errorf("Expected QoS 0 after downgrade, got %d", pub.QoS)
	}
}

func TestBrokerOfflineMessageQueue(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	// Create session but don't connect
	s1, _, _ := b.sessionMgr.GetOrCreate("client1", 4, testSessionOptions())
	b.Subscribe("client1", "test/topic", 1, store.SubscribeOptions{})

	// Ensure session is not connected
	if s1.IsConnected() {
		s1.Disconnect(true)
	}

	// Distribute message - should be queued
	b.Distribute("test/topic", []byte("offline msg"), 1, false, nil)

	// Check offline queue
	if s1.OfflineQueue().Len() != 1 {
		t.Errorf("Expected 1 message in offline queue, got %d", s1.OfflineQueue().Len())
	}

	msg := s1.OfflineQueue().Peek()
	if string(msg.Payload) != "offline msg" {
		t.Errorf("Offline message payload: got %s, want 'offline msg'", string(msg.Payload))
	}
}

func TestBrokerOfflineQueueQoS0Skip(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	s, _, _ := b.sessionMgr.GetOrCreate("client1", 4, testSessionOptions())
	b.Subscribe("client1", "test/topic", 0, store.SubscribeOptions{})

	// Ensure session is not connected
	if s.IsConnected() {
		s.Disconnect(true)
	}

	// Distribute QoS 0 message - should NOT be queued
	b.Distribute("test/topic", []byte("qos0 msg"), 0, false, nil)

	// Offline queue should be empty (QoS 0 not queued)
	if s.OfflineQueue().Len() != 0 {
		t.Errorf("Expected 0 messages in offline queue for QoS 0, got %d", s.OfflineQueue().Len())
	}
}

func TestBrokerPublish(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	s, _, _ := b.sessionMgr.GetOrCreate("client1", 4, testSessionOptions())
	conn := newMockBrokerConnection()
	s.Connect(conn)

	b.Subscribe("client1", "test/topic", 1, store.SubscribeOptions{})

	// Test Publish method
	err := b.Publish(nil, "publisher", "test/topic", []byte("via publish"), 1, false)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if len(conn.packets) == 0 {
		t.Error("Expected message to be delivered via Publish")
	}
}

func TestBrokerClose(t *testing.T) {
	b := NewBroker(nil)

	// Create and connect some sessions
	s1, _, _ := b.sessionMgr.GetOrCreate("client1", 4, testSessionOptions())
	s2, _, _ := b.sessionMgr.GetOrCreate("client2", 4, testSessionOptions())

	conn1 := newMockBrokerConnection()
	conn2 := newMockBrokerConnection()
	s1.Connect(conn1)
	s2.Connect(conn2)

	// Verify sessions are connected
	if b.sessionMgr.ConnectedCount() != 2 {
		t.Errorf("Expected 2 connected sessions, got %d", b.sessionMgr.ConnectedCount())
	}

	err := b.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify no sessions are connected after close
	if b.sessionMgr.ConnectedCount() != 0 {
		t.Errorf("Expected 0 connected sessions after Close, got %d", b.sessionMgr.ConnectedCount())
	}
}

func TestBrokerSessionDestroyCleansSubscriptions(t *testing.T) {
	b := NewBroker(nil)
	defer b.Close()

	clientID := "client-destroy-test"
	topic := "test/destroy"

	// 1. Create and connect session
	sess, _, err := b.sessionMgr.GetOrCreate(clientID, 4, testSessionOptions())
	if err != nil {
		t.Fatalf("GetOrCreate failed: %v", err)
	}
	conn := newMockBrokerConnection()
	sess.Connect(conn)

	// 2. Subscribe
	err = b.Subscribe(clientID, topic, 1, store.SubscribeOptions{})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// 3. Verify subscription exists in router
	subs := b.router.Match(topic)
	found := false
	for _, s := range subs {
		if s.SessionID == clientID {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Subscription not found in router before destroy")
	}

	// 4. Destroy session
	b.sessionMgr.Destroy(clientID)

	// Wait a bit for callback (if it existed/worked)
	time.Sleep(50 * time.Millisecond)

	// 5. Verify subscription is gone from router
	subs = b.router.Match(topic)
	for _, s := range subs {
		if s.SessionID == clientID {
			t.Fatal("Subscription still exists in router after session destroy")
		}
	}
}

// Helper functions

func testSessionOptions() session.Options {
	return session.DefaultOptions()
}

// mockBrokerConnection for testing broker functionality
type mockBrokerConnection struct {
	packets []packets.ControlPacket
	closed  bool
}

func newMockBrokerConnection() *mockBrokerConnection {
	return &mockBrokerConnection{
		packets: make([]packets.ControlPacket, 0),
	}
}

func (c *mockBrokerConnection) ReadPacket() (packets.ControlPacket, error) {
	return nil, nil
}

func (c *mockBrokerConnection) WritePacket(p packets.ControlPacket) error {
	if c.closed {
		return session.ErrNotConnected
	}
	c.packets = append(c.packets, p)
	return nil
}

func (c *mockBrokerConnection) Close() error {
	c.closed = true
	return nil
}

func (c *mockBrokerConnection) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
}

func (c *mockBrokerConnection) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *mockBrokerConnection) SetWriteDeadline(t time.Time) error {
	return nil
}
