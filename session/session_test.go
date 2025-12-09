package session

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/dborovcanin/mqtt/packets"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/memory"
)

// mockConnection implements Connection for testing.
type mockConnection struct {
	closed   bool
	packets  []packets.ControlPacket
	readCh   chan packets.ControlPacket
	deadline time.Time
}

func newMockConnection() *mockConnection {
	return &mockConnection{
		readCh: make(chan packets.ControlPacket, 10),
	}
}

func (c *mockConnection) ReadPacket() (packets.ControlPacket, error) {
	pkt, ok := <-c.readCh
	if !ok {
		return nil, ErrNotConnected
	}
	return pkt, nil
}

func (c *mockConnection) WritePacket(p packets.ControlPacket) error {
	if c.closed {
		return ErrNotConnected
	}
	c.packets = append(c.packets, p)
	return nil
}

func (c *mockConnection) Close() error {
	c.closed = true
	close(c.readCh)
	return nil
}

func (c *mockConnection) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
}

func (c *mockConnection) SetReadDeadline(t time.Time) error {
	c.deadline = t
	return nil
}

func (c *mockConnection) SetWriteDeadline(t time.Time) error {
	c.deadline = t
	return nil
}

func TestSessionNew(t *testing.T) {
	opts := Options{
		CleanStart:     true,
		ExpiryInterval: 3600,
		ReceiveMaximum: 100,
		KeepAlive:      60,
	}

	s := New("client1", 5, opts)

	if s.ID != "client1" {
		t.Errorf("ID: got %s, want client1", s.ID)
	}
	if s.Version != 5 {
		t.Errorf("Version: got %d, want 5", s.Version)
	}
	if s.State() != StateNew {
		t.Errorf("State: got %v, want StateNew", s.State())
	}
	if s.CleanStart != true {
		t.Errorf("CleanStart: got %v, want true", s.CleanStart)
	}
	if s.ReceiveMaximum != 100 {
		t.Errorf("ReceiveMaximum: got %d, want 100", s.ReceiveMaximum)
	}
}

func TestSessionConnect(t *testing.T) {
	s := New("client1", 5, DefaultOptions())
	conn := newMockConnection()

	if err := s.Connect(conn); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if s.State() != StateConnected {
		t.Errorf("State: got %v, want StateConnected", s.State())
	}
	if !s.IsConnected() {
		t.Error("IsConnected should return true")
	}
	if s.Conn() == nil {
		t.Error("Conn should not be nil")
	}
}

func TestSessionDisconnect(t *testing.T) {
	s := New("client1", 5, DefaultOptions())
	conn := newMockConnection()
	s.Connect(conn)

	// Set up disconnect callback
	callbackCalled := make(chan bool, 1)
	s.SetOnDisconnect(func(sess *Session, graceful bool) {
		callbackCalled <- graceful
	})

	// Graceful disconnect
	s.Disconnect(true)

	if s.State() != StateDisconnected {
		t.Errorf("State: got %v, want StateDisconnected", s.State())
	}
	if s.IsConnected() {
		t.Error("IsConnected should return false")
	}

	select {
	case graceful := <-callbackCalled:
		if !graceful {
			t.Error("Disconnect callback should indicate graceful=true")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Disconnect callback not called")
	}
}

func TestSessionPacketID(t *testing.T) {
	s := New("client1", 5, DefaultOptions())

	ids := make(map[uint16]bool)
	for i := 0; i < 100; i++ {
		id := s.NextPacketID()
		if id == 0 {
			t.Error("Packet ID should never be 0")
		}
		if ids[id] {
			t.Errorf("Duplicate packet ID: %d", id)
		}
		ids[id] = true
	}
}

func TestSessionSubscriptions(t *testing.T) {
	s := New("client1", 5, DefaultOptions())

	opts := store.SubscribeOptions{NoLocal: true, RetainAsPublished: true}
	s.AddSubscription("home/+/temp", opts)

	subs := s.GetSubscriptions()
	if len(subs) != 1 {
		t.Fatalf("Expected 1 subscription, got %d", len(subs))
	}

	got, ok := subs["home/+/temp"]
	if !ok {
		t.Error("Subscription not found")
	}
	if !got.NoLocal {
		t.Error("NoLocal should be true")
	}
	if !got.RetainAsPublished {
		t.Error("RetainAsPublished should be true")
	}

	s.RemoveSubscription("home/+/temp")
	subs = s.GetSubscriptions()
	if len(subs) != 0 {
		t.Errorf("Expected 0 subscriptions after remove, got %d", len(subs))
	}
}

func TestSessionTopicAliases(t *testing.T) {
	s := New("client1", 5, DefaultOptions())

	// Outbound alias
	s.SetTopicAlias("home/temp", 1)
	alias, ok := s.GetTopicAlias("home/temp")
	if !ok || alias != 1 {
		t.Errorf("Outbound alias: got %d, %v", alias, ok)
	}

	// Inbound alias
	s.SetInboundAlias(2, "sensors/humidity")
	topic, ok := s.ResolveInboundAlias(2)
	if !ok || topic != "sensors/humidity" {
		t.Errorf("Inbound alias: got %s, %v", topic, ok)
	}
}

func TestInflightTracker(t *testing.T) {
	tracker := NewInflightTracker(10)

	msg := &store.Message{
		Topic:   "test",
		Payload: []byte("data"),
		QoS:     1,
	}

	// Add message
	if err := tracker.Add(1, msg, Outbound); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Check has
	if !tracker.Has(1) {
		t.Error("Has should return true")
	}

	// Get message
	inf, ok := tracker.Get(1)
	if !ok {
		t.Fatal("Get should return message")
	}
	if inf.PacketID != 1 {
		t.Errorf("PacketID: got %d, want 1", inf.PacketID)
	}
	if inf.State != StatePublishSent {
		t.Errorf("State: got %v, want StatePublishSent", inf.State)
	}

	// Update state
	if err := tracker.UpdateState(1, StatePubRecReceived); err != nil {
		t.Fatalf("UpdateState failed: %v", err)
	}
	inf, _ = tracker.Get(1)
	if inf.State != StatePubRecReceived {
		t.Errorf("State after update: got %v, want StatePubRecReceived", inf.State)
	}

	// Ack message
	acked, err := tracker.Ack(1)
	if err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
	if string(acked.Payload) != "data" {
		t.Errorf("Acked message payload mismatch")
	}

	// Should be gone
	if tracker.Has(1) {
		t.Error("Has should return false after ack")
	}
}

func TestInflightTrackerCapacity(t *testing.T) {
	tracker := NewInflightTracker(3)

	for i := uint16(1); i <= 3; i++ {
		if err := tracker.Add(i, &store.Message{}, Outbound); err != nil {
			t.Fatalf("Add %d failed: %v", i, err)
		}
	}

	if err := tracker.Add(4, &store.Message{}, Outbound); err != ErrInflightFull {
		t.Errorf("Expected ErrInflightFull, got %v", err)
	}

	if !tracker.IsFull() {
		t.Error("IsFull should return true")
	}
}

func TestInflightTrackerExpired(t *testing.T) {
	tracker := NewInflightTracker(10)

	tracker.Add(1, &store.Message{Topic: "t1"}, Outbound)
	time.Sleep(50 * time.Millisecond)
	tracker.Add(2, &store.Message{Topic: "t2"}, Outbound)

	expired := tracker.GetExpired(40 * time.Millisecond)
	if len(expired) != 1 {
		t.Errorf("Expected 1 expired, got %d", len(expired))
	}
}

func TestInflightTrackerQoS2Received(t *testing.T) {
	tracker := NewInflightTracker(10)

	// Mark as received
	tracker.MarkReceived(123)

	if !tracker.WasReceived(123) {
		t.Error("WasReceived should return true")
	}

	// Clear
	tracker.ClearReceived(123)

	if tracker.WasReceived(123) {
		t.Error("WasReceived should return false after clear")
	}
}

func TestMessageQueue(t *testing.T) {
	q := NewMessageQueue(3)

	// Enqueue
	for i := 0; i < 3; i++ {
		msg := &store.Message{Topic: "t" + string(rune('0'+i))}
		if err := q.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue %d failed: %v", i, err)
		}
	}

	if q.Len() != 3 {
		t.Errorf("Len: got %d, want 3", q.Len())
	}

	// Queue full
	if err := q.Enqueue(&store.Message{}); !errors.Is(err, ErrQueueFull) {
		t.Errorf("Expected ErrQueueFull, got %v", err)
	}

	// Peek
	msg := q.Peek()
	if msg.Topic != "t0" {
		t.Errorf("Peek: got %s, want t0", msg.Topic)
	}

	// Dequeue
	msg = q.Dequeue()
	if msg.Topic != "t0" {
		t.Errorf("Dequeue: got %s, want t0", msg.Topic)
	}
	if q.Len() != 2 {
		t.Errorf("Len after dequeue: got %d, want 2", q.Len())
	}

	// Drain
	msgs := q.Drain()
	if len(msgs) != 2 {
		t.Errorf("Drain: got %d, want 2", len(msgs))
	}
	if !q.IsEmpty() {
		t.Error("Queue should be empty after drain")
	}

	// Dequeue empty
	if q.Dequeue() != nil {
		t.Error("Dequeue on empty should return nil")
	}
}

func TestSessionManager(t *testing.T) {
	st := memory.New()
	mgr := NewManager(st)
	defer mgr.Close()

	// Create session
	s, created, err := mgr.GetOrCreate("client1", 5, DefaultOptions())
	if err != nil {
		t.Fatalf("GetOrCreate failed: %v", err)
	}
	if !created {
		t.Error("Expected session to be created")
	}
	if s.ID != "client1" {
		t.Errorf("ID: got %s, want client1", s.ID)
	}

	// Get existing
	s2, created, _ := mgr.GetOrCreate("client1", 5, Options{CleanStart: false})
	if created {
		t.Error("Expected session to not be created")
	}
	if s2 != s {
		t.Error("Should return same session")
	}

	// Count
	if mgr.Count() != 1 {
		t.Errorf("Count: got %d, want 1", mgr.Count())
	}

	// Destroy
	mgr.Destroy("client1")
	if mgr.Count() != 0 {
		t.Errorf("Count after destroy: got %d, want 0", mgr.Count())
	}
}

func TestSessionManagerCleanStart(t *testing.T) {
	st := memory.New()
	mgr := NewManager(st)
	defer mgr.Close()

	// Create session
	s1, _, _ := mgr.GetOrCreate("client1", 5, Options{CleanStart: false})
	s1.AddSubscription("home/#", store.SubscribeOptions{NoLocal: true})

	// Clean start should create new session
	s2, created, _ := mgr.GetOrCreate("client1", 5, Options{CleanStart: true})
	if !created {
		t.Error("Clean start should create new session")
	}
	if s2 == s1 {
		t.Error("Should be different session object")
	}

	// Subscriptions should be cleared
	if len(s2.GetSubscriptions()) != 0 {
		t.Error("Clean start session should have no subscriptions")
	}
}

func TestSessionManagerTakeover(t *testing.T) {
	st := memory.New()
	mgr := NewManager(st)
	defer mgr.Close()

	// Create session and connect
	s1, _, _ := mgr.GetOrCreate("client1", 5, DefaultOptions())
	conn1 := newMockConnection()
	s1.Connect(conn1)

	// Takeover (new connection with same clientID)
	s2, created, _ := mgr.GetOrCreate("client1", 5, Options{CleanStart: false})
	if created {
		t.Error("Session takeover should not create new session")
	}

	// Old connection should be disconnected
	if !conn1.closed {
		t.Error("Old connection should be closed on takeover")
	}

	// Should be same session
	if s2 != s1 {
		t.Error("Session takeover should return same session")
	}
}

func TestSessionManagerForEach(t *testing.T) {
	st := memory.New()
	mgr := NewManager(st)
	defer mgr.Close()

	mgr.GetOrCreate("client1", 5, DefaultOptions())
	mgr.GetOrCreate("client2", 5, DefaultOptions())
	mgr.GetOrCreate("client3", 5, DefaultOptions())

	count := 0
	mgr.ForEach(func(s *Session) {
		count++
	})

	if count != 3 {
		t.Errorf("ForEach count: got %d, want 3", count)
	}
}
