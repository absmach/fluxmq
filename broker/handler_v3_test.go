package broker

import (
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/dborovcanin/mqtt/core"
	"github.com/dborovcanin/mqtt/core/packets"
	v3 "github.com/dborovcanin/mqtt/core/packets/v3"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/messages"
)

var _ core.Connection = (*mockConnection)(nil)

// mockConnection implements session.Connection for testing.
type mockConnection struct {
	closed     bool
	written    []packets.ControlPacket
	readCh     chan packets.ControlPacket
	errOnWrite error
	errOnRead  error
	writeDelay time.Duration
}

func newMockConnection() *mockConnection {
	return &mockConnection{
		readCh: make(chan packets.ControlPacket, 10),
	}
}

func (c *mockConnection) LocalAddr() net.Addr {
	return nil
}

func (c *mockConnection) Read(b []byte) (int, error) {
	return 0, nil
}

func (c *mockConnection) Write(b []byte) (int, error) {
	return 0, nil
}

func (c *mockConnection) SetDeadline(t time.Time) error {
	return nil
}

func (c *mockConnection) SetKeepAlive(d time.Duration) error {
	return nil
}

func (c *mockConnection) SetOnDisconnect(func(graceful bool)) {
}

func (c *mockConnection) Touch() {}

func (c *mockConnection) ReadPacket() (packets.ControlPacket, error) {
	if c.errOnRead != nil {
		return nil, c.errOnRead
	}
	pkt, ok := <-c.readCh
	if !ok {
		return nil, session.ErrNotConnected
	}
	return pkt, nil
}

func (c *mockConnection) WritePacket(p packets.ControlPacket) error {
	if c.closed {
		return session.ErrNotConnected
	}
	if c.writeDelay > 0 {
		time.Sleep(c.writeDelay)
	}
	if c.errOnWrite != nil {
		return c.errOnWrite
	}
	c.written = append(c.written, p)
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

func (c *mockConnection) SetReadDeadline(t time.Time) error  { return nil }
func (c *mockConnection) SetWriteDeadline(t time.Time) error { return nil }

func createTestSession(id string, version byte) (*session.Session, *mockConnection) {
	conn := newMockConnection()
	opts := session.Options{
		CleanStart:     true,
		ReceiveMaximum: 100,
		KeepAlive:      60,
	}
	inflight := messages.NewInflightTracker(100)
	queue := messages.NewMessageQueue(1000)
	s := session.New(id, version, opts, inflight, queue)
	s.Connect(conn)
	return s, conn
}

func TestHandlePublishQoS0(t *testing.T) {
	b := NewBroker(slog.Default())
	defer b.Close()

	// 1. Setup subscriber
	subS, subConn := createTestSession("sub1", 4)
	b.sessionsMap.Set(subS.ID, subS)
	b.Subscribe(subS.ID, "test/topic", 0, store.SubscribeOptions{})

	// 2. Setup publisher handler
	// NewBroker creates handler automatically, access it via b
	// But we need a session for the publisher calling HandlePublish
	pubS, _ := createTestSession("pub1", 4)

	pkt := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "test/topic",
		Payload:     []byte("hello"),
	}

	err := b.handler.HandlePublish(pubS, pkt)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}

	// 3. Verify subscriber received message
	if len(subConn.written) != 1 {
		t.Fatalf("Expected subscriber to receive 1 packet, got %d", len(subConn.written))
	}
	pub, ok := subConn.written[0].(*v3.Publish)
	if !ok {
		t.Fatalf("Expected Publish packet, got %T", subConn.written[0])
	}
	if string(pub.Payload) != "hello" {
		t.Errorf("Payload mismatch: got %s, want hello", pub.Payload)
	}
}

func TestHandlePublishQoS1(t *testing.T) {
	b := NewBroker(slog.Default())
	defer b.Close()

	subS, subConn := createTestSession("sub1", 4)
	b.sessionsMap.Set(subS.ID, subS)
	b.Subscribe(subS.ID, "test/topic", 1, store.SubscribeOptions{})

	pubS, pubConn := createTestSession("pub1", 4)

	pkt := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
		TopicName:   "test/topic",
		Payload:     []byte("hello"),
		ID:          123,
	}

	err := b.handler.HandlePublish(pubS, pkt)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}

	// Verify Publisher received PUBACK
	if len(pubConn.written) != 1 {
		t.Fatalf("Expected PUBACK, got %d packets", len(pubConn.written))
	}
	ack, ok := pubConn.written[0].(*v3.PubAck)
	if !ok {
		t.Fatalf("Expected PubAck, got %T", pubConn.written[0])
	}
	if ack.ID != 123 {
		t.Errorf("PubAck ID: got %d, want 123", ack.ID)
	}

	// Verify Subscriber received MSG
	if len(subConn.written) != 1 {
		t.Errorf("Expected subscriber to receive message")
	}
}

func TestHandlePublishQoS2(t *testing.T) {
	b := NewBroker(slog.Default())
	defer b.Close()

	subS, subConn := createTestSession("sub1", 4)
	b.sessionsMap.Set(subS.ID, subS)
	b.Subscribe(subS.ID, "test/topic", 2, store.SubscribeOptions{})

	pubS, pubConn := createTestSession("pub1", 4)

	// Step 1: PUBLISH
	pkt := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2},
		TopicName:   "test/topic",
		Payload:     []byte("hello"),
		ID:          456,
	}

	err := b.handler.HandlePublish(pubS, pkt)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}

	// Should send PUBREC to publisher
	if len(pubConn.written) != 1 {
		t.Fatalf("Expected PUBREC, got %d", len(pubConn.written))
	}
	rec, ok := pubConn.written[0].(*v3.PubRec)
	if !ok {
		t.Fatalf("Expected PubRec, got %T", pubConn.written[0])
	}
	if rec.ID != 456 {
		t.Errorf("PubRec ID: got %d, want 456", rec.ID)
	}

	// Subscriber should NOT receive anything yet
	if len(subConn.written) != 0 {
		t.Errorf("Subscriber received message before PUBREL")
	}

	// Step 2: PUBREL
	pubrel := &v3.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          456,
	}

	err = b.handler.HandlePubRel(pubS, pubrel)
	if err != nil {
		t.Fatalf("HandlePubRel failed: %v", err)
	}

	// Should send PUBCOMP to publisher
	if len(pubConn.written) != 2 {
		t.Fatalf("Expected PUBCOMP, got %d", len(pubConn.written))
	}
	comp, ok := pubConn.written[1].(*v3.PubComp)
	if !ok {
		t.Fatalf("Expected PubComp, got %T", pubConn.written[1])
	}
	if comp.ID != 456 {
		t.Errorf("PubComp ID: got %d, want 456", comp.ID)
	}

	// Subscriber SHOULD receive message now
	if len(subConn.written) != 1 {
		t.Errorf("Subscriber did not receive message after PUBREL")
	}
}

func TestHandleSubscribe(t *testing.T) {
	b := NewBroker(slog.Default())
	defer b.Close()

	s, conn := createTestSession("test-client", 4)
	b.sessionsMap.Set(s.ID, s)

	pkt := &v3.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType},
		ID:          789,
		Topics: []v3.Topic{
			{Name: "home/+/temp", QoS: 1},
			{Name: "sensors/#", QoS: 2},
		},
	}

	err := b.handler.HandleSubscribe(s, pkt)
	if err != nil {
		t.Fatalf("HandleSubscribe failed: %v", err)
	}

	// Check SUBACK
	if len(conn.written) != 1 {
		t.Fatalf("Expected SUBACK, got %d", len(conn.written))
	}
	ack, ok := conn.written[0].(*v3.SubAck)
	if !ok {
		t.Fatalf("Expected SubAck, got %T", conn.written[0])
	}
	if ack.ID != 789 {
		t.Errorf("SubAck ID: got %d, want 789", ack.ID)
	}
	if len(ack.ReturnCodes) != 2 {
		t.Errorf("Expected 2 return codes")
	}

	// Verify broker routing table
	subs, _ := b.Match("home/kitchen/temp")
	if len(subs) == 0 {
		t.Error("Subscription not active in broker")
	}
}

func TestHandleUnsubscribe(t *testing.T) {
	b := NewBroker(slog.Default())
	defer b.Close()

	s, conn := createTestSession("test-client", 4)
	b.sessionsMap.Set(s.ID, s)

	// Subscribe first
	b.Subscribe(s.ID, "home/#", 1, store.SubscribeOptions{})

	pkt := &v3.Unsubscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubscribeType},
		ID:          101,
		Topics:      []string{"home/#"},
	}

	err := b.handler.HandleUnsubscribe(s, pkt)
	if err != nil {
		t.Fatalf("HandleUnsubscribe failed: %v", err)
	}

	// Check UNSUBACK
	if len(conn.written) != 1 {
		t.Fatalf("Expected UNSUBACK, got %d", len(conn.written))
	}
	ack, ok := conn.written[0].(*v3.UnSubAck)
	if !ok {
		t.Fatalf("Expected UnSubAck, got %T", conn.written[0])
	}
	if ack.ID != 101 {
		t.Errorf("UnSubAck ID: got %d, want 101", ack.ID)
	}

	// Verify broker routing table
	subs, _ := b.Match("home/kitchen")
	for _, sub := range subs {
		if sub.ClientID == s.ID {
			t.Error("Subscription should be removed")
		}
	}
}

func TestHandlePublishWithRetain(t *testing.T) {
	b := NewBroker(slog.Default())
	defer b.Close()

	pubS, _ := createTestSession("pub1", 4)

	pkt := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0, Retain: true},
		TopicName:   "status/online",
		Payload:     []byte("true"),
	}

	err := b.handler.HandlePublish(pubS, pkt)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}

	// Check retained message stored in broker
	retained, err := b.retained.Get("status/online")
	if err != nil {
		t.Fatalf("Retained message not found: %v", err)
	}
	if string(retained.Payload) != "true" {
		t.Errorf("Retained payload: got %s, want true", retained.Payload)
	}
}
