package handlers

import (
	"net"
	"testing"
	"time"

	"github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/memory"
)

// mockConnection implements session.Connection for testing.
type mockConnection struct {
	closed      bool
	written     []packets.ControlPacket
	readCh      chan packets.ControlPacket
	errOnWrite  error
	errOnRead   error
	writeDelay  time.Duration
}

func newMockConnection() *mockConnection {
	return &mockConnection{
		readCh: make(chan packets.ControlPacket, 10),
	}
}

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

// mockRouter implements Router for testing.
type mockRouter struct {
	subscriptions map[string][]*store.Subscription
}

func newMockRouter() *mockRouter {
	return &mockRouter{
		subscriptions: make(map[string][]*store.Subscription),
	}
}

func (r *mockRouter) Subscribe(clientID string, filter string, qos byte, opts store.SubscribeOptions) error {
	r.subscriptions[filter] = append(r.subscriptions[filter], &store.Subscription{
		ClientID: clientID,
		Filter:   filter,
		QoS:      qos,
		Options:  opts,
	})
	return nil
}

func (r *mockRouter) Unsubscribe(clientID string, filter string) error {
	delete(r.subscriptions, filter)
	return nil
}

func (r *mockRouter) Match(topic string) ([]*store.Subscription, error) {
	var result []*store.Subscription
	for _, subs := range r.subscriptions {
		result = append(result, subs...)
	}
	return result, nil
}

// mockPublisher implements Publisher for testing.
type mockPublisher struct {
	published []struct {
		topic   string
		payload []byte
		qos     byte
		retain  bool
	}
}

func (p *mockPublisher) Publish(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
	p.published = append(p.published, struct {
		topic   string
		payload []byte
		qos     byte
		retain  bool
	}{topic, payload, qos, retain})
	return nil
}

func createTestSession(version byte) (*session.Session, *mockConnection) {
	conn := newMockConnection()
	opts := session.Options{
		CleanStart:     true,
		ReceiveMaximum: 100,
		KeepAlive:      60,
	}
	sess := session.New("test-client", version, opts)
	sess.Connect(conn)
	return sess, conn
}

func TestHandlePublishQoS0(t *testing.T) {
	pub := &mockPublisher{}
	h := NewBrokerHandler(BrokerHandlerConfig{
		Publisher: pub,
	})

	sess, _ := createTestSession(4)

	pkt := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "test/topic",
		Payload:     []byte("hello"),
	}

	err := h.HandlePublish(sess, pkt)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}

	// Should publish to subscribers
	if len(pub.published) != 1 {
		t.Errorf("Expected 1 publish, got %d", len(pub.published))
	}
	if pub.published[0].topic != "test/topic" {
		t.Errorf("Topic mismatch: got %s", pub.published[0].topic)
	}
}

func TestHandlePublishQoS1(t *testing.T) {
	pub := &mockPublisher{}
	h := NewBrokerHandler(BrokerHandlerConfig{
		Publisher: pub,
	})

	sess, conn := createTestSession(4)

	pkt := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
		TopicName:   "test/topic",
		Payload:     []byte("hello"),
		ID:          123,
	}

	err := h.HandlePublish(sess, pkt)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}

	// Should publish and send PUBACK
	if len(pub.published) != 1 {
		t.Errorf("Expected 1 publish, got %d", len(pub.published))
	}

	if len(conn.written) != 1 {
		t.Fatalf("Expected 1 packet written, got %d", len(conn.written))
	}

	ack, ok := conn.written[0].(*v3.PubAck)
	if !ok {
		t.Fatalf("Expected PubAck, got %T", conn.written[0])
	}
	if ack.ID != 123 {
		t.Errorf("PubAck ID: got %d, want 123", ack.ID)
	}
}

func TestHandlePublishQoS2(t *testing.T) {
	pub := &mockPublisher{}
	h := NewBrokerHandler(BrokerHandlerConfig{
		Publisher: pub,
	})

	sess, conn := createTestSession(4)

	// Step 1: PUBLISH
	pkt := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 2},
		TopicName:   "test/topic",
		Payload:     []byte("hello"),
		ID:          456,
	}

	err := h.HandlePublish(sess, pkt)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}

	// Should NOT publish yet, just send PUBREC
	if len(pub.published) != 0 {
		t.Errorf("Should not publish before PUBREL, got %d", len(pub.published))
	}

	if len(conn.written) != 1 {
		t.Fatalf("Expected 1 packet written, got %d", len(conn.written))
	}

	rec, ok := conn.written[0].(*v3.PubRec)
	if !ok {
		t.Fatalf("Expected PubRec, got %T", conn.written[0])
	}
	if rec.ID != 456 {
		t.Errorf("PubRec ID: got %d, want 456", rec.ID)
	}

	// Step 2: PUBREL
	pubrel := &v3.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          456,
	}

	err = h.HandlePubRel(sess, pubrel)
	if err != nil {
		t.Fatalf("HandlePubRel failed: %v", err)
	}

	// NOW should publish
	if len(pub.published) != 1 {
		t.Errorf("Expected 1 publish after PUBREL, got %d", len(pub.published))
	}

	// Should send PUBCOMP
	if len(conn.written) != 2 {
		t.Fatalf("Expected 2 packets written, got %d", len(conn.written))
	}

	comp, ok := conn.written[1].(*v3.PubComp)
	if !ok {
		t.Fatalf("Expected PubComp, got %T", conn.written[1])
	}
	if comp.ID != 456 {
		t.Errorf("PubComp ID: got %d, want 456", comp.ID)
	}
}

func TestHandleSubscribe(t *testing.T) {
	router := newMockRouter()
	h := NewBrokerHandler(BrokerHandlerConfig{
		Router: router,
	})

	sess, conn := createTestSession(4)

	pkt := &v3.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType},
		ID:          789,
		Topics: []v3.Topic{
			{Name: "home/+/temp", QoS: 1},
			{Name: "sensors/#", QoS: 2},
		},
	}

	err := h.HandleSubscribe(sess, pkt)
	if err != nil {
		t.Fatalf("HandleSubscribe failed: %v", err)
	}

	// Check router subscriptions
	if len(router.subscriptions) != 2 {
		t.Errorf("Expected 2 subscriptions, got %d", len(router.subscriptions))
	}

	// Check SUBACK
	if len(conn.written) != 1 {
		t.Fatalf("Expected 1 packet written, got %d", len(conn.written))
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

	// Check session subscriptions cached
	subs := sess.GetSubscriptions()
	if len(subs) != 2 {
		t.Errorf("Expected 2 cached subscriptions, got %d", len(subs))
	}
}

func TestHandleUnsubscribe(t *testing.T) {
	router := newMockRouter()
	h := NewBrokerHandler(BrokerHandlerConfig{
		Router: router,
	})

	sess, conn := createTestSession(4)

	// First subscribe
	router.Subscribe(sess.ID, "home/#", 1, store.SubscribeOptions{})
	sess.AddSubscription("home/#", store.SubscribeOptions{})

	// Then unsubscribe
	pkt := &v3.Unsubscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubscribeType},
		ID:          101,
		Topics:      []string{"home/#"},
	}

	err := h.HandleUnsubscribe(sess, pkt)
	if err != nil {
		t.Fatalf("HandleUnsubscribe failed: %v", err)
	}

	// Check router
	if len(router.subscriptions) != 0 {
		t.Errorf("Expected 0 subscriptions, got %d", len(router.subscriptions))
	}

	// Check UNSUBACK
	if len(conn.written) != 1 {
		t.Fatalf("Expected 1 packet written, got %d", len(conn.written))
	}

	ack, ok := conn.written[0].(*v3.UnSubAck)
	if !ok {
		t.Fatalf("Expected UnsubAck, got %T", conn.written[0])
	}
	if ack.ID != 101 {
		t.Errorf("UnsubAck ID: got %d, want 101", ack.ID)
	}

	// Check session
	subs := sess.GetSubscriptions()
	if len(subs) != 0 {
		t.Errorf("Expected 0 cached subscriptions, got %d", len(subs))
	}
}

func TestHandlePingReq(t *testing.T) {
	h := NewBrokerHandler(BrokerHandlerConfig{})

	sess, conn := createTestSession(4)

	err := h.HandlePingReq(sess)
	if err != nil {
		t.Fatalf("HandlePingReq failed: %v", err)
	}

	if len(conn.written) != 1 {
		t.Fatalf("Expected 1 packet written, got %d", len(conn.written))
	}

	_, ok := conn.written[0].(*v3.PingResp)
	if !ok {
		t.Fatalf("Expected PingResp, got %T", conn.written[0])
	}
}

func TestHandleDisconnect(t *testing.T) {
	h := NewBrokerHandler(BrokerHandlerConfig{})

	sess, _ := createTestSession(4)

	pkt := &v3.Disconnect{
		FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
	}

	err := h.HandleDisconnect(sess, pkt)
	if err != nil {
		t.Fatalf("HandleDisconnect failed: %v", err)
	}

	if sess.IsConnected() {
		t.Error("Session should be disconnected")
	}
}

func TestHandlePublishWithRetain(t *testing.T) {
	st := memory.New()
	h := NewBrokerHandler(BrokerHandlerConfig{
		Retained: st.Retained(),
	})

	sess, _ := createTestSession(4)

	pkt := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0, Retain: true},
		TopicName:   "status/online",
		Payload:     []byte("true"),
	}

	err := h.HandlePublish(sess, pkt)
	if err != nil {
		t.Fatalf("HandlePublish failed: %v", err)
	}

	// Check retained message stored
	msg, err := st.Retained().Get("status/online")
	if err != nil {
		t.Fatalf("Get retained failed: %v", err)
	}
	if string(msg.Payload) != "true" {
		t.Errorf("Retained payload: got %s, want true", msg.Payload)
	}

	// Clear retained with empty payload
	pkt2 := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0, Retain: true},
		TopicName:   "status/online",
		Payload:     []byte{},
	}

	h.HandlePublish(sess, pkt2)

	_, err = st.Retained().Get("status/online")
	if err != store.ErrNotFound {
		t.Errorf("Expected ErrNotFound after clear, got %v", err)
	}
}

func TestDispatcher(t *testing.T) {
	pub := &mockPublisher{}
	h := NewBrokerHandler(BrokerHandlerConfig{
		Publisher: pub,
	})
	d := NewDispatcher(h)

	sess, conn := createTestSession(4)

	// Test PUBLISH dispatch
	pubPkt := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 0},
		TopicName:   "test",
		Payload:     []byte("data"),
	}

	err := d.Dispatch(sess, pubPkt)
	if err != nil {
		t.Fatalf("Dispatch PUBLISH failed: %v", err)
	}

	// Test PINGREQ dispatch
	pingPkt := &v3.PingReq{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType},
	}

	err = d.Dispatch(sess, pingPkt)
	if err != nil {
		t.Fatalf("Dispatch PINGREQ failed: %v", err)
	}

	if len(conn.written) != 1 {
		t.Errorf("Expected 1 packet (PINGRESP), got %d", len(conn.written))
	}
}
