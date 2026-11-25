// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package faults

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/testutil"
	"github.com/stretchr/testify/require"
)

// peer is a well-behaved MQTT 5 client: a reader answers every QoS 1 and 2
// packet as soon as it arrives, so if the broker stops, the broker is the one
// that stopped.
type peer struct {
	conn    net.Conn
	writeMu sync.Mutex

	mu       sync.Mutex
	received map[string]int
	acked    map[uint16]chan struct{}
	subacked chan struct{}
	pong     chan struct{}
	nextID   atomic.Uint32
}

func dialPeer(tb testing.TB, addr, clientID string, receiveMaximum uint16) (*peer, error) {
	tb.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	tb.Cleanup(func() { _ = conn.Close() })

	expiry := uint32(0)
	connect := &v5.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ProtocolName:    "MQTT",
		ProtocolVersion: packets.V5,
		CleanStart:      true,
		KeepAlive:       60,
		ClientID:        clientID,
		Properties:      &v5.ConnectProperties{SessionExpiryInterval: &expiry, ReceiveMaximum: &receiveMaximum},
	}
	if err := connect.Pack(conn); err != nil {
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	pkt, _, _, err := v5.ReadPacket(conn)
	if err != nil {
		return nil, fmt.Errorf("read CONNACK: %w", err)
	}
	if ack, ok := pkt.(*v5.ConnAck); !ok || ack.ReasonCode != v5.ConnAckSuccess {
		return nil, fmt.Errorf("connect refused: %v", pkt)
	}
	_ = conn.SetReadDeadline(time.Time{})

	p := &peer{
		conn:     conn,
		received: make(map[string]int),
		acked:    make(map[uint16]chan struct{}),
		subacked: make(chan struct{}, 1),
		pong:     make(chan struct{}, 1),
	}
	go p.read()
	return p, nil
}

func (p *peer) send(pkt packets.ControlPacket) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_ = p.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	return pkt.Pack(p.conn)
}

func (p *peer) read() {
	for {
		pkt, _, _, err := v5.ReadPacket(p.conn)
		if err != nil {
			return
		}
		switch pkt := pkt.(type) {
		case *v5.Publish:
			p.mu.Lock()
			p.received[string(pkt.Payload)]++
			p.mu.Unlock()
			switch pkt.QoS {
			case 1:
				_ = p.send(&v5.PubAck{FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType}, ID: pkt.ID})
			case 2:
				_ = p.send(&v5.PubRec{FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType}, ID: pkt.ID})
			}
		case *v5.PubRel:
			_ = p.send(&v5.PubComp{FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType}, ID: pkt.ID})
		case *v5.PubRec:
			_ = p.send(&v5.PubRel{FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1}, ID: pkt.ID})
		case *v5.PubAck:
			p.complete(pkt.ID)
		case *v5.PubComp:
			p.complete(pkt.ID)
		case *v5.SubAck:
			p.subacked <- struct{}{}
		case *v5.PingResp:
			p.pong <- struct{}{}
		}
	}
}

// complete marks one of this client's publishes as acknowledged.
func (p *peer) complete(id uint16) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if done, ok := p.acked[id]; ok {
		close(done)
		delete(p.acked, id)
	}
}

func (p *peer) subscribe(filters ...string) error {
	return p.subscribeQoS(1, filters...)
}

func (p *peer) subscribeQoS(qos byte, filters ...string) error {
	opts := make([]v5.SubOption, len(filters))
	for i, filter := range filters {
		opts[i] = v5.SubOption{Topic: filter, MaxQoS: qos}
	}
	if err := p.send(&v5.Subscribe{FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1}, ID: 1, Opts: opts}); err != nil {
		return err
	}
	return wait(p.subacked, "SUBACK")
}

// publish sends a QoS 1 message and returns a channel closed by its PUBACK.
func (p *peer) publish(topic, payload string, retain bool) (<-chan struct{}, error) {
	return p.publishQoS(1, topic, payload, retain)
}

// publishQoS sends a QoS 1 or 2 message and returns a channel closed once the
// broker has acknowledged it: by PUBACK, or by PUBCOMP for QoS 2.
func (p *peer) publishQoS(qos byte, topic, payload string, retain bool) (<-chan struct{}, error) {
	id := uint16(p.nextID.Add(1))
	done := make(chan struct{})
	p.mu.Lock()
	p.acked[id] = done
	p.mu.Unlock()
	err := p.send(&v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: qos, Retain: retain},
		TopicName:   topic,
		ID:          id,
		Payload:     []byte(payload),
	})
	return done, err
}

func (p *peer) ping() error {
	if err := p.send(&v5.PingReq{FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType}}); err != nil {
		return err
	}
	return wait(p.pong, "PINGRESP")
}

func (p *peer) count(payload string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.received[payload]
}

const deadlockWait = 5 * time.Second

func wait(ch <-chan struct{}, what string) error {
	select {
	case <-ch:
		return nil
	case <-time.After(deadlockWait):
		return fmt.Errorf("no %s within %v", what, deadlockWait)
	}
}

// scenario is a set of well-behaved clients publishing QoS 1 and 2 messages
// to one another, themselves included, each with its own Receive Maximum.
type scenario struct {
	receiveMaximum []uint16
	subscribesTo   [][]int // subscribesTo[i] lists the publishers client i follows
	publishes      []int   // publishes[i] is how many messages client i sends
	publishQoS     []byte  // the QoS client i publishes at
	subscribeQoS   []byte  // the maximum QoS client i subscribes with
}

// decodeScenario turns fuzz input into 2–4 clients with Receive Maximum 1–3,
// any subscription graph, 1–6 messages each, and QoS 1 or 2 on each side.
func decodeScenario(data []byte) scenario {
	at := func(i int) byte {
		if i < len(data) {
			return data[i]
		}
		return byte(i * 37)
	}
	n := 2 + int(at(0))%3
	sc := scenario{
		receiveMaximum: make([]uint16, n),
		subscribesTo:   make([][]int, n),
		publishes:      make([]int, n),
		publishQoS:     make([]byte, n),
		subscribeQoS:   make([]byte, n),
	}
	for i := range n {
		sc.receiveMaximum[i] = 1 + uint16(at(1+i))%3
		sc.publishes[i] = 1 + int(at(5+i))%6
		qos := at(13 + i)
		sc.publishQoS[i] = 1 + qos&1
		sc.subscribeQoS[i] = 1 + (qos>>1)&1
		mask := at(9 + i)
		for j := range n {
			if mask&(1<<j) != 0 {
				sc.subscribesTo[i] = append(sc.subscribesTo[i], j)
			}
		}
	}
	return sc
}

var scenarioRun atomic.Int64

// run plays the scenario on the broker at addr and reports the first thing
// that did not happen in time: an acknowledgement, a delivery, or a ping.
func (sc scenario) run(tb testing.TB, addr string) error {
	tb.Helper()
	return sc.runAcross(tb, []string{addr})
}

// runAcross is run with client i connected to addrs[i % len(addrs)].
func (sc scenario) runAcross(tb testing.TB, addrs []string) error {
	tb.Helper()

	run := scenarioRun.Add(1)
	topic := func(i int) string { return fmt.Sprintf("dl/%d/%d", run, i) }
	peers := make([]*peer, len(sc.receiveMaximum))
	for i := range peers {
		p, err := dialPeer(tb, addrs[i%len(addrs)], fmt.Sprintf("dl-%d-%d", run, i), sc.receiveMaximum[i])
		if err != nil {
			return err
		}
		peers[i] = p
	}
	for i, p := range peers {
		filters := make([]string, 0, len(sc.subscribesTo[i]))
		for _, j := range sc.subscribesTo[i] {
			filters = append(filters, topic(j))
		}
		if len(filters) == 0 {
			continue
		}
		if err := p.subscribeQoS(sc.subscribeQoS[i], filters...); err != nil {
			return fmt.Errorf("client %d: %w", i, err)
		}
	}

	// Everyone publishes at once, as fast as the broker takes it.
	var acks []<-chan struct{}
	for round := range 6 {
		for i, p := range peers {
			if round >= sc.publishes[i] {
				continue
			}
			done, err := p.publishQoS(sc.publishQoS[i], topic(i), fmt.Sprintf("%d-%d", i, round), false)
			if err != nil {
				return fmt.Errorf("client %d publish: %w", i, err)
			}
			acks = append(acks, done)
		}
	}
	for _, done := range acks {
		if err := wait(done, "PUBACK"); err != nil {
			return err
		}
	}

	deadline := time.Now().Add(deadlockWait)
	for i, p := range peers {
		for _, j := range sc.subscribesTo[i] {
			for round := range sc.publishes[j] {
				payload := fmt.Sprintf("%d-%d", j, round)
				for p.count(payload) == 0 {
					if time.Now().After(deadline) {
						return fmt.Errorf("client %d never received %s", i, payload)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}
		if err := p.ping(); err != nil {
			return fmt.Errorf("client %d: %w", i, err)
		}
	}
	return nil
}

// FuzzQoSExchangeNeverStalls has clients publish QoS 1 and 2 messages to
// each other and to themselves with small Receive Maximums. Every client
// acknowledges promptly, so every message must be acknowledged and delivered
// and every connection must keep answering pings. A delivery that waits for
// quota on a connection's read loop would stop that loop from reading the
// acknowledgements that free the quota.
func FuzzQoSExchangeNeverStalls(f *testing.F) {
	for _, seed := range [][]byte{
		{0, 0, 0, 0, 0, 5, 5, 0, 0, 0b01, 0b10},                     // two clients, each following itself
		{0, 0, 0, 0, 0, 5, 5, 0, 0, 0b10, 0b01},                     // two clients following each other
		{1, 0, 1, 2, 0, 5, 3, 2, 0, 0b111, 0b111},                   // three clients, everyone follows everyone
		{2, 0, 0, 0, 0, 1, 1, 1, 1, 0b0010, 0b0100, 0b1000, 0b0001}, // a ring of four
		{0, 0, 0, 0, 0, 5, 5, 0, 0, 0b11, 0b11, 0, 0, 0b11, 0b11},   // two clients, QoS 2 both ways
	} {
		f.Add(seed)
	}

	lb := testutil.StartLocalBroker(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		sc := decodeScenario(data)
		if err := sc.run(t, lb.TCPAddr); err != nil {
			t.Fatalf("%+v: %v", sc, err)
		}
	})
}

// Retained messages matching a new subscription are delivered while the
// SUBSCRIBE is being handled. More of them than the subscriber's Receive
// Maximum must still all arrive.
func TestRetainedBeyondReceiveMaximum(t *testing.T) {
	lb := testutil.StartLocalBroker(t)

	publisher, err := dialPeer(t, lb.TCPAddr, "retained-pub", 10)
	if err != nil {
		t.Fatal(err)
	}
	const count = 5
	for i := range count {
		done, err := publisher.publish(fmt.Sprintf("kept/%d", i), fmt.Sprintf("kept-%d", i), true)
		if err != nil {
			t.Fatal(err)
		}
		if err := wait(done, "PUBACK"); err != nil {
			t.Fatal(err)
		}
	}

	subscriber, err := dialPeer(t, lb.TCPAddr, "retained-sub", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := subscriber.subscribe("kept/#"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(deadlockWait)
	for i := range count {
		for subscriber.count(fmt.Sprintf("kept-%d", i)) == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("retained kept-%d never arrived", i)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	if err := subscriber.ping(); err != nil {
		t.Fatal(err)
	}
}

// The same cycle across nodes: each client's publish is forwarded to the
// other's node while that node's read loop waits on its own forward. A
// delivery served for a remote node that waited for quota would hold both
// until the forwards timed out.
func TestCrossNodeExchangeNeverStalls(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a two-node cluster")
	}
	tc := testutil.NewTestCluster(t, 2)
	require.NoError(t, tc.Start())
	t.Cleanup(tc.Stop)
	require.NoError(t, tc.WaitForClusterReady(30*time.Second))

	sc := scenario{
		receiveMaximum: []uint16{1, 1},
		subscribesTo:   [][]int{{1}, {0}},
		publishes:      []int{6, 6},
		publishQoS:     []byte{1, 1},
		subscribeQoS:   []byte{1, 1},
	}
	require.NoError(t, sc.runAcross(t, []string{tc.Nodes[0].TCPAddr, tc.Nodes[1].TCPAddr}))
}
