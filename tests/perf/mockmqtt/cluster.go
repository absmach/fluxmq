// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mockmqtt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/topics"
)

var defaultAddrs = []string{
	"127.0.0.1:1883",
	"127.0.0.1:1885",
	"127.0.0.1:1887",
}

// Cluster is a lightweight in-memory MQTT mock that exposes multiple TCP listeners
// and shares subscriptions across all listeners (cluster-like behavior).
type Cluster struct {
	addrs []string

	mu        sync.RWMutex
	listeners []net.Listener
	clients   map[*clientConn]struct{}

	started atomic.Bool
	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

type clientConn struct {
	conn     net.Conn
	protocol byte
	clientID string

	writeMu sync.Mutex
	subMu   sync.RWMutex
	subs    map[string]byte
}

// New creates a new mock cluster.
// When addrs is empty, default MQTT v3 node ports are used (1883, 1885, 1887).
func New(addrs []string) *Cluster {
	if len(addrs) == 0 {
		addrs = append([]string(nil), defaultAddrs...)
	} else {
		addrs = append([]string(nil), addrs...)
	}

	return &Cluster{
		addrs:   addrs,
		clients: make(map[*clientConn]struct{}),
		closeCh: make(chan struct{}),
	}
}

// Start begins listening on all configured addresses.
func (c *Cluster) Start(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return errors.New("mockmqtt cluster already started")
	}

	listeners := make([]net.Listener, 0, len(c.addrs))
	for _, addr := range c.addrs {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			for _, l := range listeners {
				_ = l.Close()
			}
			return fmt.Errorf("failed to listen on %s: %w", addr, err)
		}
		listeners = append(listeners, ln)
	}

	c.mu.Lock()
	c.listeners = listeners
	c.mu.Unlock()

	if ctx != nil {
		go func() {
			select {
			case <-ctx.Done():
				_ = c.Close()
			case <-c.closeCh:
			}
		}()
	}

	for _, ln := range listeners {
		c.wg.Add(1)
		go c.acceptLoop(ln)
	}

	return nil
}

// Addrs returns currently bound listener addresses.
func (c *Cluster) Addrs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	addrs := make([]string, 0, len(c.listeners))
	for _, ln := range c.listeners {
		addrs = append(addrs, ln.Addr().String())
	}
	if len(addrs) > 0 {
		return addrs
	}
	return append([]string(nil), c.addrs...)
}

// Close stops listeners and disconnects all clients.
func (c *Cluster) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.closeCh)

	c.mu.Lock()
	listeners := c.listeners
	c.listeners = nil
	clients := make([]*clientConn, 0, len(c.clients))
	for cc := range c.clients {
		clients = append(clients, cc)
	}
	c.clients = make(map[*clientConn]struct{})
	c.mu.Unlock()

	var errs []error
	for _, ln := range listeners {
		if ln == nil {
			continue
		}
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}
	for _, cc := range clients {
		if cc == nil || cc.conn == nil {
			continue
		}
		if err := cc.conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}

	c.wg.Wait()
	return errors.Join(errs...)
}

func (c *Cluster) acceptLoop(ln net.Listener) {
	defer c.wg.Done()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if c.closed.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.serveConn(conn)
		}()
	}
}

func (c *Cluster) serveConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()

	proto, clientID, err := handshake(conn)
	if err != nil {
		return
	}

	cc := &clientConn{
		conn:     conn,
		protocol: proto,
		clientID: clientID,
		subs:     make(map[string]byte),
	}

	c.mu.Lock()
	c.clients[cc] = struct{}{}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.clients, cc)
		c.mu.Unlock()
	}()

	switch proto {
	case packets.V5:
		c.serveV5(cc)
	default:
		c.serveV3(cc)
	}
}

func (c *Cluster) serveV3(cc *clientConn) {
	for {
		pkt, err := v3.ReadPacket(cc.conn)
		if err != nil {
			return
		}

		switch p := pkt.(type) {
		case *v3.Subscribe:
			cc.setSubsV3(p.Topics)
			codes := make([]byte, len(p.Topics))
			for i, t := range p.Topics {
				if t.QoS > 2 {
					codes[i] = v3.SubAckFailure
					continue
				}
				codes[i] = t.QoS
			}
			ack := &v3.SubAck{
				FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
				ID:          p.ID,
				ReturnCodes: codes,
			}
			if err := cc.send(ack.Encode()); err != nil {
				return
			}
		case *v3.Unsubscribe:
			cc.removeSubs(p.Topics)
			ack := &v3.UnSubAck{
				FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
				ID:          p.ID,
			}
			if err := cc.send(ack.Encode()); err != nil {
				return
			}
		case *v3.Publish:
			c.route(p.TopicName, p.Payload)
			switch p.QoS {
			case 1:
				ack := &v3.PubAck{
					FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
					ID:          p.ID,
				}
				if err := cc.send(ack.Encode()); err != nil {
					return
				}
			case 2:
				rec := &v3.PubRec{
					FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
					ID:          p.ID,
				}
				if err := cc.send(rec.Encode()); err != nil {
					return
				}
			}
		case *v3.PubRel:
			comp := &v3.PubComp{
				FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
				ID:          p.ID,
			}
			if err := cc.send(comp.Encode()); err != nil {
				return
			}
		case *v3.PingReq:
			resp := &v3.PingResp{
				FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
			}
			if err := cc.send(resp.Encode()); err != nil {
				return
			}
		case *v3.Disconnect:
			return
		}
	}
}

func (c *Cluster) serveV5(cc *clientConn) {
	for {
		pkt, _, _, err := v5.ReadPacket(cc.conn)
		if err != nil {
			return
		}

		switch p := pkt.(type) {
		case *v5.Subscribe:
			cc.setSubsV5(p.Opts)
			reasonCodes := make([]byte, len(p.Opts))
			for i, opt := range p.Opts {
				if opt.MaxQoS > 2 {
					reasonCodes[i] = v5.SubAckTopicFilterInvalid
					continue
				}
				reasonCodes[i] = opt.MaxQoS
			}
			ack := &v5.SubAck{
				FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
				ID:          p.ID,
				ReasonCodes: &reasonCodes,
			}
			if err := cc.send(ack.Encode()); err != nil {
				return
			}
		case *v5.Unsubscribe:
			cc.removeSubs(p.Topics)
			reasonCodes := make([]byte, len(p.Topics))
			for i := range p.Topics {
				reasonCodes[i] = v5.UnsubAckSuccess
			}
			ack := &v5.UnsubAck{
				FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
				ID:          p.ID,
				ReasonCodes: &reasonCodes,
			}
			if err := cc.send(ack.Encode()); err != nil {
				return
			}
		case *v5.Publish:
			c.route(p.TopicName, p.Payload)
			switch p.QoS {
			case 1:
				rc := byte(v5.PubAckSuccess)
				ack := &v5.PubAck{
					FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
					ID:          p.ID,
					ReasonCode:  &rc,
				}
				if err := cc.send(ack.Encode()); err != nil {
					return
				}
			case 2:
				rc := byte(v5.PubRecSuccess)
				rec := &v5.PubRec{
					FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
					ID:          p.ID,
					ReasonCode:  &rc,
				}
				if err := cc.send(rec.Encode()); err != nil {
					return
				}
			}
		case *v5.PubRel:
			rc := byte(0)
			comp := &v5.PubComp{
				FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
				ID:          p.ID,
				ReasonCode:  &rc,
			}
			if err := cc.send(comp.Encode()); err != nil {
				return
			}
		case *v5.PingReq:
			resp := &v5.PingResp{
				FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
			}
			if err := cc.send(resp.Encode()); err != nil {
				return
			}
		case *v5.Disconnect:
			return
		}
	}
}

func (c *Cluster) route(topic string, payload []byte) {
	c.mu.RLock()
	clients := make([]*clientConn, 0, len(c.clients))
	for cc := range c.clients {
		clients = append(clients, cc)
	}
	c.mu.RUnlock()

	for _, cc := range clients {
		if cc == nil || !cc.matches(topic) {
			continue
		}
		if cc.protocol == packets.V5 {
			msg := &v5.Publish{
				FixedHeader: packets.FixedHeader{
					PacketType: packets.PublishType,
					QoS:        0,
				},
				TopicName: topic,
				Payload:   payload,
			}
			_ = cc.send(msg.Encode())
			continue
		}

		msg := &v3.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        0,
			},
			TopicName: topic,
			Payload:   payload,
		}
		_ = cc.send(msg.Encode())
	}
}

func handshake(conn net.Conn) (byte, string, error) {
	fh, body, err := readRawPacket(conn)
	if err != nil {
		return 0, "", err
	}
	if fh.PacketType != packets.ConnectType {
		return 0, "", fmt.Errorf("first packet must be CONNECT, got type %d", fh.PacketType)
	}

	proto, err := connectProtocolVersion(body)
	if err != nil {
		return 0, "", err
	}

	switch proto {
	case packets.V5:
		pkt := &v5.Connect{FixedHeader: fh}
		if err := pkt.Unpack(bytes.NewReader(body)); err != nil {
			return 0, "", err
		}
		ack := &v5.ConnAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.ConnAckType},
			ReasonCode:  v5.ConnAckSuccess,
		}
		if _, err := conn.Write(ack.Encode()); err != nil {
			return 0, "", err
		}
		return packets.V5, pkt.ClientID, nil
	case packets.V311, packets.V31:
		pkt := &v3.Connect{FixedHeader: fh}
		if err := pkt.Unpack(bytes.NewReader(body)); err != nil {
			return 0, "", err
		}
		ack := &v3.ConnAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.ConnAckType},
			ReturnCode:  v3.ConnAckAccepted,
		}
		if _, err := conn.Write(ack.Encode()); err != nil {
			return 0, "", err
		}
		return packets.V311, pkt.ClientID, nil
	default:
		return 0, "", fmt.Errorf("unsupported mqtt protocol version %d", proto)
	}
}

func readRawPacket(r io.Reader) (packets.FixedHeader, []byte, error) {
	var fh packets.FixedHeader
	first := make([]byte, 1)
	if _, err := io.ReadFull(r, first); err != nil {
		return fh, nil, err
	}
	if err := fh.Decode(first[0], r); err != nil {
		return fh, nil, err
	}

	body := make([]byte, fh.RemainingLength)
	if _, err := io.ReadFull(r, body); err != nil {
		return fh, nil, err
	}

	return fh, body, nil
}

func connectProtocolVersion(body []byte) (byte, error) {
	if len(body) < 4 {
		return 0, errors.New("connect payload too short")
	}

	protoNameLen := int(binary.BigEndian.Uint16(body[0:2]))
	minLen := 2 + protoNameLen + 1
	if len(body) < minLen {
		return 0, errors.New("connect payload missing protocol level")
	}

	return body[2+protoNameLen], nil
}

func (cc *clientConn) send(data []byte) error {
	cc.writeMu.Lock()
	defer cc.writeMu.Unlock()
	_, err := cc.conn.Write(data)
	return err
}

func (cc *clientConn) setSubsV3(topics []v3.Topic) {
	cc.subMu.Lock()
	defer cc.subMu.Unlock()
	for _, t := range topics {
		if t.Name == "" {
			continue
		}
		cc.subs[t.Name] = t.QoS
	}
}

func (cc *clientConn) setSubsV5(opts []v5.SubOption) {
	cc.subMu.Lock()
	defer cc.subMu.Unlock()
	for _, opt := range opts {
		if opt.Topic == "" {
			continue
		}
		cc.subs[opt.Topic] = opt.MaxQoS
	}
}

func (cc *clientConn) removeSubs(topics []string) {
	cc.subMu.Lock()
	defer cc.subMu.Unlock()
	for _, topic := range topics {
		delete(cc.subs, topic)
	}
}

func (cc *clientConn) matches(topic string) bool {
	cc.subMu.RLock()
	defer cc.subMu.RUnlock()

	if len(cc.subs) == 0 {
		return false
	}

	for filter := range cc.subs {
		if topics.TopicMatch(filter, topic) {
			return true
		}
	}
	return false
}
