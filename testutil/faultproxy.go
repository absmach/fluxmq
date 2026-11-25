// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"net"
	"sync"
	"testing"
)

// FaultProxy is a TCP proxy that sits between a client and a server so a
// test can break the network between them on demand: cut every connection,
// silently drop traffic as a dead link would, or stop reading the server's
// side so its writes back up as they would behind a stalled client.
//
// A FaultProxy is safe for concurrent use.
type FaultProxy struct {
	listener net.Listener
	target   string

	mu        sync.Mutex
	links     map[*proxyLink]struct{}
	blackhole bool
	stall     chan struct{} // closed while server-to-client traffic flows
	closed    bool
}

type proxyLink struct {
	client, server net.Conn
}

// NewFaultProxy starts a proxy to target on a free loopback port. It is closed
// when the test ends.
func NewFaultProxy(t testing.TB, target string) *FaultProxy {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fault proxy listen: %v", err)
	}
	flowing := make(chan struct{})
	close(flowing)
	p := &FaultProxy{
		listener: listener,
		target:   target,
		links:    make(map[*proxyLink]struct{}),
		stall:    flowing,
	}
	go p.accept()
	t.Cleanup(p.Close)
	return p
}

// Addr is the address clients dial instead of the target.
func (p *FaultProxy) Addr() string {
	return p.listener.Addr().String()
}

// Cut resets every open connection, as a crashed peer or a dropped route
// would. New connections are still accepted.
func (p *FaultProxy) Cut() {
	p.mu.Lock()
	links := p.links
	p.links = make(map[*proxyLink]struct{})
	p.mu.Unlock()

	for link := range links {
		link.reset()
	}
}

// Blackhole drops traffic in both directions while keeping every socket open,
// so neither end learns that the other has gone. Restore undoes it.
func (p *FaultProxy) Blackhole() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blackhole = true
}

// StallClient stops reading what the server sends, so the server's writes
// fill the socket buffers and then block. Restore undoes it.
func (p *FaultProxy) StallClient() {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.stall:
		p.stall = make(chan struct{})
	default:
	}
}

// Restore resumes normal forwarding after Blackhole or StallClient.
func (p *FaultProxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blackhole = false
	select {
	case <-p.stall:
	default:
		close(p.stall)
	}
}

// Close stops accepting connections and resets the open ones.
func (p *FaultProxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()

	_ = p.listener.Close()
	p.Restore()
	p.Cut()
}

func (p *FaultProxy) accept() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}

		link := &proxyLink{client: client, server: server}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			link.reset()
			return
		}
		p.links[link] = struct{}{}
		p.mu.Unlock()

		go p.pump(link, client, server, false)
		go p.pump(link, server, client, true)
	}
}

// pump copies src to dst until either side closes. toClient marks the
// server-to-client direction, the one StallClient holds back.
func (p *FaultProxy) pump(link *proxyLink, src, dst net.Conn, toClient bool) {
	defer link.reset()

	buf := make([]byte, 32<<10)
	for {
		if toClient {
			p.mu.Lock()
			stall := p.stall
			p.mu.Unlock()
			<-stall
		}

		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			drop := p.blackhole
			p.mu.Unlock()
			if !drop {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
		}
		if err != nil {
			// EOF, a reset, or a Cut: the link is over either way.
			return
		}
	}
}

// reset closes both ends with a TCP reset rather than an orderly close.
func (l *proxyLink) reset() {
	for _, conn := range []net.Conn{l.client, l.server} {
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = conn.Close()
	}
}
