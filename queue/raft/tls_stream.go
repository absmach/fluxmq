// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/absmach/fluxmq/cluster"
	hraft "github.com/hashicorp/raft"
)

type tlsStreamLayer struct {
	listener  net.Listener
	advertise net.Addr
	clientTLS *tls.Config
}

func newTLSStreamLayer(bindAddr string, advertise net.Addr, cfg *cluster.TransportTLSConfig) (*tlsStreamLayer, error) {
	serverTLS, clientTLS, err := cluster.LoadMutualTLSConfigs(cfg)
	if err != nil {
		return nil, err
	}
	// Queue Raft is a raw byte stream, not HTTP/2.
	serverTLS.NextProtos = nil
	clientTLS.NextProtos = nil

	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	return &tlsStreamLayer{
		listener:  tls.NewListener(listener, serverTLS),
		advertise: advertise,
		clientTLS: clientTLS,
	}, nil
}

func (l *tlsStreamLayer) Dial(address hraft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return dialRaftPeer(string(address), timeout, l.clientTLS)
}

func (l *tlsStreamLayer) Accept() (net.Conn, error) { return l.listener.Accept() }
func (l *tlsStreamLayer) Close() error              { return l.listener.Close() }
func (l *tlsStreamLayer) Addr() net.Addr {
	if l.advertise != nil {
		return l.advertise
	}
	return l.listener.Addr()
}

func dialRaftPeer(address string, timeout time.Duration, baseTLS *tls.Config) (net.Conn, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid raft peer address %q: %w", address, err)
	}
	dialer := &net.Dialer{Timeout: timeout}
	if baseTLS == nil {
		return dialer.Dial("tcp", address)
	}
	clientTLS := baseTLS.Clone()
	clientTLS.ServerName = host
	return tls.DialWithDialer(dialer, "tcp", address, clientTLS)
}
