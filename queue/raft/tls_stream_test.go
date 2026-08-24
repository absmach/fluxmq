// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/internal/testcert"
	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestTLSStreamLayerAuthenticatesRaftPeers(t *testing.T) {
	files, err := testcert.Generate(t.TempDir())
	require.NoError(t, err)
	tlsCfg := &cluster.TransportTLSConfig{CertFile: files.CertFile, KeyFile: files.KeyFile, CAFile: files.CAFile}
	stream, err := newTLSStreamLayer("127.0.0.1:0", nil, tlsCfg)
	require.NoError(t, err)
	defer stream.Close()

	accept := func() <-chan error {
		result := make(chan error, 1)
		go func() {
			conn, acceptErr := stream.Accept()
			if acceptErr != nil {
				result <- acceptErr
				return
			}
			defer conn.Close()
			result <- conn.(*tls.Conn).Handshake()
		}()
		return result
	}

	validResult := accept()
	conn, err := stream.Dial(hraft.ServerAddress(stream.Addr().String()), 2*time.Second)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.NoError(t, <-validResult)

	_, anonymousTLS, err := cluster.LoadMutualTLSConfigs(tlsCfg)
	require.NoError(t, err)
	anonymousTLS.NextProtos = nil
	anonymousTLS.Certificates = nil
	invalidResult := accept()
	conn, err = dialRaftPeer(stream.Addr().String(), 2*time.Second, anonymousTLS)
	if err == nil {
		_ = conn.Close()
	}
	require.Error(t, <-invalidResult)
}
