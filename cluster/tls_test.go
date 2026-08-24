// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster_test

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/internal/testcert"
	"github.com/stretchr/testify/require"
)

func TestClusterMutualTLSAcceptsNodeIdentityAndRejectsAnonymousClient(t *testing.T) {
	files, err := testcert.Generate(t.TempDir())
	require.NoError(t, err)
	tlsCfg := &cluster.TransportTLSConfig{CertFile: files.CertFile, KeyFile: files.KeyFile, CAFile: files.CAFile}
	serverTLS, clientTLS, err := cluster.LoadMutualTLSConfigs(tlsCfg)
	require.NoError(t, err)

	listener, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
	require.NoError(t, err)
	defer listener.Close()
	accept := func() <-chan error {
		result := make(chan error, 1)
		go func() {
			conn, acceptErr := listener.Accept()
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
	validClient := clientTLS.Clone()
	validClient.ServerName = "127.0.0.1"
	conn, err := tls.Dial("tcp", listener.Addr().String(), validClient)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.NoError(t, <-validResult)

	invalidResult := accept()
	anonymous := clientTLS.Clone()
	anonymous.Certificates = nil
	anonymous.ServerName = "127.0.0.1"
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err = tls.DialWithDialer(dialer, "tcp", listener.Addr().String(), anonymous)
	if err == nil {
		_ = conn.Close()
	}
	require.Error(t, <-invalidResult)
}
