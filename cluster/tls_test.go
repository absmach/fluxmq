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

const tlsTestHost = "127.0.0.1"

func TestClusterMutualTLSAcceptsNodeIdentityAndRejectsAnonymousClient(t *testing.T) {
	files, err := testcert.Generate(t.TempDir())
	require.NoError(t, err)
	tlsCfg := &cluster.TransportTLSConfig{CertFile: files.CertFile, KeyFile: files.KeyFile, CAFile: files.CAFile}
	serverTLS, clientTLS, err := cluster.LoadMutualTLSConfigs(tlsCfg)
	require.NoError(t, err)

	listener, err := tls.Listen("tcp", tlsTestHost+":0", serverTLS)
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
	validClient.ServerName = tlsTestHost
	conn, err := tls.Dial("tcp", listener.Addr().String(), validClient)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.NoError(t, <-validResult)

	invalidResult := accept()
	anonymous := clientTLS.Clone()
	anonymous.Certificates = nil
	anonymous.ServerName = tlsTestHost
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err = tls.DialWithDialer(dialer, "tcp", listener.Addr().String(), anonymous)
	if err == nil {
		_ = conn.Close()
	}
	require.Error(t, <-invalidResult)
}

// A node holding a certificate from another authority is a stranger to the
// cluster whatever name it presents: neither side of the handshake may accept
// the other.
func TestClusterMutualTLSRejectsNodeFromForeignCA(t *testing.T) {
	clusterFiles, err := testcert.Generate(t.TempDir())
	require.NoError(t, err)
	rogueFiles, err := testcert.Generate(t.TempDir())
	require.NoError(t, err)

	clusterServer, clusterClient, err := cluster.LoadMutualTLSConfigs(&cluster.TransportTLSConfig{
		CertFile: clusterFiles.CertFile, KeyFile: clusterFiles.KeyFile, CAFile: clusterFiles.CAFile,
	})
	require.NoError(t, err)
	rogueServer, rogueClient, err := cluster.LoadMutualTLSConfigs(&cluster.TransportTLSConfig{
		CertFile: rogueFiles.CertFile, KeyFile: rogueFiles.KeyFile, CAFile: rogueFiles.CAFile,
	})
	require.NoError(t, err)

	tests := []struct {
		name   string
		server *tls.Config
		client *tls.Config
	}{
		{"rogue_client_dials_cluster_node", clusterServer, rogueClient},
		{"cluster_node_dials_rogue_server", rogueServer, clusterClient},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := tls.Listen("tcp", tlsTestHost+":0", tt.server)
			require.NoError(t, err)
			t.Cleanup(func() { _ = listener.Close() })

			serverResult := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					serverResult <- err
					return
				}
				defer conn.Close()
				serverResult <- conn.(*tls.Conn).Handshake()
			}()

			client := tt.client.Clone()
			client.ServerName = tlsTestHost
			dialer := &net.Dialer{Timeout: 2 * time.Second}
			conn, err := tls.DialWithDialer(dialer, "tcp", listener.Addr().String(), client)
			if err == nil {
				// TLS 1.3 clients finish before the server checks their
				// certificate; the server's verdict arrives on first read.
				_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				_, err = conn.Read(make([]byte, 1))
				_ = conn.Close()
			}
			require.Error(t, err, "client side accepted the handshake")
			require.Error(t, <-serverResult, "server side accepted the handshake")
		})
	}
}
