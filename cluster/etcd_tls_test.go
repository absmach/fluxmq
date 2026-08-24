// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/absmach/fluxmq/internal/testcert"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedEtcdRejectsNonLoopbackClientAddress(t *testing.T) {
	_, err := NewEtcdCluster(&EtcdConfig{
		NodeID:         "unsafe-etcd-node",
		DataDir:        t.TempDir(),
		BindAddr:       "127.0.0.1:2380",
		ClientAddr:     "0.0.0.0:2379",
		InitialCluster: "unsafe-etcd-node=http://127.0.0.1:2380",
		AllowInsecure:  true,
	}, memory.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.ErrorContains(t, err, "loopback-only")
}

func TestEmbeddedEtcdUsesSharedClusterTLS(t *testing.T) {
	files, err := testcert.Generate(t.TempDir())
	require.NoError(t, err)
	peerPort := freeLocalPort(t)
	clientPort := freeLocalPort(t)
	const nodeID = "secure-etcd-node"

	c, err := NewEtcdCluster(&EtcdConfig{
		NodeID:         nodeID,
		DataDir:        filepath.Join(t.TempDir(), "etcd"),
		BindAddr:       fmt.Sprintf("127.0.0.1:%d", peerPort),
		ClientAddr:     fmt.Sprintf("127.0.0.1:%d", clientPort),
		AdvertiseAddr:  fmt.Sprintf("127.0.0.1:%d", peerPort),
		InitialCluster: fmt.Sprintf("%s=https://127.0.0.1:%d", nodeID, peerPort),
		Bootstrap:      true,
		TransportTLS: &TransportTLSConfig{
			CertFile: files.CertFile,
			KeyFile:  files.KeyFile,
			CAFile:   files.CAFile,
		},
	}, memory.New(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Stop() })
	require.NoError(t, c.Start())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, c.AcquireSession(ctx, "secure-client", nodeID))
	owner, ok, err := c.GetSessionOwner(ctx, "secure-client")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, nodeID, owner)
}
