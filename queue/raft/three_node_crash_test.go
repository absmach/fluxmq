// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/absmach/fluxmq/internal/testcert"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/require"
)

const (
	crashHelperMode = "FLUXMQ_RAFT_CRASH_HELPER_MODE"
	crashHelperDir  = "FLUXMQ_RAFT_CRASH_HELPER_DIR"
	crashHelperAddr = "FLUXMQ_RAFT_CRASH_HELPER_ADDRS"
)

// The parent kills the process containing all three nodes after the append
// returns success, then opens the same disks in a fresh process. This tests
// process-crash recovery, not power loss of the host or its page cache.
func TestThreeNodeRaftRecoversAfterAbruptProcessExit(t *testing.T) {
	root, addresses := threeNodeTestDirs(t)
	_, err := testcert.Generate(root)
	require.NoError(t, err)
	env := []string{
		crashHelperDir + "=" + root,
		crashHelperAddr + "=" + strings.Join(addresses[:], ","),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	writer := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestThreeNodeRaftCrashHelper$")
	writer.Env = append(os.Environ(), append(env, crashHelperMode+"=write")...)
	var writerErr bytes.Buffer
	writer.Stderr = &writerErr
	stdout, err := writer.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, writer.Start())
	t.Cleanup(func() {
		if writer.Process != nil && writer.ProcessState == nil {
			_ = writer.Process.Kill()
			_ = writer.Wait()
		}
	})

	line, err := bufio.NewReader(stdout).ReadString('\n')
	// The child is still alive here, so its stderr buffer is still being
	// written. Do not inspect it until Wait has joined the copy goroutine.
	require.NoError(t, err, "writer did not acknowledge append")
	require.Equal(t, "ACKED\n", line, "unexpected writer output")
	require.NoError(t, writer.Process.Kill())
	_ = writer.Wait() // SIGKILL is the expected process exit.
	require.Empty(t, writerErr.String(), "writer reported an error before the crash")

	recovery := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestThreeNodeRaftCrashHelper$")
	recovery.Env = append(os.Environ(), append(env, crashHelperMode+"=recover")...)
	output, err := recovery.CombinedOutput()
	require.NoError(t, err, "recovery failed: %s", output)
}

// TestThreeNodeRaftCrashHelper is invoked only by the parent above. Running
// the package normally leaves it inert.
func TestThreeNodeRaftCrashHelper(t *testing.T) {
	mode := os.Getenv(crashHelperMode)
	if mode == "" {
		return
	}
	root := os.Getenv(crashHelperDir)
	parts := strings.Split(os.Getenv(crashHelperAddr), ",")
	require.NotEmpty(t, root)
	require.Len(t, parts, 3)
	var addresses [3]string
	copy(addresses[:], parts)
	c := newThreeNodeClusterAt(t, root, addresses)
	leader := c.waitForLeader(t)

	switch mode {
	case "write":
		queueName := createTestReplicatedQueue(t, leader)
		offset := appendTestRecord(t, leader, queueName, "crash-1", []byte("survive process crash"))
		require.Equal(t, uint64(0), offset)

		// Recovery may discard only queues owned by this Raft group.
		localQueue := types.DefaultQueueConfig("local-jobs", "local/#")
		require.NoError(t, c.nodes[0].store.CreateQueue(context.Background(), localQueue))
		localRecord := newQueuedEnvelope("local-1", "local/ready", []byte("keep local state"))
		localOffset, localErr := c.nodes[0].store.AppendAndSync(context.Background(), localQueue.Name, localRecord)
		if localErr != nil {
			message.Release(localRecord)
		}
		require.NoError(t, localErr)
		require.Equal(t, uint64(0), localOffset)
		foreignQueue := types.DefaultQueueConfig("foreign-jobs", "foreign/#")
		foreignQueue.Replication.Enabled = true
		foreignQueue.Replication.Group = "another-group"
		require.NoError(t, c.nodes[0].store.CreateQueue(context.Background(), foreignQueue))
		foreignRecord := newQueuedEnvelope("foreign-1", "foreign/ready", []byte("keep other group"))
		foreignOffset, foreignErr := c.nodes[0].store.AppendAndSync(context.Background(), foreignQueue.Name, foreignRecord)
		if foreignErr != nil {
			message.Release(foreignRecord)
		}
		require.NoError(t, foreignErr)
		require.Equal(t, uint64(0), foreignOffset)

		_, err := fmt.Fprintln(os.Stdout, "ACKED")
		require.NoError(t, err)
		for {
			time.Sleep(time.Hour)
		}
	case "recover":
		c.waitForRecord(t, "replicated-jobs", 0, "crash-1", []byte("survive process crash"))
		for _, node := range c.nodes {
			count, err := node.store.Count(context.Background(), "replicated-jobs")
			require.NoError(t, err)
			require.Equal(t, uint64(1), count, "%s should recover exactly one record", node.id)
		}
		localRecord, err := c.nodes[0].store.Read(context.Background(), "local-jobs", 0)
		require.NoError(t, err, "recovery must preserve the non-replicated queue")
		require.Equal(t, "local-1", localRecord.PublisherMeta.MessageID)
		require.Equal(t, []byte("keep local state"), localRecord.PayloadBytes())
		message.Release(localRecord)
		foreignRecord, err := c.nodes[0].store.Read(context.Background(), "foreign-jobs", 0)
		require.NoError(t, err, "recovery must preserve another group's queue")
		require.Equal(t, "foreign-1", foreignRecord.PublisherMeta.MessageID)
		require.Equal(t, []byte("keep other group"), foreignRecord.PayloadBytes())
		message.Release(foreignRecord)
	default:
		t.Fatalf("unknown crash helper mode %q", mode)
	}
}
