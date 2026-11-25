// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package interop checks wire compatibility with the Eclipse Mosquitto command
// line clients, which implement MQTT independently of this repository. The
// tests skip when mosquitto_pub and mosquitto_sub are not installed.
package interop

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/absmach/fluxmq/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// messagePrefix marks mosquitto_sub's message lines apart from its debug
	// output, which goes to the same stream.
	messagePrefix = "MSG|"
	// messageFormat prints topic, QoS, retain flag and payload.
	messageFormat = messagePrefix + "%t|%q|%r|%p"
	clientTimeout = 10 * time.Second
)

type mosquitto struct {
	pub, sub   string
	websockets bool
}

// requireMosquitto finds the Mosquitto clients or skips the test.
func requireMosquitto(t *testing.T) mosquitto {
	t.Helper()

	pub, pubErr := exec.LookPath("mosquitto_pub")
	sub, subErr := exec.LookPath("mosquitto_sub")
	if pubErr != nil || subErr != nil {
		t.Skip("mosquitto_pub and mosquitto_sub are not installed")
	}
	help, _ := exec.Command(sub, "--help").CombinedOutput() //nolint:gosec // fixed binary from PATH
	return mosquitto{pub: pub, sub: sub, websockets: bytes.Contains(help, []byte("--ws"))}
}

// endpoint is how a Mosquitto client reaches the broker.
type endpoint struct {
	name string
	args func(lb *testutil.LocalBroker) []string
}

var (
	overTCP = endpoint{"tcp", func(lb *testutil.LocalBroker) []string { return hostPort(lb.TCPAddr) }}
	// Mosquitto turns TLS on by itself for port 8883; the local broker never
	// listens there, so plain WebSocket is what these tests exercise.
	overWebSocket = endpoint{"ws", func(lb *testutil.LocalBroker) []string { return append(hostPort(lb.WSAddr), "--ws") }}
)

func hostPort(addr string) []string {
	host, port, _ := net.SplitHostPort(addr)
	return []string{"-h", host, "-p", port}
}

type received struct {
	topic, qos, retain, payload string
}

// subscriber is a running mosquitto_sub. Its output is block-buffered into
// the pipe, so it is read once the process has exited.
type subscriber struct {
	cmd    *exec.Cmd
	output bytes.Buffer
	done   chan error
}

// subscribe starts mosquitto_sub as clientID on filter, with extra args, and
// returns once the broker holds that subscription for a connected client, or
// once mosquitto_sub has finished successfully.
func (m mosquitto) subscribe(t *testing.T, lb *testutil.LocalBroker, clientID, filter string, args ...string) *subscriber {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	t.Cleanup(cancel)
	args = append([]string{"-d", "-F", messageFormat, "-i", clientID, "-t", filter}, args...)
	s := &subscriber{done: make(chan error, 1)}
	s.cmd = exec.CommandContext(ctx, m.sub, args...) //nolint:gosec // fixed binary, test-chosen arguments
	s.cmd.Stdout = &s.output
	s.cmd.Stderr = &s.output
	require.NoError(t, s.cmd.Start())
	go func() { s.done <- s.cmd.Wait() }()
	t.Cleanup(func() {
		_ = s.cmd.Process.Kill()
		err := <-s.done
		s.done <- err
	})

	subscribed := func() bool {
		sess := lb.Broker.Get(clientID)
		if sess == nil || !sess.IsConnected() {
			return false
		}
		_, ok := sess.GetSubscriptions()[filter]
		return ok
	}
	deadline := time.Now().Add(clientTimeout)
	for !subscribed() {
		select {
		case err := <-s.done:
			s.done <- err
			if err == nil {
				// It subscribed and already got what -C asked for.
				return s
			}
			t.Fatalf("mosquitto_sub exited before subscribing: %v\n%s", err, s.output.String())
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("mosquitto_sub did not subscribe")
		}
	}
	return s
}

// wait blocks until mosquitto_sub exits by itself, as -C, -E and -W make it,
// and returns what it received. expectSuccess is false for a -W timeout,
// which mosquitto_sub reports as a failure.
func (s *subscriber) wait(t *testing.T, expectSuccess bool) []received {
	t.Helper()

	var err error
	select {
	case err = <-s.done:
		s.done <- err // for the cleanup
	case <-time.After(clientTimeout):
		t.Fatalf("mosquitto_sub did not finish\n%s", s.output.String())
	}
	if expectSuccess {
		require.NoError(t, err, "mosquitto_sub failed\n%s", s.output.String())
	}

	var messages []received
	for line := range strings.SplitSeq(s.output.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, messagePrefix); ok {
			if parts := strings.SplitN(rest, "|", 4); len(parts) == 4 {
				messages = append(messages, received{parts[0], parts[1], parts[2], parts[3]})
			}
		}
	}
	return messages
}

func (m mosquitto) publish(t *testing.T, args ...string) {
	t.Helper()
	m.run(t, m.pub, args...)
}

// run runs a Mosquitto client to completion.
func (m mosquitto) run(t *testing.T, binary string, args ...string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput() //nolint:gosec // fixed binary, test-chosen arguments
	require.NoError(t, err, "%s %v: %s", filepath.Base(binary), args, out)
}

var protocols = []struct {
	name    string
	version string
}{
	{"v311", "mqttv311"},
	{"v5", "mqttv5"},
}

func TestMosquittoPublishSubscribe(t *testing.T) {
	m := requireMosquitto(t)
	lb := testutil.StartLocalBroker(t)

	for _, ep := range []endpoint{overTCP, overWebSocket} {
		for _, proto := range protocols {
			for _, qos := range []string{"0", "1", "2"} {
				t.Run(ep.name+"/"+proto.name+"/qos"+qos, func(t *testing.T) {
					t.Parallel()
					if ep.name == overWebSocket.name && !m.websockets {
						t.Skip("this mosquitto build has no WebSocket support")
					}
					topic := fmt.Sprintf("interop/%s/%s/%s", ep.name, proto.name, qos)
					common := append(ep.args(lb), "-V", proto.version, "-q", qos)

					sub := m.subscribe(t, lb, "sub-"+strings.ReplaceAll(topic, "/", "-"), topic, append(common, "-C", "1")...)
					m.publish(t, append(common, "-t", topic, "-m", "hello "+topic)...)

					got := sub.wait(t, true)
					require.Len(t, got, 1)
					assert.Equal(t, topic, got[0].topic)
					assert.Equal(t, qos, got[0].qos)
					assert.Equal(t, "hello "+topic, got[0].payload)
				})
			}
		}
	}
}

func TestMosquittoRetainedMessage(t *testing.T) {
	m := requireMosquitto(t)
	lb := testutil.StartLocalBroker(t)

	for _, proto := range protocols {
		t.Run(proto.name, func(t *testing.T) {
			topic := "interop/retained/" + proto.name
			common := append(overTCP.args(lb), "-V", proto.version)
			m.publish(t, append(common, "-t", topic, "-q", "1", "-r", "-m", "kept")...)

			got := m.subscribe(t, lb, "retained-"+proto.name, topic, append(common, "-C", "1")...).wait(t, true)
			require.Len(t, got, 1)
			assert.Equal(t, "kept", got[0].payload)
			assert.Equal(t, "1", got[0].retain, "a retained message must arrive flagged as retained")

			// An empty retained publish clears it [MQTT-3.3.1-10].
			m.publish(t, append(common, "-t", topic, "-r", "-n")...)
			late := m.subscribe(t, lb, "retained-late-"+proto.name, topic, append(common, "-W", "1")...)
			assert.Empty(t, late.wait(t, false), "a cleared retained message was delivered")
		})
	}
}

func TestMosquittoWillOnAbruptDisconnect(t *testing.T) {
	m := requireMosquitto(t)
	lb := testutil.StartLocalBroker(t)

	for _, proto := range protocols {
		t.Run(proto.name, func(t *testing.T) {
			willTopic := "interop/will/" + proto.name
			watcher := m.subscribe(t, lb, "will-watcher-"+proto.name, willTopic, append(overTCP.args(lb), "-V", proto.version, "-C", "1")...)

			victim := m.subscribe(t, lb, "will-victim-"+proto.name, "interop/will/unused",
				append(overTCP.args(lb), "-V", proto.version, "--will-topic", willTopic, "--will-payload", "victim gone", "--will-qos", "1")...)
			require.NoError(t, victim.cmd.Process.Kill())

			got := watcher.wait(t, true)
			require.Len(t, got, 1)
			assert.Equal(t, "victim gone", got[0].payload)
		})
	}
}

func TestMosquittoPersistentSessionReceivesWhileAway(t *testing.T) {
	m := requireMosquitto(t)
	lb := testutil.StartLocalBroker(t)

	for _, proto := range protocols {
		t.Run(proto.name, func(t *testing.T) {
			topic := "interop/offline/" + proto.name
			clientID := "interop-offline-" + proto.name
			session := append(overTCP.args(lb), "-V", proto.version, "-c", "-q", "1")
			if proto.version == "mqttv5" {
				session = append(session, "-x", "300") // a v5 session ends at disconnect without an expiry
			}

			// -E subscribes and leaves at once, which creates the session.
			m.run(t, m.sub, append(session, "-i", clientID, "-t", topic, "-E")...)
			require.Eventually(t, func() bool {
				sess := lb.Broker.Get(clientID)
				return sess != nil && !sess.IsConnected()
			}, clientTimeout, 10*time.Millisecond, "the first connection did not end")
			for i := range 3 {
				m.publish(t, append(overTCP.args(lb), "-V", proto.version, "-q", "1", "-t", topic, "-m", fmt.Sprintf("while-away-%d", i))...)
			}

			got := m.subscribe(t, lb, clientID, topic, append(session, "-C", "3")...).wait(t, true)
			payloads := make([]string, len(got))
			for i, msg := range got {
				payloads[i] = msg.payload
			}
			assert.Equal(t, []string{"while-away-0", "while-away-1", "while-away-2"}, payloads)
		})
	}
}

func TestMosquittoLargePayload(t *testing.T) {
	m := requireMosquitto(t)
	lb := testutil.StartLocalBroker(t)

	payload := bytes.Repeat([]byte("0123456789abcdef"), 32<<10) // 512 KiB, no line breaks
	file := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(file, payload, 0o600))

	for _, proto := range protocols {
		t.Run(proto.name, func(t *testing.T) {
			topic := "interop/large/" + proto.name
			common := append(overTCP.args(lb), "-V", proto.version, "-q", "1")
			sub := m.subscribe(t, lb, "large-"+proto.name, topic, append(common, "-C", "1")...)
			m.publish(t, append(common, "-t", topic, "-f", file)...)

			got := sub.wait(t, true)
			require.Len(t, got, 1)
			assert.Equal(t, len(payload), len(got[0].payload))
			assert.Equal(t, string(payload), got[0].payload)
		})
	}
}
