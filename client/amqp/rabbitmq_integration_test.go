//go:build integration

// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	rabbitMQSuiteLabelKey   = "fluxmq.test.suite"
	rabbitMQSuiteLabelValue = "client-amqp-integration"
)

var (
	suiteRabbitURL  string
	suiteContainer  string
	suiteSetupError error
)

func TestMain(m *testing.M) {
	cleanupLabeledRabbitMQContainers()

	if !shortModeRequested() {
		suiteRabbitURL, suiteContainer, suiteSetupError = setupRabbitMQSuite()
	}

	code := m.Run()

	stopContainer(suiteContainer)
	cleanupLabeledRabbitMQContainers()
	os.Exit(code)
}

func TestRabbitMQAdminAPIIntegration(t *testing.T) {
	url := suiteRabbitURLOrSkip(t)
	c := newConnectedClient(t, url)
	defer c.Close()

	exchange := uniqueName("it-ex")
	altExchange := uniqueName("it-ex-alt")
	queue := uniqueName("it-q")
	routingKey := "events.created"

	require.NoError(t, c.DeclareExchange(&ExchangeDeclareOptions{Name: exchange, Kind: "direct"}))
	require.NoError(t, c.DeclareExchange(&ExchangeDeclareOptions{Name: altExchange, Kind: "direct"}))

	qName, err := c.DeclareQueue(&QueueDeclareOptions{Name: queue})
	require.NoError(t, err)
	require.Equal(t, queue, qName)

	require.NoError(t, c.BindQueue(qName, routingKey, exchange, false, nil))
	require.NoError(t, c.BindExchange(altExchange, routingKey, exchange, false, nil))
	require.NoError(t, c.UnbindExchange(altExchange, routingKey, exchange, false, nil))
	require.NoError(t, c.SetQoS(8, 0, false))

	received := make(chan *Message, 1)
	require.NoError(t, c.Subscribe(qName, func(msg *Message) {
		select {
		case received <- msg:
		default:
		}
	}))
	defer c.Unsubscribe(qName)

	payload := []byte("admin-api")
	require.NoError(t, c.PublishWithOptions(&PublishOptions{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Payload:    payload,
	}))

	select {
	case msg := <-received:
		require.NotNil(t, msg)
		assert.Equal(t, payload, msg.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for delivered message")
	}

	require.NoError(t, c.UnbindQueue(qName, routingKey, exchange, nil))

	purged, err := c.PurgeQueue(qName, false)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, purged, 0)

	deleted, err := c.DeleteQueue(qName, false, false, false)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, deleted, 0)

	require.NoError(t, c.DeleteExchange(altExchange, false, false))
	require.NoError(t, c.DeleteExchange(exchange, false, false))
}

func TestRabbitMQPublishConfirmAndReturnIntegration(t *testing.T) {
	url := suiteRabbitURLOrSkip(t)
	c := newConnectedClient(t, url)
	defer c.Close()

	exchange := uniqueName("it-confirm-ex")
	queue := uniqueName("it-confirm-q")
	routingKey := "confirm.ok"

	require.NoError(t, c.DeclareExchange(&ExchangeDeclareOptions{Name: exchange, Kind: "direct"}))
	qName, err := c.DeclareQueue(&QueueDeclareOptions{Name: queue})
	require.NoError(t, err)
	require.NoError(t, c.BindQueue(qName, routingKey, exchange, false, nil))
	defer c.DeleteQueue(qName, false, false, false)
	defer c.DeleteExchange(exchange, false, false)

	returns := make(chan amqp091.Return, 1)
	confirms := make(chan amqp091.Confirmation, 4)

	c.SetReturnHandler(func(ret amqp091.Return) {
		select {
		case returns <- ret:
		default:
		}
	})
	c.SetPublishConfirmHandler(func(confirm amqp091.Confirmation) {
		select {
		case confirms <- confirm:
		default:
		}
	})

	require.NoError(t, c.EnablePublisherConfirms())

	require.NoError(t, c.PublishWithConfirm(&PublishOptions{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Payload:    []byte("confirm-blocking"),
	}, 5*time.Second))

	require.NoError(t, c.PublishWithOptions(&PublishOptions{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Payload:    []byte("confirm-callback"),
	}))

	select {
	case confirm := <-confirms:
		assert.True(t, confirm.Ack)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for publisher confirmation callback")
	}

	unroutablePayload := []byte("must-return")
	require.NoError(t, c.PublishWithOptions(&PublishOptions{
		Exchange:   exchange,
		RoutingKey: "missing.route",
		Payload:    unroutablePayload,
		Mandatory:  true,
	}))

	select {
	case ret := <-returns:
		assert.Equal(t, "missing.route", ret.RoutingKey)
		assert.Equal(t, unroutablePayload, ret.Body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for mandatory publish return")
	}
}

func newConnectedClient(t *testing.T, url string) *Client {
	t.Helper()

	c, err := New(NewOptions().SetURL(url).SetAutoReconnect(false))
	require.NoError(t, err)
	require.NoError(t, c.Connect())

	return c
}

func suiteRabbitURLOrSkip(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping RabbitMQ integration test in short mode")
	}
	if suiteSetupError != nil {
		t.Skipf("rabbitmq integration setup unavailable: %v", suiteSetupError)
	}
	if suiteRabbitURL == "" {
		t.Skip("rabbitmq integration setup did not provide a broker URL")
	}
	return suiteRabbitURL
}

func setupRabbitMQSuite() (string, string, error) {
	// Allow running against an already running broker.
	if externalURL := strings.TrimSpace(os.Getenv("FLUXMQ_AMQP_TEST_URL")); externalURL != "" {
		if err := waitForRabbitMQ(externalURL, 30*time.Second, ""); err != nil {
			return "", "", err
		}
		return externalURL, "", nil
	}

	if err := dockerAvailable(); err != nil {
		return "", "", err
	}

	image := os.Getenv("FLUXMQ_AMQP_TEST_IMAGE")
	if image == "" {
		image = "rabbitmq:3.13-alpine"
	}

	containerName := uniqueName("fluxmq-amqp-it")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx,
		"docker", "run", "-d", "--rm",
		"--name", containerName,
		"--label", fmt.Sprintf("%s=%s", rabbitMQSuiteLabelKey, rabbitMQSuiteLabelValue),
		"-p", "5672",
		"-e", "RABBITMQ_DEFAULT_USER=guest",
		"-e", "RABBITMQ_DEFAULT_PASS=guest",
		image,
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", "", fmt.Errorf("failed to start RabbitMQ container: %w: %s", err, strings.TrimSpace(string(out)))
	}

	addr, err := containerBrokerAddress(containerName)
	if err != nil {
		stopContainer(containerName)
		return "", "", err
	}
	amqpURL := fmt.Sprintf("amqp://guest:guest@%s/", addr)
	if err := waitForRabbitMQ(amqpURL, 60*time.Second, containerName); err != nil {
		stopContainer(containerName)
		return "", "", err
	}

	return amqpURL, containerName, nil
}

func waitForRabbitMQ(url string, timeout time.Duration, containerName string) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := dialAMQP(url, 2*time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	if containerName == "" {
		return fmt.Errorf("rabbitmq not ready within %s at %s", timeout, url)
	}

	logCtx, logCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer logCancel()
	logs, _ := exec.CommandContext(logCtx, "docker", "logs", containerName).CombinedOutput()
	return fmt.Errorf("rabbitmq not ready within %s at %s; logs:\n%s", timeout, url, strings.TrimSpace(string(logs)))
}

func dockerAvailable() error {
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("docker not found: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "info").Run(); err != nil {
		return fmt.Errorf("docker daemon unavailable: %w", err)
	}

	return nil
}

func stopContainer(containerName string) {
	if containerName == "" {
		return
	}
	rmCtx, rmCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer rmCancel()
	_ = exec.CommandContext(rmCtx, "docker", "rm", "-f", containerName).Run()
}

func shortModeRequested() bool {
	for _, arg := range os.Args {
		if arg == "-test.short" || arg == "-test.short=true" || arg == "-test.short=1" {
			return true
		}
	}
	return false
}

func cleanupLabeledRabbitMQContainers() {
	if _, err := exec.LookPath("docker"); err != nil {
		return
	}

	filter := fmt.Sprintf("label=%s=%s", rabbitMQSuiteLabelKey, rabbitMQSuiteLabelValue)
	psCtx, psCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer psCancel()
	out, err := exec.CommandContext(psCtx, "docker", "ps", "-aq", "--filter", filter).CombinedOutput()
	if err != nil {
		return
	}

	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return
	}

	args := append([]string{"rm", "-f"}, ids...)
	rmCtx, rmCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer rmCancel()
	_ = exec.CommandContext(rmCtx, "docker", args...).Run()
}

func containerBrokerAddress(containerName string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, "docker", "port", containerName, "5672/tcp").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to resolve published AMQP port: %w: %s", err, strings.TrimSpace(string(out)))
	}

	var hostPort string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		hostPort = line
		break
	}
	if hostPort == "" {
		return "", fmt.Errorf("docker port returned no mapping for 5672/tcp")
	}

	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", fmt.Errorf("invalid published host/port %q: %w", hostPort, err)
	}

	if host == "0.0.0.0" || host == "::" || host == "[::]" || host == "" {
		host = dockerReachableHost()
	}

	return net.JoinHostPort(host, port), nil
}

func dockerReachableHost() string {
	if v := strings.TrimSpace(os.Getenv("FLUXMQ_AMQP_TEST_HOST")); v != "" {
		return v
	}

	raw := strings.TrimSpace(os.Getenv("DOCKER_HOST"))
	if raw == "" {
		return "127.0.0.1"
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "127.0.0.1"
	}

	if u.Scheme == "tcp" {
		if h := u.Hostname(); h != "" {
			return h
		}
	}

	return "127.0.0.1"
}

func dialAMQP(rawURL string, timeout time.Duration) (*amqp091.Connection, error) {
	d := &net.Dialer{Timeout: timeout}
	cfg := amqp091.Config{
		Dial:      d.Dial,
		Heartbeat: 5 * time.Second,
	}
	return amqp091.DialConfig(rawURL, cfg)
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
