// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package main demonstrates FluxMQ protocol interoperability in a single program.
//
// It runs three scenarios sequentially:
//  1. MQTT pub/sub on a regular topic (standard MQTT messaging)
//  2. MQTT publish → AMQP 0.9.1 consume on a durable queue (cross-protocol interop)
//  3. AMQP 0.9.1 stream queue: declare, publish, and consume with offset-based replay
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

var (
	mqttAddr    = flag.String("mqtt", "localhost:1883", "MQTT broker address")
	amqp091Addr = flag.String("amqp091", "localhost:5682", "AMQP 0.9.1 broker address")
	runID       = flag.String("run-id", "", "Optional run suffix for queue/stream names (defaults to current unix timestamp)")
)

var demoRunID string

func main() {
	flag.Parse()
	demoRunID = *runID
	if demoRunID == "" {
		demoRunID = fmt.Sprintf("%d", time.Now().Unix())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutdown signal received")
		cancel()
	}()

	log.Println("=== Scenario 1: MQTT Pub/Sub ===")
	mqttPubSub(ctx)

	log.Println("\n=== Scenario 2: MQTT → AMQP 0.9.1 (Durable Queue) ===")
	mqttToAMQP091Queue(ctx)

	log.Println("\n=== Scenario 3: AMQP 0.9.1 Stream Queue ===")
	amqp091Stream(ctx)

	log.Println("\nAll scenarios completed.")
}

func demoName(base string) string {
	return fmt.Sprintf("%s-%s", base, demoRunID)
}

// --- Scenario 1: Standard MQTT pub/sub ---

func mqttPubSub(ctx context.Context) {
	topic := "demo/sensors/temperature"
	received := make(chan struct{}, 1)

	// Subscriber
	subOpts := mqtt.NewClientOptions().
		AddBroker("tcp://" + *mqttAddr).
		SetClientID("demo-mqtt-sub").
		SetCleanSession(true).
		SetProtocolVersion(4)

	sub := mqtt.NewClient(subOpts)
	if tok := sub.Connect(); tok.WaitTimeout(5*time.Second) && tok.Error() != nil {
		log.Fatalf("MQTT subscriber connect failed: %v", tok.Error())
	}
	defer sub.Disconnect(250)

	if tok := sub.Subscribe(topic, 1, func(_ mqtt.Client, msg mqtt.Message) {
		log.Printf("  [MQTT sub] Received on %s: %s", msg.Topic(), msg.Payload())
		select {
		case received <- struct{}{}:
		default:
		}
	}); tok.WaitTimeout(5*time.Second) && tok.Error() != nil {
		log.Fatalf("MQTT subscribe failed: %v", tok.Error())
	}
	log.Printf("  [MQTT sub] Subscribed to %s", topic)

	// Publisher
	pubOpts := mqtt.NewClientOptions().
		AddBroker("tcp://" + *mqttAddr).
		SetClientID("demo-mqtt-pub").
		SetCleanSession(true).
		SetProtocolVersion(4)

	pub := mqtt.NewClient(pubOpts)
	if tok := pub.Connect(); tok.WaitTimeout(5*time.Second) && tok.Error() != nil {
		log.Fatalf("MQTT publisher connect failed: %v", tok.Error())
	}
	defer pub.Disconnect(250)

	payload := `{"sensor":"temp-1","value":22.5}`
	log.Printf("  [MQTT pub] Publishing to %s: %s", topic, payload)
	if tok := pub.Publish(topic, 1, false, payload); tok.WaitTimeout(5*time.Second) && tok.Error() != nil {
		log.Fatalf("MQTT publish failed: %v", tok.Error())
	}

	select {
	case <-received:
		log.Println("  [OK] MQTT pub/sub round-trip successful")
	case <-time.After(5 * time.Second):
		log.Println("  [TIMEOUT] Did not receive MQTT message")
	case <-ctx.Done():
		return
	}
}

// --- Scenario 2: MQTT publish to queue, AMQP 0.9.1 consume ---

func mqttToAMQP091Queue(ctx context.Context) {
	queueName := demoName("demo-orders")
	queueTopic := "$queue/" + queueName
	queueFilter := queueTopic + "/#"
	consumerTag := "demo-consumer"
	messageCount := 5
	received := make(chan string, messageCount)

	// AMQP 0.9.1 consumer
	conn091, err := amqp091.Dial(fmt.Sprintf("amqp://guest:guest@%s/", *amqp091Addr))
	if err != nil {
		log.Fatalf("AMQP 0.9.1 connect failed: %v", err)
	}
	defer conn091.Close()

	ch, err := conn091.Channel()
	if err != nil {
		log.Fatalf("AMQP 0.9.1 channel open failed: %v", err)
	}
	defer ch.Close()

	deliveries, err := ch.Consume(
		queueFilter, // queue filter
		consumerTag, // consumer tag
		false,       // auto-ack off — we ack manually
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		amqp091.Table{
			"x-consumer-group": "demo-workers",
		},
	)
	if err != nil {
		log.Fatalf("AMQP 0.9.1 consume failed: %v", err)
	}
	log.Printf("  [AMQP 0.9.1] Consuming from queue '%s' in group 'demo-workers'", queueName)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for d := range deliveries {
			log.Printf("  [AMQP 0.9.1] Received: %s", d.Body)
			_ = d.Ack(false)
			received <- string(d.Body)
			if len(received) >= messageCount {
				return
			}
		}
	}()

	// Allow the consumer to attach
	time.Sleep(500 * time.Millisecond)

	// MQTT publisher
	pubOpts := mqtt.NewClientOptions().
		AddBroker("tcp://" + *mqttAddr).
		SetClientID("demo-queue-pub").
		SetCleanSession(true).
		SetProtocolVersion(4)

	pub := mqtt.NewClient(pubOpts)
	if tok := pub.Connect(); tok.WaitTimeout(5*time.Second) && tok.Error() != nil {
		log.Fatalf("MQTT publisher connect failed: %v", tok.Error())
	}
	defer pub.Disconnect(250)

	for i := 1; i <= messageCount; i++ {
		payload := fmt.Sprintf(`{"order_id":"order-%d","item":"widget","qty":%d}`, i, i*10)
		log.Printf("  [MQTT pub] Publishing to %s: %s", queueTopic, payload)
		if tok := pub.Publish(queueTopic, 1, false, payload); tok.WaitTimeout(5*time.Second) && tok.Error() != nil {
			log.Printf("  [MQTT pub] Publish failed: %v", tok.Error())
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for consumer to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("  [OK] All %d messages published via MQTT, consumed via AMQP 0.9.1", messageCount)
	case <-time.After(10 * time.Second):
		log.Println("  [TIMEOUT] Not all messages received via AMQP 0.9.1")
	case <-ctx.Done():
		return
	}

	// Cleanup keeps the demo repeatable and avoids backlog replay for ad-hoc subscribers.
	if err := ch.Cancel(consumerTag, false); err != nil {
		log.Printf("  [AMQP 0.9.1] Consumer cancel failed: %v", err)
	}
	if _, err := ch.QueueDelete(queueName, false, false, false); err != nil {
		log.Printf("  [AMQP 0.9.1] Queue cleanup failed: %v", err)
	} else {
		log.Printf("  [AMQP 0.9.1] Deleted queue '%s' after demo", queueName)
	}

	log.Printf("  [INFO] Manual MQTT inspection topic for this run: %s", queueFilter)
}

// --- Scenario 3: AMQP 0.9.1 stream queue ---

func amqp091Stream(ctx context.Context) {
	streamName := demoName("demo-events")
	streamFilter := "$queue/" + streamName + "/#"
	messageCount := 5

	conn, err := amqp091.Dial(fmt.Sprintf("amqp://guest:guest@%s/", *amqp091Addr))
	if err != nil {
		log.Fatalf("AMQP 0.9.1 connect failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("AMQP 0.9.1 channel open failed: %v", err)
	}
	defer ch.Close()

	// Declare a stream queue
	_, err = ch.QueueDeclare(
		streamName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		amqp091.Table{
			"x-queue-type": "stream",
			"x-max-age":    "1h",
		},
	)
	if err != nil {
		log.Fatalf("Stream queue declare failed: %v", err)
	}
	log.Printf("  [AMQP 0.9.1] Declared stream queue '%s'", streamName)

	// Publish messages to the stream
	for i := 1; i <= messageCount; i++ {
		body := fmt.Sprintf(`{"event":"user.action","seq":%d}`, i)
		err := ch.PublishWithContext(ctx, "", streamName, false, false, amqp091.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		})
		if err != nil {
			log.Printf("  [AMQP 0.9.1] Publish to stream failed: %v", err)
			continue
		}
		log.Printf("  [AMQP 0.9.1] Published to stream: %s", body)
	}

	time.Sleep(500 * time.Millisecond)

	// Consume from the beginning of the stream (replay)
	deliveries, err := ch.Consume(
		streamFilter,    // stream queue filter
		"stream-reader", // consumer tag
		false,           // auto-ack off
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		amqp091.Table{
			"x-consumer-group": "demo-readers",
			"x-stream-offset":  "first", // replay from the beginning
		},
	)
	if err != nil {
		log.Fatalf("Stream consume failed: %v", err)
	}
	log.Printf("  [AMQP 0.9.1] Consuming stream '%s' from offset 'first'", streamName)

	count := 0
	timeout := time.After(10 * time.Second)
	for count < messageCount {
		select {
		case d, ok := <-deliveries:
			if !ok {
				log.Println("  [AMQP 0.9.1] Delivery channel closed")
				return
			}
			offset := d.Headers["x-stream-offset"]
			log.Printf("  [AMQP 0.9.1] Stream message (offset=%v): %s", offset, d.Body)
			_ = d.Ack(false)
			count++
		case <-timeout:
			log.Printf("  [TIMEOUT] Received %d/%d stream messages", count, messageCount)
			return
		case <-ctx.Done():
			return
		}
	}

	log.Printf("  [OK] Published %d events, replayed all %d from stream", messageCount, count)
}
