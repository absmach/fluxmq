// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package main demonstrates cross-protocol queue interop between MQTT, AMQP 1.0, and AMQP 0.9.1.
//
// Scenario: Order Processing Pipeline
//   - 3 MQTT Publishers send orders from different customers
//   - 3 Consumer Groups process the same queue:
//   - "order-validators" (2 MQTT consumers) - validates each order
//   - "order-fulfillment" (1 AMQP 1.0 consumer) - fulfills validated orders
//   - "order-shipper" (1 AMQP 0.9.1 consumer) - ships fulfilled orders
//
// Key behaviors demonstrated:
//  1. MQTT publishers write to a durable queue
//  2. MQTT consumers receive messages via the FluxMQ client library
//  3. An AMQP 1.0 consumer (using Azure/go-amqp) receives the same messages
//  4. An AMQP 0.9.1 consumer (using rabbitmq/amqp091-go) receives the same messages
//  5. AMQP consumers use dispositions/acks for message acknowledgment
//  6. All three consumer groups receive ALL messages independently (fan-out)
//  7. Within a group, messages are load-balanced across consumers
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/absmach/fluxmq/client"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

const queueName = "tasks/orders"

var (
	mqttAddr    = flag.String("mqtt", "localhost:1883", "MQTT broker address")
	amqpAddr    = flag.String("amqp", "localhost:5672", "AMQP 1.0 broker address")
	amqp091Addr = flag.String("amqp091", "localhost:5682", "AMQP 0.9.1 broker address")
	numMessages = flag.Int("messages", 10, "Number of messages per publisher")
	publishRate = flag.Duration("rate", 200*time.Millisecond, "Delay between publishes")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	var (
		publishedCount   int64
		validatorCount   int64
		fulfillmentCount int64
		shipperCount     int64
	)

	log.Println("Starting consumers...")

	// Consumer Group 1: order-validators (2 MQTT consumers for load balancing)
	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runValidator(ctx, id, &validatorCount)
		}(i)
	}

	// Consumer Group 2: order-fulfillment (1 AMQP 1.0 consumer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runAMQPFulfillment(ctx, &fulfillmentCount)
	}()

	// Consumer Group 3: order-shipper (1 AMQP 0.9.1 consumer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runAMQP091Fulfillment(ctx, &shipperCount)
	}()

	time.Sleep(time.Second)

	log.Println("Starting publishers...")
	publishWg := &sync.WaitGroup{}

	customers := []string{"customer-alice", "customer-bob", "customer-charlie"}
	for i, customer := range customers {
		publishWg.Add(1)
		go func(id int, customerID string) {
			defer publishWg.Done()
			runPublisher(ctx, id, customerID, *mqttAddr, *numMessages, &publishedCount)
		}(i+1, customer)
	}

	done := make(chan struct{})
	go func() {
		publishWg.Wait()
		log.Println("All publishers finished")
		time.Sleep(2 * time.Second)
		close(done)
	}()

	select {
	case <-sigCh:
		log.Println("\nShutdown signal received")
		cancel()
	case <-done:
		log.Println("Demo complete")
		cancel()
	}

	wg.Wait()

	fmt.Println("\n=== Statistics ===")
	fmt.Printf("Messages published:                 %d\n", atomic.LoadInt64(&publishedCount))
	fmt.Printf("Validator group processed (MQTT):    %d\n", atomic.LoadInt64(&validatorCount))
	fmt.Printf("Fulfillment group processed (AMQP 1.0): %d\n", atomic.LoadInt64(&fulfillmentCount))
	fmt.Printf("Shipper group processed (AMQP 0.9.1): %d\n", atomic.LoadInt64(&shipperCount))
	fmt.Println()
	fmt.Println("Expected behavior:")
	fmt.Printf("  - Each group receives all %d messages\n", atomic.LoadInt64(&publishedCount))
	fmt.Println("  - Validators split work across 2 MQTT consumers")
	fmt.Println("  - Fulfillment processes all messages via AMQP 1.0")
	fmt.Println("  - Shipper processes all messages via AMQP 0.9.1")
}

// runPublisher publishes orders via MQTT with QoS 2.
func runPublisher(ctx context.Context, id int, customerID, nodeAddr string, count int, published *int64) {
	clientID := fmt.Sprintf("publisher-%d", id)

	opts := client.NewOptions().
		SetServers(nodeAddr).
		SetClientID(clientID).
		SetProtocolVersion(5).
		SetCleanSession(true).
		SetKeepAlive(30 * time.Second).
		SetConnectTimeout(10 * time.Second).
		SetAckTimeout(10 * time.Second).
		SetOnConnectionLost(func(err error) {
			log.Printf("[%s] Connection lost: %v", clientID, err)
		})

	c, err := client.New(opts)
	if err != nil {
		log.Printf("[%s] Failed to create client: %v", clientID, err)
		return
	}

	if err := c.Connect(); err != nil {
		log.Printf("[%s] Failed to connect: %v", clientID, err)
		return
	}
	defer c.Disconnect()

	log.Printf("[%s] Connected to %s (MQTT), publishing orders for %s", clientID, nodeAddr, customerID)

	for i := 1; i <= count; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		orderID := fmt.Sprintf("%s-order-%d", customerID, i)
		payload := fmt.Appendf(nil, `{"order_id":"%s","customer":"%s","amount":%d}`,
			orderID, customerID, rand.Intn(1000)+100)

		err := c.PublishToQueueWithOptions(&client.QueuePublishOptions{
			QueueName: queueName,
			Payload:   payload,
			QoS:       2,
		})
		if err != nil {
			log.Printf("[%s] Failed to publish order %s: %v", clientID, orderID, err)
			continue
		}

		atomic.AddInt64(published, 1)
		time.Sleep(*publishRate)
	}

	log.Printf("[%s] Finished publishing %d orders", clientID, count)
}

// runValidator runs an MQTT order validator consumer.
func runValidator(ctx context.Context, id int, processed *int64) {
	clientID := fmt.Sprintf("validator-%d", id)
	consumerGroup := "order-validators"

	opts := client.NewOptions().
		SetServers(*mqttAddr).
		SetClientID(clientID).
		SetProtocolVersion(5).
		SetCleanSession(true).
		SetKeepAlive(30 * time.Second).
		SetConnectTimeout(10 * time.Second).
		SetOnConnectionLost(func(err error) {
			log.Printf("[%s] Connection lost: %v", clientID, err)
		})

	c, err := client.New(opts)
	if err != nil {
		log.Printf("[%s] Failed to create client: %v", clientID, err)
		return
	}

	if err := c.Connect(); err != nil {
		log.Printf("[%s] Failed to connect: %v", clientID, err)
		return
	}
	defer c.Disconnect()

	log.Printf("[%s] Connected to %s (MQTT)", clientID, *mqttAddr)

	msgCh := make(chan *client.QueueMessage, 100)

	err = c.SubscribeToQueue(queueName, consumerGroup, func(msg *client.QueueMessage) {
		select {
		case msgCh <- msg:
		default:
			log.Printf("[%s] Message channel full, dropping message", clientID)
		}
	})
	if err != nil {
		log.Printf("[%s] Failed to subscribe: %v", clientID, err)
		return
	}

	log.Printf("[%s] Subscribed to queue '%s' in group '%s'", clientID, queueName, consumerGroup)

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-msgCh:
			time.Sleep(100 * time.Millisecond)

			atomic.AddInt64(processed, 1)
			log.Printf("[%s] Validated order (seq=%d): %s",
				clientID, msg.Sequence, string(msg.Payload))

			if err := msg.Ack(); err != nil {
				log.Printf("[%s] Failed to ack message: %v", clientID, err)
			}
		}
	}
}

// runAMQPFulfillment runs the order fulfillment consumer over AMQP 1.0.
func runAMQPFulfillment(ctx context.Context, processed *int64) {
	clientID := "amqp-fulfillment-1"
	consumerGroup := "order-fulfillment"

	addr := fmt.Sprintf("amqp://%s", *amqpAddr)

	conn, err := amqp.Dial(ctx, addr, &amqp.ConnOptions{
		ContainerID: clientID,
		SASLType:    amqp.SASLTypeAnonymous(),
	})
	if err != nil {
		log.Printf("[%s] Failed to connect: %v", clientID, err)
		return
	}
	defer conn.Close()

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		log.Printf("[%s] Failed to create session: %v", clientID, err)
		return
	}

	// Attach receiver to the queue address with consumer-group in link properties.
	// FluxMQ maps $queue/<name> addresses to durable queue subscriptions.
	receiver, err := session.NewReceiver(ctx, "$queue/"+queueName, &amqp.ReceiverOptions{
		Credit: 10,
		Properties: map[string]any{
			"consumer-group": consumerGroup,
		},
	})
	if err != nil {
		log.Printf("[%s] Failed to create receiver: %v", clientID, err)
		return
	}
	defer receiver.Close(ctx)

	log.Printf("[%s] Connected to %s (AMQP 1.0), receiving from queue '%s' in group '%s'",
		clientID, *amqpAddr, queueName, consumerGroup)

	for {
		msg, err := receiver.Receive(ctx, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[%s] Receive error: %v", clientID, err)
			return
		}

		var payload []byte
		if len(msg.Data) > 0 {
			payload = msg.Data[0]
		} else {
			payload = []byte(fmt.Sprintf("%v", msg.Value))
		}

		// Simulate fulfillment work
		time.Sleep(200 * time.Millisecond)

		atomic.AddInt64(processed, 1)
		log.Printf("[%s] Fulfilled order (AMQP 1.0): %s", clientID, string(payload))

		// Demonstrate different disposition types
		if rand.Float32() < 0.05 {
			log.Printf("[%s] Order needs retry, releasing message", clientID)
			if err := receiver.ReleaseMessage(ctx, msg); err != nil {
				log.Printf("[%s] Failed to release message: %v", clientID, err)
			}
		} else if rand.Float32() < 0.02 {
			log.Printf("[%s] Order invalid, rejecting message", clientID)
			if err := receiver.RejectMessage(ctx, msg, nil); err != nil {
				log.Printf("[%s] Failed to reject message: %v", clientID, err)
			}
		} else {
			if err := receiver.AcceptMessage(ctx, msg); err != nil {
				log.Printf("[%s] Failed to accept message: %v", clientID, err)
			}
		}
	}
}

// runAMQP091Fulfillment runs the order shipping consumer over AMQP 0.9.1.
func runAMQP091Fulfillment(ctx context.Context, processed *int64) {
	clientID := "amqp091-shipper-1"
	consumerGroup := "order-shipper"

	addr := fmt.Sprintf("amqp://guest:guest@%s/", *amqp091Addr)

	conn, err := amqp091.Dial(addr)
	if err != nil {
		log.Printf("[%s] Failed to connect: %v", clientID, err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("[%s] Failed to open a channel: %v", clientID, err)
		return
	}
	defer ch.Close()

	// FluxMQ uses consumer arguments to specify the group
	args := amqp091.Table{"x-consumer-group": consumerGroup}

	msgs, err := ch.Consume(
		"$queue/"+queueName,
		clientID, // consumer tag
		false,    // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		args,
	)
	if err != nil {
		log.Printf("[%s] Failed to register a consumer: %v", clientID, err)
		return
	}

	log.Printf("[%s] Connected to %s (AMQP 0.9.1), receiving from queue '%s' in group '%s'",
		clientID, *amqp091Addr, "$queue/"+queueName, consumerGroup)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Shutting down", clientID)
			return
		case d, ok := <-msgs:
			if !ok {
				log.Printf("[%s] Consumer channel closed", clientID)
				return
			}
			time.Sleep(250 * time.Millisecond) // Simulate shipping work

			atomic.AddInt64(processed, 1)
			log.Printf("[%s] Shipped order (AMQP 0.9.1): %s", clientID, string(d.Body))

			if err := d.Ack(false); err != nil {
				log.Printf("[%s] Failed to ack message: %v", clientID, err)
			}
		}
	}
}
