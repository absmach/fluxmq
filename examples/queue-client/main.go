// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package main demonstrates the MQTT broker's durable queue functionality.
//
// This example showcases:
//   - QoS 2 publishing with exactly-once delivery semantics
//   - Durable queues with message persistence
//   - Consumer groups for load balancing
//   - Partition keys for message ordering
//   - Ack/Nack/Reject for message processing control
//
// Scenario: Order Processing Pipeline
//   - 3 Publishers send orders from different customers
//   - 2 Consumer Groups process the same queue:
//   - "order-validators" (2 consumers) - validates each order
//   - "order-fulfillment" (1 consumer) - fulfills validated orders
//
// Key behaviors demonstrated:
//  1. Each consumer group receives ALL messages (fan-out)
//  2. Within a group, messages are load-balanced across consumers
//  3. Partition keys ensure orders from the same customer are processed in order
//  4. QoS 2 guarantees exactly-once delivery from publisher to broker
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

	"github.com/absmach/fluxmq/client"
)

const queueName = "tasks/orders"

var (
	node1Addr   = flag.String("node1", "localhost:1883", "MQTT node 1 address")
	node2Addr   = flag.String("node2", "localhost:1884", "MQTT node 2 address")
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

	// Counters for statistics
	var (
		publishedCount   int64
		validatorCount   int64
		fulfillmentCount int64
	)

	// Start consumers first to ensure they're ready
	log.Println("Starting consumers...")

	// Consumer Group 1: order-validators (2 consumers for load balancing)
	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runValidator(ctx, id, &validatorCount)
		}(i)
	}

	// Consumer Group 2: order-fulfillment (1 consumer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runFulfillment(ctx, &fulfillmentCount)
	}()

	// Give consumers time to connect and subscribe
	time.Sleep(time.Second)

	// Start publishers
	log.Println("Starting publishers...")
	publishWg := &sync.WaitGroup{}

	customers := []string{"customer-alice", "customer-bob", "customer-charlie"}
	for i, customer := range customers {
		publishWg.Add(1)
		go func(id int, customerID string) {
			defer publishWg.Done()
			node := node1Addr
			// if i%2 != 0 {
			// 	node = node2Addr
			// }
			runPublisher(ctx, id, customerID, *node, *numMessages, &publishedCount)
		}(i+1, customer)
	}

	// Wait for publishers to finish or signal
	done := make(chan struct{})
	go func() {
		publishWg.Wait()
		log.Println("All publishers finished")
		time.Sleep(2 * time.Second) // Allow consumers to process remaining messages
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

	// Print statistics
	fmt.Println("\n=== Statistics ===")
	fmt.Printf("Messages published:           %d\n", atomic.LoadInt64(&publishedCount))
	fmt.Printf("Validator group processed:    %d\n", atomic.LoadInt64(&validatorCount))
	fmt.Printf("Fulfillment group processed:  %d\n", atomic.LoadInt64(&fulfillmentCount))
	fmt.Println()
	fmt.Println("Expected behavior:")
	fmt.Printf("  - Each group receives all %d messages\n", atomic.LoadInt64(&publishedCount))
	fmt.Println("  - Validators split work across 2 consumers")
	fmt.Println("  - Orders from same customer processed in order (partition key)")
}

// runPublisher publishes orders with QoS 2 for exactly-once delivery.
func runPublisher(ctx context.Context, id int, customerID, nodeAddr string, count int, published *int64) {
	clientID := fmt.Sprintf("publisher-%d", id)

	opts := client.NewOptions().
		SetServers(nodeAddr).
		SetClientID(clientID).
		SetProtocolVersion(5). // MQTT v5 required for user properties (partition keys)
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

	log.Printf("[%s] Connected to %s, publishing orders for %s", clientID, nodeAddr, customerID)

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
			QoS:       2, // Exactly-once delivery
		})
		if err != nil {
			log.Printf("[%s] Failed to publish order %s: %v", clientID, orderID, err)
			continue
		}

		atomic.AddInt64(published, 1)
		// log.Printf("[%s] Published order %s (QoS 2)", clientID, orderID)

		time.Sleep(*publishRate)
	}

	log.Printf("[%s] Finished publishing %d orders", clientID, count)
}

// runValidator runs an order validator consumer (part of "order-validators" group).
func runValidator(ctx context.Context, id int, processed *int64) {
	clientID := fmt.Sprintf("validator-%d", id)
	consumerGroup := "order-validators"

	opts := client.NewOptions().
		SetServers(*node1Addr).
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

	log.Printf("[%s] Connected to %s", clientID, *node1Addr)

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
			// Simulate validation work
			time.Sleep(100 * time.Millisecond)

			atomic.AddInt64(processed, 1)
			log.Printf("[%s] Validated order (seq=%d): %s",
				clientID, msg.Sequence, string(msg.Payload))

			// Acknowledge successful processing
			if err := msg.Ack(); err != nil {
				log.Printf("[%s] Failed to ack message: %v", clientID, err)
			}
		}
	}
}

// runFulfillment runs the order fulfillment consumer (single consumer in "order-fulfillment" group).
func runFulfillment(ctx context.Context, processed *int64) {
	clientID := "fulfillment-1"
	consumerGroup := "order-fulfillment"

	opts := client.NewOptions().
		SetServers(*node1Addr).
		SetClientID(clientID).
		SetProtocolVersion(5).
		SetCleanSession(true).
		SetKeepAlive(30 * time.Second).
		SetConnectTimeout(30 * time.Second).
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

	log.Printf("[%s] Connected to %s", clientID, *node1Addr)

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
			// Simulate fulfillment work (slower than validation)
			time.Sleep(200 * time.Millisecond)

			atomic.AddInt64(processed, 1)
			log.Printf("[%s] Fulfilled order (seq=%d): %s",
				clientID, msg.Sequence, string(msg.Payload))

			// Demonstrate different acknowledgment types
			if rand.Float32() < 0.05 { // 5% chance of needing retry
				log.Printf("[%s] Order needs retry, sending NACK", clientID)
				if err := msg.Nack(); err != nil {
					log.Printf("[%s] Failed to nack message: %v", clientID, err)
				}
			} else if rand.Float32() < 0.02 { // 2% chance of rejection (goes to DLQ)
				log.Printf("[%s] Order invalid, rejecting to DLQ", clientID)
				if err := msg.Reject(); err != nil {
					log.Printf("[%s] Failed to reject message: %v", clientID, err)
				}
			} else {
				if err := msg.Ack(); err != nil {
					log.Printf("[%s] Failed to ack message: %v", clientID, err)
				}
			}
		}
	}
}
