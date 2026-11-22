// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Creates real protocol traffic for the console's live integration test.
// Run only against a disposable broker: this creates sessions and queues.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	amqp "github.com/rabbitmq/amqp091-go"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func wait(t mqtt.Token) {
	if !t.WaitTimeout(10 * time.Second) {
		log.Fatal("MQTT operation timed out")
	}
	check(t.Error())
}
func main() {
	mqttURL := flag.String("mqtt", "", "Required disposable MQTT broker URL")
	amqpURL := flag.String("amqp", "", "Required disposable AMQP 0.9.1 broker URL")
	duration := flag.Duration("duration", 3*time.Minute, "How long to keep clients and traffic active")
	flag.Parse()
	if *mqttURL == "" || *amqpURL == "" {
		log.Fatal("both -mqtt and -amqp are required; use a disposable test broker")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	newClient := func(id string, clean bool) mqtt.Client {
		c := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(*mqttURL).SetClientID(id).SetCleanSession(clean).SetProtocolVersion(4))
		wait(c.Connect())
		return c
	}
	subscriber := newClient("console-live-subscriber", true)
	defer subscriber.Disconnect(250)
	filters := map[string]byte{"console/live/+/temperature": 1}
	for i := 0; i < 205; i++ {
		filters[fmt.Sprintf("console/live/page/%03d", i)] = 1
	}
	wait(subscriber.SubscribeMultiple(filters, func(mqtt.Client, mqtt.Message) {}))
	offline := newClient("console-live-offline", false)
	wait(offline.Subscribe("console/live/offline", 1, nil))
	offline.Disconnect(250)
	publisher := newClient("console-live-publisher", true)
	defer publisher.Disconnect(250)
	for i := 0; i < 3; i++ {
		wait(publisher.Publish("console/live/offline", 1, false, "offline payload"))
	}
	conn, err := amqp.DialConfig(*amqpURL, amqp.Config{Properties: amqp.Table{"connection_name": "console-live-amqp"}})
	check(err)
	defer conn.Close()
	ch, err := conn.Channel()
	check(err)
	defer ch.Close()
	_, err = ch.QueueDeclare("console-live-stream", true, false, false, false, amqp.Table{"x-queue-type": "stream"})
	check(err)
	_, err = ch.QueueDeclare("console-live-classic", true, false, false, false, nil)
	check(err)
	deliveries, err := ch.Consume("$queue/console-live-classic/#", "console-live-consumer", false, false, false, false, amqp.Table{"x-consumer-group": "console-live-workers"})
	check(err)
	go func() {
		for d := range deliveries {
			if string(d.Body) != "keep-pending" {
				check(d.Ack(false))
			}
		}
	}()
	check(ch.PublishWithContext(ctx, "", "console-live-classic", false, false, amqp.Publishing{Body: []byte("keep-pending")}))
	// Interleave sources in the same retained stream to exercise the source filter.
	for i := 0; i < 12; i++ {
		wait(publisher.Publish("$queue/console-live-stream/events", 1, false, fmt.Sprintf(`{"source":"mqtt","seq":%d}`, i)))
		check(ch.PublishWithContext(ctx, "", "console-live-stream", false, false, amqp.Publishing{ContentType: "application/json", Body: []byte(fmt.Sprintf(`{"source":"amqp","seq":%d}`, i))}))
	}
	fmt.Println("READY: real MQTT sessions, paginated subscriptions, AMQP queues, mixed-source messages and pending delivery")
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			wait(publisher.Publish("console/live/device/temperature", 1, false, `{"temperature":23}`))
			check(ch.PublishWithContext(ctx, "", "console-live-classic", false, false, amqp.Publishing{Body: []byte("ack-me")}))
		}
	}
}
