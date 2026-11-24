// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// queue-load exercises real pending deliveries, acknowledgements, retries and
// dead-letter transfers. Run alongside the all-protocol publisher workload.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Remember only outstanding injected retries across the competing consumers.
var retryPending sync.Map

func main() {
	endpoints := flag.String("amqp", "", "Comma-separated AMQP 0.9.1 URLs (required)")
	prefix := flag.String("prefix", "console-queue-load", "Prefix for dedicated test queues")
	interval := flag.Duration("interval", 50*time.Millisecond, "Publish interval per node")
	flag.Parse()
	if *endpoints == "" || *interval <= 0 {
		log.Fatal("provide -amqp and a positive -interval")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup
	for i, endpoint := range strings.Split(*endpoints, ",") {
		// Two connections in the same group exercise retries across consumers.
		for worker := 0; worker < 2; worker++ {
			wg.Add(1)
			go func(i int, endpoint string) {
				defer wg.Done()
				name := fmt.Sprintf("%s-node%d", *prefix, i+1)
				for ctx.Err() == nil {
					if err := run(ctx, strings.TrimSpace(endpoint), name, 2**interval); err != nil && ctx.Err() == nil {
						log.Printf("%s: %v; reconnecting", name, err)
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Second):
					}
				}
			}(i, endpoint)
		}
	}
	wg.Wait()
}

func run(ctx context.Context, endpoint, name string, interval time.Duration) error {
	conn, err := amqp.DialConfig(endpoint, amqp.Config{Heartbeat: 10 * time.Second, Dial: amqp.DefaultDial(5 * time.Second)})
	if err != nil {
		return err
	}
	defer conn.Close()
	pub, err := conn.Channel()
	if err != nil {
		return err
	}
	sub, err := conn.Channel()
	if err != nil {
		return err
	}
	if _, err = pub.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": "classic"}); err != nil {
		return err
	}
	if err = sub.Qos(16, 0, false); err != nil {
		return err
	}
	deliveries, err := sub.Consume("$queue/"+name+"/#", name+"-consumer", false, false, false, false, amqp.Table{"x-consumer-group": "console-load"})
	if err != nil {
		return err
	}
	if err = pub.Confirm(false); err != nil {
		return err
	}
	confirms := pub.NotifyPublish(make(chan amqp.Confirmation, 1))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errors := make(chan error, 1)
	fail := func(err error) {
		if err != nil {
			select {
			case errors <- err:
			default:
			}
		}
	}
	var acked, retried, rejected, recovered atomic.Uint64
	var workers sync.WaitGroup
	defer workers.Wait()
	// Consume with a bounded prefetch and delayed outcomes, keeping real messages
	// pending long enough to be observed by the console's two-second sampler.
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					fail(fmt.Errorf("delivery channel closed"))
					return
				}
				workers.Add(1)
				go func(d amqp.Delivery) {
					defer workers.Done()
					parts := strings.Split(string(d.Body), ":")
					n, _ := strconv.Atoi(parts[len(parts)-1])
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Duration(350+n%5*100) * time.Millisecond):
					}
					switch {
					case n%17 == 0:
						fail(d.Reject(false))
						rejected.Add(1)
					case n%10 == 0:
						key := name + ":" + string(d.Body)
						if _, loaded := retryPending.LoadOrStore(key, true); !loaded {
							fail(d.Nack(false, true))
							retried.Add(1)
						} else {
							fail(d.Ack(false))
							acked.Add(1)
							recovered.Add(1)
							retryPending.Delete(key)
						}
					default:
						fail(d.Ack(false))
						acked.Add(1)
					}
				}(d)
			}
		}
	}()
	// Cancel workers before waiting, including on a failed publisher confirmation.
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	report := time.NewTicker(5 * time.Second)
	defer report.Stop()
	sequence := 0
	runID := strconv.FormatInt(time.Now().UnixNano(), 10)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errors:
			return err
		case <-report.C:
			log.Printf("%s published=%d ack=%d nack=%d reject=%d recovered=%d", name, sequence, acked.Load(), retried.Load(), rejected.Load(), recovered.Load())
		case <-ticker.C:
			sequence++
			err := pub.PublishWithContext(ctx, "", "$queue/"+name+"/events", false, false, amqp.Publishing{ContentType: "text/plain", DeliveryMode: amqp.Persistent, Body: []byte(runID + ":" + strconv.Itoa(sequence))})
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return nil
			case c, ok := <-confirms:
				if !ok || !c.Ack {
					return fmt.Errorf("publisher confirmation failed")
				}
			case <-time.After(5 * time.Second):
				return fmt.Errorf("publisher confirmation timeout")
			}
		}
	}
}
