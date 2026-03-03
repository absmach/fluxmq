// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/absmach/fluxmq/tests/perf/mockmqtt"
)

func main() {
	addrsFlag := flag.String("addrs", "127.0.0.1:1883,127.0.0.1:1885,127.0.0.1:1887", "Comma-separated MQTT mock node addresses")
	flag.Parse()

	addrs := splitAddrs(*addrsFlag)
	if len(addrs) == 0 {
		fmt.Fprintln(os.Stderr, "no addresses provided")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cluster := mockmqtt.New(addrs)
	if err := cluster.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start mock cluster: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("mock mqtt cluster listening on %s\n", strings.Join(cluster.Addrs(), ","))

	<-ctx.Done()
	if err := cluster.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to stop mock cluster cleanly: %v\n", err)
		os.Exit(1)
	}
}

func splitAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}
