// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
)

type result struct {
	Scenario       string  `json:"scenario"`
	Description    string  `json:"description"`
	PayloadLabel   string  `json:"payload_label"`
	PayloadBytes   int     `json:"payload_bytes"`
	Publishers     int     `json:"publishers"`
	Subscribers    int     `json:"subscribers"`
	ConsumerGroups int     `json:"consumer_groups"`
	Published      int64   `json:"published"`
	Expected       int64   `json:"expected"`
	Received       int64   `json:"received"`
	PublishRateMPS float64 `json:"publish_rate_mps"`
	ReceiveRateMPS float64 `json:"receive_rate_mps"`
	DeliveryRatio  float64 `json:"delivery_ratio"`
	Errors         int64   `json:"errors"`
	Pass           bool    `json:"pass"`
	Notes          string  `json:"notes"`
}

func main() {
	input := flag.String("input", "", "Path to JSONL results file")
	flag.Parse()

	if *input == "" {
		fmt.Fprintln(os.Stderr, "-input is required")
		os.Exit(2)
	}

	f, err := os.Open(*input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", *input, err)
		os.Exit(2)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	w := tabwriter.NewWriter(os.Stdout, 2, 2, 2, ' ', 0)
	fmt.Fprintln(w, "SCENARIO\tMSG_SIZE\tSENT\tEXPECTED\tRECEIVED\tPUB\tCONS\tGROUPS\tMPS_SENT\tMPS_RECV\tRATIO\tPASS\tERRORS\tDESCRIPTION")
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var r result
		if err := json.Unmarshal(line, &r); err != nil {
			continue
		}
		desc := r.Description
		if desc == "" {
			desc = "-"
		}
		fmt.Fprintf(w, "%s\t%dB\t%d\t%d\t%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.4f\t%v\t%d\t%s\n",
			r.Scenario,
			r.PayloadBytes,
			r.Published,
			r.Expected,
			r.Received,
			r.Publishers,
			r.Subscribers,
			r.ConsumerGroups,
			r.PublishRateMPS,
			r.ReceiveRateMPS,
			r.DeliveryRatio,
			r.Pass,
			r.Errors,
			desc,
		)
		if r.Notes != "" {
			fmt.Fprintf(w, "notes\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\t%s\n", r.Notes)
		}
	}
	_ = w.Flush()

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to read %s: %v\n", *input, err)
		os.Exit(2)
	}
}
