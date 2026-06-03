// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics_test

import (
	"testing"

	"github.com/absmach/fluxmq/topics"
)

const (
	testFooBar           = "foo/bar"
	testFooPlus          = "foo/+"
	testFoo              = "foo"
	testFooBarBaz        = "foo/bar/baz"
	testSysMonitorClient = "$SYS/monitor/Clients"
	testUserPlusCreated  = "user/+/created"
	testEmpty            = "empty"
	testOrdersEuCreated  = "orders.eu.created"
	testOrdersEuMqtt     = "orders/eu/created"
	testOrders           = "orders"
)

func TestTopicMatch(t *testing.T) {
	tests := []struct {
		filter string
		topic  string
		want   bool
	}{
		{testFooBar, testFooBar, true},
		{testFooPlus, testFooBar, true},
		{testFooPlus, "foo/baz", true},
		{testFooPlus, testFoo, false},
		{testFooPlus, testFooBarBaz, false},
		{"foo/#", testFooBarBaz, true},
		{"foo/#", testFoo, true},
		{"#", testFooBar, true},
		{"#", "anything", true},
		{"+/+", testFooBar, true},
		{"+/+", testFooBarBaz, false},
		{testSysMonitorClient, testSysMonitorClient, true},
		{"$SYS/#", testSysMonitorClient, true},
		{"#", testSysMonitorClient, false},
		{"+/monitor/Clients", testSysMonitorClient, false},
		{testFooBar, "foo/baz", false},
		{"", testFoo, false},
		{testFoo, "", false},
	}

	for _, tt := range tests {
		if got := topics.TopicMatch(tt.filter, tt.topic); got != tt.want {
			t.Errorf("TopicMatch(%q, %q) = %v, want %v", tt.filter, tt.topic, got, tt.want)
		}
	}
}
