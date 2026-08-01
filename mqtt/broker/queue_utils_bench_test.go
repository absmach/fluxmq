// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"

	corebroker "github.com/absmach/fluxmq/broker"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

// The reserved-property filter runs for every user property on every v5
// PUBLISH, so it must not add allocations to the publish path.
func BenchmarkExtractAllProperties(b *testing.B) {
	benchmarks := []struct {
		name string
		user []v5.User
	}{
		{name: "no properties"},
		{
			name: "ordinary properties",
			user: []v5.User{{Key: testTraceKey, Value: testTraceVal}, {Key: testTenantKey, Value: testTenantValue}},
		},
		{
			name: "reserved property",
			user: []v5.User{
				{Key: testTraceKey, Value: testTraceVal},
				{Key: corebroker.ReservedPropertyPrefix + "re.trace", Value: `["rule-a"]`},
			},
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			props := &v5.PublishProperties{User: bm.user}
			b.ReportAllocs()
			for b.Loop() {
				_ = extractAllProperties(props)
			}
		})
	}
}
