// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authatom

import (
	"errors"
	"testing"
)

const (
	testTenantID   = "11111111-1111-4111-8111-111111111111"
	testResourceID = "22222222-2222-4222-8222-222222222222"
)

func TestParseMagistralaTopic(t *testing.T) {
	cases := []struct {
		name      string
		topic     string
		action    topicAction
		wantErr   error
		wantRoute topicRoute
	}{
		{
			name:   "uuid route publish",
			topic:  "m/" + testTenantID + "/c/" + testResourceID + "/temp",
			action: actionPublish,
			wantRoute: topicRoute{
				tenant:      testTenantID,
				channel:     testResourceID,
				subpath:     "temp",
				tenantUUID:  true,
				channelUUID: true,
				normalized:  "m/" + testTenantID + "/c/" + testResourceID + "/temp",
				raw:         "m/" + testTenantID + "/c/" + testResourceID + "/temp",
			},
		},
		{
			name:   "alias route subscribe wildcard after channel",
			topic:  "m/factory-a/c/telemetry/line-1/#",
			action: actionSubscribe,
			wantRoute: topicRoute{
				tenant:       "factory-a",
				channel:      "telemetry",
				subpath:      "line-1/#",
				normalized:   "m/factory-a/c/telemetry/line-1/#",
				raw:          "m/factory-a/c/telemetry/line-1/#",
				isSubscribe:  true,
				channelAlias: "telemetry",
				tenantAlias:  "factory-a",
			},
		},
		{
			name:    "tenant wildcard denied",
			topic:   "m/+/c/telemetry",
			action:  actionSubscribe,
			wantErr: errUnsupportedTopicFilter,
		},
		{
			name:    "channel wildcard denied",
			topic:   "m/factory-a/c/#",
			action:  actionSubscribe,
			wantErr: errUnsupportedTopicFilter,
		},
		{
			name:    "broad filter denied",
			topic:   "#",
			action:  actionSubscribe,
			wantErr: errUnsupportedTopic,
		},
		{
			name:    "tenant alias with resource uuid denied",
			topic:   "m/factory-a/c/" + testResourceID,
			action:  actionPublish,
			wantErr: errUnsupportedAliasRoute,
		},
		{
			name:    "publish wildcard denied",
			topic:   "m/" + testTenantID + "/c/" + testResourceID + "/+",
			action:  actionPublish,
			wantErr: errInvalidTopicRoute,
		},
		{
			name:    "hash must be final",
			topic:   "m/factory-a/c/telemetry/#/extra",
			action:  actionSubscribe,
			wantErr: errUnsupportedTopicFilter,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseMagistralaTopic(tc.topic, tc.action)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("parseMagistralaTopic() error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMagistralaTopic() error = %v", err)
			}
			if got != tc.wantRoute {
				t.Fatalf("route = %#v, want %#v", got, tc.wantRoute)
			}
		})
	}
}
