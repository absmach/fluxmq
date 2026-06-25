// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authatom

import (
	"errors"
	"testing"
)

const (
	testTenantID              = "11111111-1111-4111-8111-111111111111"
	testResourceID            = "22222222-2222-4222-8222-222222222222"
	testTenantAlias           = "factory-a"
	testChannelAlias          = "telemetry"
	testAliasTopicBase        = "m/" + testTenantAlias + "/c/" + testChannelAlias
	testAliasSubpath          = "line-1/#"
	testAliasPublishTopic     = testAliasTopicBase + "/temp"
	testAliasSubscribeFilter  = testAliasTopicBase + "/" + testAliasSubpath
	testSharedSubscribeFilter = "$share/workers/" + testAliasSubscribeFilter
	testAMQPSubscribeFilter   = "m." + testTenantAlias + ".c." + testChannelAlias + ".*"
	testAMQPNormalizedFilter  = testAliasTopicBase + "/+"
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
			topic:  testAliasSubscribeFilter,
			action: actionSubscribe,
			wantRoute: topicRoute{
				tenant:       testTenantAlias,
				channel:      testChannelAlias,
				subpath:      testAliasSubpath,
				normalized:   testAliasSubscribeFilter,
				raw:          testAliasSubscribeFilter,
				isSubscribe:  true,
				channelAlias: testChannelAlias,
				tenantAlias:  testTenantAlias,
			},
		},
		{
			name:    "tenant wildcard denied",
			topic:   "m/+/c/" + testChannelAlias,
			action:  actionSubscribe,
			wantErr: errUnsupportedTopicFilter,
		},
		{
			name:    "channel wildcard denied",
			topic:   "m/" + testTenantAlias + "/c/#",
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
			topic:   "m/" + testTenantAlias + "/c/" + testResourceID,
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
			topic:   testAliasTopicBase + "/#/extra",
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
