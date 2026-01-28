// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"testing"

	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

func TestParseConnAckProperties_Nil(t *testing.T) {
	caps := parseConnAckProperties(nil)

	if caps.ReceiveMaximum != 65535 {
		t.Errorf("Expected default ReceiveMaximum 65535, got %d", caps.ReceiveMaximum)
	}
	if caps.MaximumQoS != 2 {
		t.Errorf("Expected default MaximumQoS 2, got %d", caps.MaximumQoS)
	}
	if !caps.RetainAvailable {
		t.Error("Expected default RetainAvailable true")
	}
	if !caps.WildcardSubscriptionAvailable {
		t.Error("Expected default WildcardSubscriptionAvailable true")
	}
	if !caps.SubscriptionIdentifiersAvailable {
		t.Error("Expected default SubscriptionIdentifiersAvailable true")
	}
	if !caps.SharedSubscriptionAvailable {
		t.Error("Expected default SharedSubscriptionAvailable true")
	}
}

func TestParseConnAckProperties_Complete(t *testing.T) {
	sessionExpiry := uint32(3600)
	receiveMax := uint16(100)
	maxQoS := byte(1)
	retainAvail := byte(1)
	maxPacketSize := uint32(65536)
	topicAliasMax := uint16(10)
	wildcardSubAvail := byte(1)
	subIDAvail := byte(1)
	serverKeepAlive := uint16(120)

	props := &v5.ConnAckProperties{
		SessionExpiryInterval: &sessionExpiry,
		ReceiveMax:            &receiveMax,
		MaxQoS:                &maxQoS,
		RetainAvailable:       &retainAvail,
		MaximumPacketSize:     &maxPacketSize,
		AssignedClientID:      "auto-generated-id",
		TopicAliasMax:         &topicAliasMax,
		ReasonString:          "Connection accepted",
		User: []v5.User{
			{Key: "region", Value: "us-west"},
			{Key: "tier", Value: "premium"},
		},
		WildcardSubAvailable: &wildcardSubAvail,
		SubIDAvailable:       &subIDAvail,
		ServerKeepAlive:      &serverKeepAlive,
		ResponseInfo:         "/response/client-123",
		ServerReference:      "backup.mqtt.example.com",
		AuthMethod:           "SCRAM-SHA-256",
		AuthData:             []byte("auth-data"),
	}

	caps := parseConnAckProperties(props)

	if caps.SessionExpiryInterval == nil || *caps.SessionExpiryInterval != sessionExpiry {
		t.Error("SessionExpiryInterval mismatch")
	}
	if caps.ReceiveMaximum != receiveMax {
		t.Errorf("Expected ReceiveMaximum %d, got %d", receiveMax, caps.ReceiveMaximum)
	}
	if caps.MaximumQoS != maxQoS {
		t.Errorf("Expected MaximumQoS %d, got %d", maxQoS, caps.MaximumQoS)
	}
	if !caps.RetainAvailable {
		t.Error("Expected RetainAvailable true")
	}
	if caps.MaximumPacketSize == nil || *caps.MaximumPacketSize != maxPacketSize {
		t.Error("MaximumPacketSize mismatch")
	}
	if caps.AssignedClientID != "auto-generated-id" {
		t.Errorf("Expected AssignedClientID 'auto-generated-id', got %s", caps.AssignedClientID)
	}
	if caps.TopicAliasMaximum != topicAliasMax {
		t.Errorf("Expected TopicAliasMaximum %d, got %d", topicAliasMax, caps.TopicAliasMaximum)
	}
	if caps.ReasonString != "Connection accepted" {
		t.Errorf("Expected ReasonString 'Connection accepted', got %s", caps.ReasonString)
	}
	if len(caps.UserProperties) != 2 {
		t.Errorf("Expected 2 user properties, got %d", len(caps.UserProperties))
	}
	if caps.UserProperties["region"] != "us-west" {
		t.Error("User property 'region' mismatch")
	}
	if caps.UserProperties["tier"] != "premium" {
		t.Error("User property 'tier' mismatch")
	}
	if !caps.WildcardSubscriptionAvailable {
		t.Error("Expected WildcardSubscriptionAvailable true")
	}
	if !caps.SubscriptionIdentifiersAvailable {
		t.Error("Expected SubscriptionIdentifiersAvailable true")
	}
	if caps.ServerKeepAlive == nil || *caps.ServerKeepAlive != serverKeepAlive {
		t.Error("ServerKeepAlive mismatch")
	}
	if caps.ResponseInformation != "/response/client-123" {
		t.Errorf("Expected ResponseInformation '/response/client-123', got %s", caps.ResponseInformation)
	}
	if caps.ServerReference != "backup.mqtt.example.com" {
		t.Errorf("Expected ServerReference 'backup.mqtt.example.com', got %s", caps.ServerReference)
	}
	if caps.AuthenticationMethod != "SCRAM-SHA-256" {
		t.Errorf("Expected AuthenticationMethod 'SCRAM-SHA-256', got %s", caps.AuthenticationMethod)
	}
	if string(caps.AuthenticationData) != "auth-data" {
		t.Error("AuthenticationData mismatch")
	}
}

func TestParseConnAckProperties_DisabledFeatures(t *testing.T) {
	receiveMax := uint16(50)
	maxQoS := byte(0)
	retainAvail := byte(0)
	topicAliasMax := uint16(0)
	wildcardSubAvail := byte(0)
	subIDAvail := byte(0)

	props := &v5.ConnAckProperties{
		ReceiveMax:           &receiveMax,
		MaxQoS:               &maxQoS,
		RetainAvailable:      &retainAvail,
		TopicAliasMax:        &topicAliasMax,
		WildcardSubAvailable: &wildcardSubAvail,
		SubIDAvailable:       &subIDAvail,
	}

	caps := parseConnAckProperties(props)

	if caps.ReceiveMaximum != receiveMax {
		t.Errorf("Expected ReceiveMaximum %d, got %d", receiveMax, caps.ReceiveMaximum)
	}
	if caps.MaximumQoS != 0 {
		t.Errorf("Expected MaximumQoS 0, got %d", caps.MaximumQoS)
	}
	if caps.RetainAvailable {
		t.Error("Expected RetainAvailable false")
	}
	if caps.TopicAliasMaximum != 0 {
		t.Errorf("Expected TopicAliasMaximum 0, got %d", caps.TopicAliasMaximum)
	}
	if caps.WildcardSubscriptionAvailable {
		t.Error("Expected WildcardSubscriptionAvailable false")
	}
	if caps.SubscriptionIdentifiersAvailable {
		t.Error("Expected SubscriptionIdentifiersAvailable false")
	}
}

func TestParseConnAckProperties_PartialProperties(t *testing.T) {
	receiveMax := uint16(200)
	assignedID := "server-assigned-123"

	props := &v5.ConnAckProperties{
		ReceiveMax:       &receiveMax,
		AssignedClientID: assignedID,
	}

	caps := parseConnAckProperties(props)

	// Explicit properties
	if caps.ReceiveMaximum != receiveMax {
		t.Errorf("Expected ReceiveMaximum %d, got %d", receiveMax, caps.ReceiveMaximum)
	}
	if caps.AssignedClientID != assignedID {
		t.Errorf("Expected AssignedClientID %s, got %s", assignedID, caps.AssignedClientID)
	}

	// Default properties
	if caps.MaximumQoS != 2 {
		t.Errorf("Expected default MaximumQoS 2, got %d", caps.MaximumQoS)
	}
	if !caps.RetainAvailable {
		t.Error("Expected default RetainAvailable true")
	}
	if caps.TopicAliasMaximum != 0 {
		t.Errorf("Expected default TopicAliasMaximum 0, got %d", caps.TopicAliasMaximum)
	}
	if !caps.WildcardSubscriptionAvailable {
		t.Error("Expected default WildcardSubscriptionAvailable true")
	}
	if !caps.SubscriptionIdentifiersAvailable {
		t.Error("Expected default SubscriptionIdentifiersAvailable true")
	}
}
