// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hookcallout

import (
	"strings"

	"github.com/absmach/fluxmq/broker"
	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
)

func protocolToProto(protocol string) authv1.Protocol {
	switch strings.ToLower(protocol) {
	case broker.HookProtocolMQTT:
		return authv1.Protocol_MQTT
	case broker.HookProtocolAMQP10:
		return authv1.Protocol_AMQP_1_0
	case broker.HookProtocolAMQP091:
		return authv1.Protocol_AMQP_0_9_1
	case broker.HookProtocolHTTP:
		return authv1.Protocol_HTTP
	case broker.HookProtocolCoAP:
		return authv1.Protocol_CoAP
	default:
		return authv1.Protocol_Unspecified
	}
}

func protocolToString(protocol string) string {
	return strings.ToLower(protocol)
}

func hookToProto(hook string) authv1.HookType {
	switch strings.ToLower(hook) {
	case broker.HookAuthOnRegister:
		return authv1.HookType_AuthOnRegister
	case broker.HookAuthOnPublish:
		return authv1.HookType_AuthOnPublish
	case broker.HookAuthOnSubscribe:
		return authv1.HookType_AuthOnSubscribe
	case broker.HookAuthOnUnsubscribe:
		return authv1.HookType_AuthOnUnsubscribe
	default:
		return authv1.HookType_HookTypeUnspecified
	}
}

func hookToString(hook string) string {
	return strings.ToLower(hook)
}

func resultFromProto(res *authv1.HookRes) broker.BlockingHookResult {
	if res == nil {
		return broker.BlockingHookResult{Allowed: true}
	}
	return broker.BlockingHookResult{
		Allowed:    res.GetResult() != authv1.HookResult_HookResultDeny,
		Topic:      res.GetTopic(),
		Payload:    append([]byte(nil), res.GetPayload()...),
		PayloadSet: res.GetPayloadSet(),
		QoS:        byte(res.GetQos()),
		QoSSet:     res.GetQosSet(),
		Retain:     res.GetRetain(),
		RetainSet:  res.GetRetainSet(),
		Properties: res.GetProperties(),
		ExternalID: res.GetExternalId(),
		Reason:     res.GetReason(),
		ReasonCode: res.GetReasonCode(),
	}
}
