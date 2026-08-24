// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package hook

import (
	"fmt"
	"strings"

	"github.com/absmach/fluxmq/broker"
	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
)

func protocolToProto(protocol string) authv1.Protocol {
	switch strings.ToLower(protocol) {
	case broker.HookProtocolMQTT:
		return authv1.Protocol_PROTOCOL_MQTT
	case broker.HookProtocolAMQP10:
		return authv1.Protocol_PROTOCOL_AMQP_1_0
	case broker.HookProtocolAMQP091:
		return authv1.Protocol_PROTOCOL_AMQP_0_9_1
	case broker.HookProtocolHTTP:
		return authv1.Protocol_PROTOCOL_HTTP
	case broker.HookProtocolCoAP:
		return authv1.Protocol_PROTOCOL_COAP
	default:
		return authv1.Protocol_PROTOCOL_UNSPECIFIED
	}
}

func protocolToString(protocol string) string {
	return strings.ToLower(protocol)
}

func hookToProto(hook string) authv1.HookType {
	switch strings.ToLower(hook) {
	case broker.HookAuthOnRegister:
		return authv1.HookType_HOOK_TYPE_AUTH_ON_REGISTER
	case broker.HookAuthOnPublish:
		return authv1.HookType_HOOK_TYPE_AUTH_ON_PUBLISH
	case broker.HookAuthOnSubscribe:
		return authv1.HookType_HOOK_TYPE_AUTH_ON_SUBSCRIBE
	case broker.HookAuthOnUnsubscribe:
		return authv1.HookType_HOOK_TYPE_AUTH_ON_UNSUBSCRIBE
	default:
		return authv1.HookType_HOOK_TYPE_UNSPECIFIED
	}
}

func hookToString(hook string) string {
	return strings.ToLower(hook)
}

func resultFromProto(res *authv1.HookRes) (broker.BlockingHookResult, error) {
	if res == nil {
		return broker.BlockingHookResult{}, fmt.Errorf("nil hook response")
	}

	var allowed bool
	switch res.GetResult() {
	case authv1.HookResult_HOOK_RESULT_OK:
		allowed = true
	case authv1.HookResult_HOOK_RESULT_DENY:
		allowed = false
	default:
		return broker.BlockingHookResult{}, fmt.Errorf("unknown hook result %s", res.GetResult().String())
	}

	return broker.BlockingHookResult{
		Allowed:    allowed,
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
	}, nil
}
