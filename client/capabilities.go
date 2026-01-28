// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import v5 "github.com/absmach/fluxmq/mqtt/packets/v5"

// ServerCapabilities represents the capabilities and limits
// advertised by the server in the CONNACK packet (MQTT 5.0).
type ServerCapabilities struct {
	// SessionExpiryInterval is the session expiry interval negotiated by the server.
	// If nil, the server accepted the client's requested value.
	SessionExpiryInterval *uint32

	// ReceiveMaximum is the maximum number of QoS 1 and QoS 2 publications
	// that the server is willing to process concurrently.
	// Default is 65535 if not present.
	ReceiveMaximum uint16

	// MaximumQoS is the maximum QoS level the server supports.
	// Default is 2 (QoS 0, 1, and 2 supported).
	MaximumQoS byte

	// RetainAvailable indicates whether the server supports retained messages.
	// Default is true if not present.
	RetainAvailable bool

	// MaximumPacketSize is the maximum packet size the server is willing to accept.
	// If nil, there is no limit beyond the protocol maximum.
	MaximumPacketSize *uint32

	// AssignedClientID is the client identifier assigned by the server if the
	// client connected with an empty client ID.
	AssignedClientID string

	// TopicAliasMaximum is the maximum topic alias value the server accepts.
	// 0 means topic aliases are not supported by the server.
	TopicAliasMaximum uint16

	// ReasonString provides additional diagnostic information.
	ReasonString string

	// UserProperties contains user-defined properties from the server.
	UserProperties map[string]string

	// WildcardSubscriptionAvailable indicates whether wildcard subscriptions are supported.
	// Default is true if not present.
	WildcardSubscriptionAvailable bool

	// SubscriptionIdentifiersAvailable indicates whether subscription identifiers are supported.
	// Default is true if not present.
	SubscriptionIdentifiersAvailable bool

	// SharedSubscriptionAvailable indicates whether shared subscriptions are supported.
	// Default is true if not present.
	SharedSubscriptionAvailable bool

	// ServerKeepAlive is the keep alive time assigned by the server.
	// If nil, the server accepted the client's requested value.
	ServerKeepAlive *uint16

	// ResponseInformation can be used by the client to construct response topics.
	ResponseInformation string

	// ServerReference indicates another server the client can use.
	ServerReference string

	// AuthenticationMethod is the authentication method used.
	AuthenticationMethod string

	// AuthenticationData contains authentication-specific data.
	AuthenticationData []byte
}

// parseConnAckProperties parses MQTT 5.0 CONNACK properties into ServerCapabilities.
func parseConnAckProperties(props *v5.ConnAckProperties) *ServerCapabilities {
	if props == nil {
		return &ServerCapabilities{
			ReceiveMaximum:                   65535, // Default
			MaximumQoS:                       2,     // Default
			RetainAvailable:                  true,  // Default
			WildcardSubscriptionAvailable:    true,  // Default
			SubscriptionIdentifiersAvailable: true,  // Default
			SharedSubscriptionAvailable:      true,  // Default
		}
	}

	caps := &ServerCapabilities{
		SessionExpiryInterval: props.SessionExpiryInterval,
		MaximumPacketSize:     props.MaximumPacketSize,
		AssignedClientID:      props.AssignedClientID,
		ReasonString:          props.ReasonString,
		ServerKeepAlive:       props.ServerKeepAlive,
		ResponseInformation:   props.ResponseInfo,
		ServerReference:       props.ServerReference,
		AuthenticationMethod:  props.AuthMethod,
		AuthenticationData:    props.AuthData,
	}

	// ReceiveMaximum (default 65535)
	if props.ReceiveMax != nil {
		caps.ReceiveMaximum = *props.ReceiveMax
	} else {
		caps.ReceiveMaximum = 65535
	}

	// MaximumQoS (default 2)
	if props.MaxQoS != nil {
		caps.MaximumQoS = *props.MaxQoS
	} else {
		caps.MaximumQoS = 2
	}

	// RetainAvailable (default true)
	if props.RetainAvailable != nil {
		caps.RetainAvailable = *props.RetainAvailable != 0
	} else {
		caps.RetainAvailable = true
	}

	// TopicAliasMaximum (default 0 - not supported)
	if props.TopicAliasMax != nil {
		caps.TopicAliasMaximum = *props.TopicAliasMax
	}

	// WildcardSubscriptionAvailable (default true)
	if props.WildcardSubAvailable != nil {
		caps.WildcardSubscriptionAvailable = *props.WildcardSubAvailable != 0
	} else {
		caps.WildcardSubscriptionAvailable = true
	}

	// SubscriptionIdentifiersAvailable (default true)
	if props.SubIDAvailable != nil {
		caps.SubscriptionIdentifiersAvailable = *props.SubIDAvailable != 0
	} else {
		caps.SubscriptionIdentifiersAvailable = true
	}

	// SharedSubscriptionAvailable (default true - MQTT 5.0 spec)
	// Note: This property doesn't exist in CONNACK, so we default to true
	caps.SharedSubscriptionAvailable = true

	// User properties
	if len(props.User) > 0 {
		caps.UserProperties = make(map[string]string, len(props.User))
		for _, u := range props.User {
			caps.UserProperties[u.Key] = u.Value
		}
	}

	return caps
}
