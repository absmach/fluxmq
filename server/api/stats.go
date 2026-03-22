// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import "net/http"

type connectionStats struct {
	Current        uint64 `json:"current"`
	Total          uint64 `json:"total"`
	Disconnections uint64 `json:"disconnections"`
}

type messageStats struct {
	Received uint64 `json:"received"`
	Sent     uint64 `json:"sent"`
}

type byteStats struct {
	Received uint64 `json:"received"`
	Sent     uint64 `json:"sent"`
}

type errorStats struct {
	Protocol uint64 `json:"protocol"`
}

// mqttStats holds MQTT-specific counters not present in other protocols.
type mqttStats struct {
	Connections   connectionStats       `json:"connections"`
	Messages      mqttMessageStats      `json:"messages"`
	Bytes         byteStats             `json:"bytes"`
	Subscriptions mqttSubscriptionStats `json:"subscriptions"`
	Errors        mqttErrorStats        `json:"errors"`
}

type mqttMessageStats struct {
	Received        uint64 `json:"received"`
	Sent            uint64 `json:"sent"`
	PublishReceived uint64 `json:"publish_received"`
	PublishSent     uint64 `json:"publish_sent"`
}

type mqttSubscriptionStats struct {
	Active           uint64 `json:"active"`
	RetainedMessages uint64 `json:"retained_messages"`
}

type mqttErrorStats struct {
	Protocol uint64 `json:"protocol"`
	Auth     uint64 `json:"auth"`
	Authz    uint64 `json:"authz"`
	Packet   uint64 `json:"packet"`
}

// amqpStats holds AMQP 0.9.1-specific counters.
type amqpStats struct {
	Connections connectionStats `json:"connections"`
	Messages    messageStats    `json:"messages"`
	Bytes       byteStats       `json:"bytes"`
	Channels    uint64          `json:"channels"`
	Consumers   uint64          `json:"consumers"`
	Errors      errorStats      `json:"errors"`
}

type byProtocolStats struct {
	MQTT *mqttStats `json:"mqtt,omitempty"`
	AMQP *amqpStats `json:"amqp,omitempty"`
}

type statsResponse struct {
	UptimeSeconds float64         `json:"uptime_seconds"`
	Connections   connectionStats `json:"connections"`
	Messages      messageStats    `json:"messages"`
	Bytes         byteStats       `json:"bytes"`
	Errors        errorStats      `json:"errors"`
	ByProtocol    byProtocolStats `json:"by_protocol"`
}

func (s *Server) buildStatsResponse() statsResponse {
	var resp statsResponse

	if s.broker != nil {
		st := s.broker.Stats()
		resp.UptimeSeconds = st.GetUptime().Seconds()

		mqttConns := connectionStats{
			Current:        st.GetCurrentConnections(),
			Total:          st.GetTotalConnections(),
			Disconnections: st.GetDisconnections(),
		}
		mqttMsgs := mqttMessageStats{
			Received:        st.GetMessagesReceived(),
			Sent:            st.GetMessagesSent(),
			PublishReceived: st.GetPublishReceived(),
			PublishSent:     st.GetPublishSent(),
		}
		mqttBytes := byteStats{
			Received: st.GetBytesReceived(),
			Sent:     st.GetBytesSent(),
		}

		resp.Connections = mqttConns
		resp.Messages = messageStats{Received: mqttMsgs.Received, Sent: mqttMsgs.Sent}
		resp.Bytes = mqttBytes
		resp.Errors = errorStats{Protocol: st.GetProtocolErrors()}

		resp.ByProtocol.MQTT = &mqttStats{
			Connections: mqttConns,
			Messages:    mqttMsgs,
			Bytes:       mqttBytes,
			Subscriptions: mqttSubscriptionStats{
				Active:           st.GetSubscriptions(),
				RetainedMessages: st.GetRetainedMessages(),
			},
			Errors: mqttErrorStats{
				Protocol: st.GetProtocolErrors(),
				Auth:     st.GetAuthErrors(),
				Authz:    st.GetAuthzErrors(),
				Packet:   st.GetPacketErrors(),
			},
		}
	}

	if s.amqpBroker != nil {
		ast := s.amqpBroker.GetStats()

		// Use AMQP uptime if MQTT broker is absent
		if s.broker == nil {
			resp.UptimeSeconds = ast.GetUptime().Seconds()
		}

		amqpConns := connectionStats{
			Current:        ast.GetCurrentConnections(),
			Total:          ast.GetTotalConnections(),
			Disconnections: ast.GetDisconnections(),
		}
		amqpMsgs := messageStats{
			Received: ast.GetMessagesReceived(),
			Sent:     ast.GetMessagesSent(),
		}
		amqpBytes := byteStats{
			Received: ast.GetBytesReceived(),
			Sent:     ast.GetBytesSent(),
		}

		resp.Connections.Current += amqpConns.Current
		resp.Connections.Total += amqpConns.Total
		resp.Connections.Disconnections += amqpConns.Disconnections
		resp.Messages.Received += amqpMsgs.Received
		resp.Messages.Sent += amqpMsgs.Sent
		resp.Bytes.Received += amqpBytes.Received
		resp.Bytes.Sent += amqpBytes.Sent
		resp.Errors.Protocol += ast.GetProtocolErrors()

		resp.ByProtocol.AMQP = &amqpStats{
			Connections: amqpConns,
			Messages:    amqpMsgs,
			Bytes:       amqpBytes,
			Channels:    ast.GetCurrentChannels(),
			Consumers:   ast.GetConsumers(),
			Errors:      errorStats{Protocol: ast.GetProtocolErrors()},
		}
	}

	return resp
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.broker == nil && s.amqpBroker == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "broker not available")
		return
	}

	writeJSON(w, http.StatusOK, s.buildStatsResponse())
}
