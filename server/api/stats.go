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
	Connections     connectionStats         `json:"connections"`
	Messages        messageStats            `json:"messages"`
	Bytes           byteStats               `json:"bytes"`
	Channels        uint64                  `json:"channels"`
	Consumers       uint64                  `json:"consumers"`
	Errors          errorStats              `json:"errors"`
	LocalPrincipals amqpLocalPrincipalStats `json:"local_principals"`
}

// amqpLocalPrincipalStats uses a fixed set of counters. No principal,
// certificate, URI, routing-key, or tenant values become metric dimensions.
type amqpLocalPrincipalStats struct {
	ActiveConnections uint64                       `json:"active_connections"`
	PublishTimeouts   uint64                       `json:"publish_timeouts"`
	PublishRejections uint64                       `json:"publish_rejections"`
	Authentication    amqpLocalAuthenticationStats `json:"authentication"`
	Authorization     amqpLocalAuthorizationStats  `json:"authorization"`
	Reloads           amqpLocalReloadStats         `json:"reloads"`
}

type amqpLocalAuthenticationStats struct {
	Success uint64 `json:"success"`
	Failure uint64 `json:"failure"`
}

type amqpLocalAuthorizationStats struct {
	PublishDenied   uint64 `json:"publish_denied"`
	SubscribeDenied uint64 `json:"subscribe_denied"`
	OperationDenied uint64 `json:"operation_denied"`
}

type amqpLocalReloadStats struct {
	Success           uint64 `json:"success"`
	Failure           uint64 `json:"failure"`
	ForcedDisconnects uint64 `json:"forced_disconnects"`
}

type byProtocolStats struct {
	MQTT *mqttStats `json:"mqtt,omitempty"`
	AMQP *amqpStats `json:"amqp,omitempty"`
}

// queueStats reports the queue manager's counters. Queues are shared by every
// protocol, so this sits beside by_protocol rather than inside it.
type queueStats struct {
	// CaptureFailures counts matching queues a captured publish failed to
	// reach, and CaptureDropped counts capture jobs discarded before they were
	// attempted. Capture runs off the publish path and never fails the publish,
	// so without these a queue silently dropping the traffic its pattern binds
	// would leave no trace outside the logs. A rising CaptureDropped says
	// capture cannot keep up; a rising CaptureFailures says the store is
	// rejecting writes.
	CaptureFailures uint64            `json:"capture_failures"`
	CaptureDropped  uint64            `json:"capture_dropped"`
	Claims          queueAttemptStats `json:"claims"`
	Steals          queueAttemptStats `json:"steals"`
	Acknowledgments queueAckStats     `json:"acknowledgments"`
	Pending         queuePendingStats `json:"pending"`
}

type queueAttemptStats struct {
	Attempts  uint64 `json:"attempts"`
	Successes uint64 `json:"successes"`
	Failures  uint64 `json:"failures"`
}

type queueAckStats struct {
	Ack    uint64 `json:"ack"`
	Nack   uint64 `json:"nack"`
	Reject uint64 `json:"reject"`
	DLQ    uint64 `json:"dlq"`

	// DLQTransferFailures counts dead-letter transfers that could not be
	// completed, and PoisonWithoutDLQ counts poison messages returned to
	// ordinary redelivery because their queue has no destination at all.
	//
	// Both are operator-facing: a message that keeps being redelivered and
	// never dead-lettered looks like an ordinary retry loop from the outside,
	// and these are what distinguish it from one.
	DLQTransferFailures uint64 `json:"dlq_transfer_failures"`
	PoisonWithoutDLQ    uint64 `json:"poison_without_dlq"`

	// PoisonPending and PoisonPendingNoDestination are gauges: how many entries
	// are stuck now, and how many of those have nowhere to go. These are the
	// values worth alerting on; the counters above only say it has happened.
	PoisonPending              uint64 `json:"poison_pending"`
	PoisonPendingNoDestination uint64 `json:"poison_pending_no_destination"`
}

// queuePendingStats describes the pending entry list, which only classic queues
// maintain: a stream group tracks a cursor instead.
type queuePendingStats struct {
	Current   uint64 `json:"current"`
	HighWater uint64 `json:"high_water"`
}

type statsResponse struct {
	UptimeSeconds float64         `json:"uptime_seconds"`
	Connections   connectionStats `json:"connections"`
	Messages      messageStats    `json:"messages"`
	Bytes         byteStats       `json:"bytes"`
	Errors        errorStats      `json:"errors"`
	ByProtocol    byProtocolStats `json:"by_protocol"`
	Queues        *queueStats     `json:"queues,omitempty"`
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
			LocalPrincipals: amqpLocalPrincipalStats{
				ActiveConnections: ast.GetLocalConnections(),
				PublishTimeouts:   ast.GetLocalPublishTimeouts(),
				PublishRejections: ast.GetLocalPublishRejections(),
				Authentication: amqpLocalAuthenticationStats{
					Success: ast.GetLocalAuthSuccess(),
					Failure: ast.GetLocalAuthFailures(),
				},
				Authorization: amqpLocalAuthorizationStats{
					PublishDenied:   ast.GetLocalPublishDenials(),
					SubscribeDenied: ast.GetLocalSubscribeDenials(),
					OperationDenied: ast.GetLocalOperationDenials(),
				},
				Reloads: amqpLocalReloadStats{
					Success:           ast.GetLocalReloadSuccess(),
					Failure:           ast.GetLocalReloadFailures(),
					ForcedDisconnects: ast.GetLocalForcedDisconnects(),
				},
			},
		}
	}

	if s.queueManager != nil {
		qm := s.queueManager.GetMetrics()
		resp.Queues = &queueStats{
			CaptureFailures: qm.CaptureFailures,
			CaptureDropped:  qm.CaptureDropped,
			Claims: queueAttemptStats{
				Attempts:  qm.ClaimAttempts,
				Successes: qm.ClaimSuccesses,
				Failures:  qm.ClaimFailures,
			},
			Steals: queueAttemptStats{
				Attempts:  qm.StealAttempts,
				Successes: qm.StealSuccesses,
				Failures:  qm.StealFailures,
			},
			Acknowledgments: queueAckStats{
				Ack:                 qm.AckCount,
				Nack:                qm.NackCount,
				Reject:              qm.RejectCount,
				DLQ:                 qm.DLQCount,
				DLQTransferFailures: qm.DLQTransferFailures,
				PoisonWithoutDLQ:    qm.PoisonWithoutDLQ,

				PoisonPending:              qm.PoisonPending,
				PoisonPendingNoDestination: qm.PoisonPendingNoDestination,
			},
			Pending: queuePendingStats{
				Current:   qm.PELSize,
				HighWater: qm.PELHighWater,
			},
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
