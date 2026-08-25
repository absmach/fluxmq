// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/absmach/fluxmq/amqp1/message"
	queuepkg "github.com/absmach/fluxmq/queue"
	"github.com/absmach/fluxmq/queue/types"
)

// Management status codes.
const (
	statusOK      = int32(200)
	statusCreated = int32(201)
)

// Management entity type and operation names.
const (
	entityTypeQueue = "queue"
	opCreate        = "CREATE"
)

// managementHandler handles AMQP management node requests.
type managementHandler struct {
	broker *Broker
	logger *slog.Logger
}

func newManagementHandler(b *Broker) *managementHandler {
	return &managementHandler{
		broker: b,
		logger: b.logger,
	}
}

// handleRequest processes a management request message and returns a response.
func (h *managementHandler) handleRequest(msg *message.Message) *message.Message {
	if msg.ApplicationProperties == nil {
		return h.queueErrorResponse(msg, managementFailure(queuepkg.ErrorCodeInvalidArgument, false, "missing application-properties"))
	}

	operation, _ := msg.ApplicationProperties["operation"].(string)
	entityType, _ := msg.ApplicationProperties["type"].(string)
	name, _ := msg.ApplicationProperties["name"].(string)

	if entityType != entityTypeQueue {
		return h.queueErrorResponse(msg, managementFailure(queuepkg.ErrorCodeInvalidArgument, false, "unsupported type"))
	}

	ctx := context.Background()

	switch operation {
	case opCreate:
		return h.handleCreate(ctx, msg, name)
	case "DELETE":
		return h.handleDelete(ctx, msg, name)
	case "READ":
		return h.handleRead(ctx, msg, name)
	case "QUERY":
		return h.handleQuery(ctx, msg)
	default:
		return h.queueErrorResponse(msg, managementFailure(queuepkg.ErrorCodeInvalidArgument, false, "unsupported operation"))
	}
}

func (h *managementHandler) handleCreate(ctx context.Context, req *message.Message, name string) *message.Message {
	if name == "" {
		return h.queueErrorResponse(req, managementFailure(queuepkg.ErrorCodeInvalidArgument, false, "name is required for CREATE"))
	}

	qm := h.broker.queueAdminManager
	if qm == nil {
		return h.queueErrorResponse(req, managementFailure(queuepkg.ErrorCodeUnavailable, true, "queue manager not available"))
	}

	// Build queue config from request properties
	topicPatterns := []string{"$queue/" + name + "/#"}

	// Allow custom topics from the request body/properties
	if topics, ok := req.ApplicationProperties["topics"]; ok {
		if topicStr, ok := topics.(string); ok {
			topicPatterns = append(topicPatterns, topicStr)
		}
	}

	cfg := types.DefaultQueueConfig(name, topicPatterns...)

	if err := qm.CreateQueue(ctx, cfg); err != nil {
		return h.queueErrorResponse(req, err)
	}

	h.logger.Info("queue created via management", slog.String(entityTypeQueue, name))
	return h.statusResponse(req, statusCreated, "queue created")
}

func (h *managementHandler) handleDelete(ctx context.Context, req *message.Message, name string) *message.Message {
	if name == "" {
		return h.queueErrorResponse(req, managementFailure(queuepkg.ErrorCodeInvalidArgument, false, "name is required for DELETE"))
	}

	qm := h.broker.queueAdminManager
	if qm == nil {
		return h.queueErrorResponse(req, managementFailure(queuepkg.ErrorCodeUnavailable, true, "queue manager not available"))
	}

	// Check if queue exists
	cfg, err := qm.GetQueue(ctx, name)
	if err != nil {
		return h.queueErrorResponse(req, err)
	}

	if cfg.Reserved {
		return h.queueErrorResponse(req, managementFailure(queuepkg.ErrorCodeFailedPrecondition, false, "cannot delete reserved queue"))
	}

	if err := qm.DeleteQueue(ctx, name); err != nil {
		return h.queueErrorResponse(req, err)
	}

	h.logger.Info("queue deleted via management", slog.String(entityTypeQueue, name))
	return h.statusResponse(req, statusOK, "queue deleted")
}

func (h *managementHandler) handleRead(ctx context.Context, req *message.Message, name string) *message.Message {
	if name == "" {
		return h.queueErrorResponse(req, managementFailure(queuepkg.ErrorCodeInvalidArgument, false, "name is required for READ"))
	}

	qm := h.broker.queueAdminManager
	if qm == nil {
		return h.queueErrorResponse(req, managementFailure(queuepkg.ErrorCodeUnavailable, true, "queue manager not available"))
	}

	cfg, err := qm.GetQueue(ctx, name)
	if err != nil {
		return h.queueErrorResponse(req, err)
	}

	resp := h.statusResponse(req, statusOK, "OK")
	resp.ApplicationProperties["name"] = cfg.Name
	resp.ApplicationProperties["reserved"] = cfg.Reserved
	// Encode topics as comma-separated for simplicity
	if len(cfg.Topics) > 0 {
		resp.ApplicationProperties["topics"] = cfg.Topics[0]
		for i := 1; i < len(cfg.Topics); i++ {
			resp.ApplicationProperties["topics"] = resp.ApplicationProperties["topics"].(string) + "," + cfg.Topics[i]
		}
	}

	return resp
}

func (h *managementHandler) handleQuery(ctx context.Context, req *message.Message) *message.Message {
	qm := h.broker.queueAdminManager
	if qm == nil {
		return h.queueErrorResponse(req, managementFailure(queuepkg.ErrorCodeUnavailable, true, "queue manager not available"))
	}

	queues, err := qm.ListQueues(ctx)
	if err != nil {
		return h.queueErrorResponse(req, err)
	}

	resp := h.statusResponse(req, statusOK, "OK")

	// Encode queue names as comma-separated string in a property
	names := ""
	for i, q := range queues {
		if i > 0 {
			names += ","
		}
		names += q.Name
	}
	resp.ApplicationProperties["queues"] = names
	resp.ApplicationProperties["count"] = fmt.Sprintf("%d", len(queues))

	return resp
}

func (h *managementHandler) statusResponse(req *message.Message, code int32, description string) *message.Message {
	resp := &message.Message{
		Properties: &message.Properties{},
		ApplicationProperties: map[string]any{
			"statusCode":        code,
			"statusDescription": description,
		},
	}

	// Set correlation-id from request's message-id for request/response matching
	if req.Properties != nil {
		resp.Properties.CorrelationID = req.Properties.MessageID
		if req.Properties.ReplyTo != "" {
			resp.Properties.To = req.Properties.ReplyTo
		}
	}

	return resp
}

func (h *managementHandler) queueErrorResponse(req *message.Message, err error) *message.Message {
	failure := queuepkg.ClassifyError(err)
	resp := h.statusResponse(req, amqp1ManagementStatus(failure.Code), "queue operation failed")
	// Same external vocabulary as the rejected-delivery outcome, carried as
	// management application-properties. Pinned by TestAMQP1QueueVocabularyIsStable.
	resp.ApplicationProperties[amqp1ManagementErrorCodeKey] = failure.Code.String()
	resp.ApplicationProperties[amqp1ManagementRetryableKey] = failure.Retryable
	resp.ApplicationProperties[amqp1ManagementOwnershipKey] = failure.Ownership.String()
	resp.ApplicationProperties[amqp1ManagementLeaderKey] = failure.Leader.String()
	resp.ApplicationProperties[amqp1ManagementDurabilityKey] = failure.Durability.String()
	return resp
}

func managementFailure(code queuepkg.ErrorCode, retryable bool, message string) error {
	return queuepkg.WithFailure(fmt.Errorf("%s", message), queuepkg.Failure{Code: code, Retryable: retryable})
}
