// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/absmach/fluxmq/amqp1/message"
	"github.com/absmach/fluxmq/queue/types"
)

// Management status codes.
const (
	statusOK       = int32(200)
	statusCreated  = int32(201)
	statusNotFound = int32(404)
	statusConflict = int32(409)
	statusError    = int32(500)
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
		return h.errorResponse(msg, statusError, "missing application-properties")
	}

	operation, _ := msg.ApplicationProperties["operation"].(string)
	entityType, _ := msg.ApplicationProperties["type"].(string)
	name, _ := msg.ApplicationProperties["name"].(string)

	if entityType != "queue" {
		return h.errorResponse(msg, statusError, fmt.Sprintf("unsupported type: %s", entityType))
	}

	ctx := context.Background()

	switch operation {
	case "CREATE":
		return h.handleCreate(ctx, msg, name)
	case "DELETE":
		return h.handleDelete(ctx, msg, name)
	case "READ":
		return h.handleRead(ctx, msg, name)
	case "QUERY":
		return h.handleQuery(ctx, msg)
	default:
		return h.errorResponse(msg, statusError, fmt.Sprintf("unsupported operation: %s", operation))
	}
}

func (h *managementHandler) handleCreate(ctx context.Context, req *message.Message, name string) *message.Message {
	if name == "" {
		return h.errorResponse(req, statusError, "name is required for CREATE")
	}

	qm := h.broker.getQueueManager()
	if qm == nil {
		return h.errorResponse(req, statusError, "queue manager not available")
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
		// Check if already exists
		if err.Error() == "queue already exists" {
			return h.statusResponse(req, statusConflict, "queue already exists")
		}
		return h.errorResponse(req, statusError, err.Error())
	}

	h.logger.Info("queue created via management", slog.String("queue", name))
	return h.statusResponse(req, statusCreated, "queue created")
}

func (h *managementHandler) handleDelete(ctx context.Context, req *message.Message, name string) *message.Message {
	if name == "" {
		return h.errorResponse(req, statusError, "name is required for DELETE")
	}

	qm := h.broker.getQueueManager()
	if qm == nil {
		return h.errorResponse(req, statusError, "queue manager not available")
	}

	// Check if queue exists
	cfg, err := qm.GetQueue(ctx, name)
	if err != nil {
		return h.statusResponse(req, statusNotFound, "queue not found")
	}

	if cfg.Reserved {
		return h.errorResponse(req, statusError, "cannot delete reserved queue")
	}

	if err := qm.DeleteQueue(ctx, name); err != nil {
		return h.errorResponse(req, statusError, err.Error())
	}

	h.logger.Info("queue deleted via management", slog.String("queue", name))
	return h.statusResponse(req, statusOK, "queue deleted")
}

func (h *managementHandler) handleRead(ctx context.Context, req *message.Message, name string) *message.Message {
	if name == "" {
		return h.errorResponse(req, statusError, "name is required for READ")
	}

	qm := h.broker.getQueueManager()
	if qm == nil {
		return h.errorResponse(req, statusError, "queue manager not available")
	}

	cfg, err := qm.GetQueue(ctx, name)
	if err != nil {
		return h.statusResponse(req, statusNotFound, "queue not found")
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
	qm := h.broker.getQueueManager()
	if qm == nil {
		return h.errorResponse(req, statusError, "queue manager not available")
	}

	queues, err := qm.ListQueues(ctx)
	if err != nil {
		return h.errorResponse(req, statusError, err.Error())
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

func (h *managementHandler) errorResponse(req *message.Message, code int32, description string) *message.Message {
	return h.statusResponse(req, code, description)
}
