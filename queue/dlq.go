// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	queueStorage "github.com/absmach/mqtt/queue/storage"
)

// DLQManager manages dead letter queue operations.
type DLQManager struct {
	messageStore queueStorage.MessageStore
	alertHandler AlertHandler
}

// NewDLQManager creates a new DLQ manager.
func NewDLQManager(messageStore queueStorage.MessageStore, alertHandler AlertHandler) *DLQManager {
	return &DLQManager{
		messageStore: messageStore,
		alertHandler: alertHandler,
	}
}

// MoveToDLQ moves a failed message to the dead letter queue.
func (d *DLQManager) MoveToDLQ(ctx context.Context, queue *Queue, msg *queueStorage.Message, reason string) error {
	config := queue.Config()

	// Only move to DLQ if enabled
	if !config.DLQConfig.Enabled {
		// Just delete the message
		return d.messageStore.DeleteMessage(ctx, queue.Name(), msg.ID)
	}

	// Update message metadata
	msg.State = queueStorage.StateDLQ
	msg.FailureReason = reason
	msg.MovedToDLQAt = time.Now()

	// Determine DLQ topic
	dlqTopic := config.DLQConfig.Topic
	if dlqTopic == "" {
		dlqTopic = "$queue/dlq/" + strings.TrimPrefix(queue.Name(), "$queue/")
	}

	// Store in DLQ storage
	if err := d.messageStore.EnqueueDLQ(ctx, dlqTopic, msg); err != nil {
		return fmt.Errorf("failed to enqueue to DLQ: %w", err)
	}

	// Delete from original queue
	if err := d.messageStore.DeleteMessage(ctx, queue.Name(), msg.ID); err != nil {
		// Message is in DLQ but also still in original queue - log but don't fail
		return nil
	}

	// Trigger alert if webhook configured
	if config.DLQConfig.AlertWebhook != "" && d.alertHandler != nil {
		alert := &DLQAlert{
			QueueName:     queue.Name(),
			DLQTopic:      dlqTopic,
			MessageID:     msg.ID,
			FailureReason: reason,
			RetryCount:    msg.RetryCount,
			FirstAttempt:  msg.CreatedAt,
			MovedToDLQAt:  msg.MovedToDLQAt,
			TotalDuration: msg.MovedToDLQAt.Sub(msg.CreatedAt),
		}

		// Send alert asynchronously to not block message processing
		go func() {
			if err := d.alertHandler.Send(config.DLQConfig.AlertWebhook, alert); err != nil {
				// Alert failed, but message is in DLQ - log but continue
			}
		}()
	}

	return nil
}

// RetryFromDLQ moves a message from DLQ back to the original queue.
func (d *DLQManager) RetryFromDLQ(ctx context.Context, queueName, messageID string) error {
	// Get DLQ topic name
	dlqTopic := "$queue/dlq/" + strings.TrimPrefix(queueName, "$queue/")

	// Get message from DLQ
	dlqMessages, err := d.messageStore.ListDLQ(ctx, dlqTopic, 0)
	if err != nil {
		return fmt.Errorf("failed to list DLQ messages: %w", err)
	}

	var msg *queueStorage.Message
	for _, m := range dlqMessages {
		if m.ID == messageID {
			msg = m
			break
		}
	}

	if msg == nil {
		return queueStorage.ErrMessageNotFound
	}

	// Reset message state for retry
	msg.State = queueStorage.StateQueued
	msg.RetryCount = 0
	msg.NextRetryAt = time.Time{}
	msg.FailureReason = ""

	// Re-enqueue to original queue
	if err := d.messageStore.Enqueue(ctx, queueName, msg); err != nil {
		return fmt.Errorf("failed to re-enqueue message: %w", err)
	}

	// Delete from DLQ
	if err := d.messageStore.DeleteDLQMessage(ctx, dlqTopic, messageID); err != nil {
		// Message is back in queue but also still in DLQ - acceptable
		return nil
	}

	return nil
}

// PurgeDLQ removes all messages from a DLQ.
func (d *DLQManager) PurgeDLQ(ctx context.Context, queueName string) (int, error) {
	dlqTopic := "$queue/dlq/" + strings.TrimPrefix(queueName, "$queue/")

	messages, err := d.messageStore.ListDLQ(ctx, dlqTopic, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to list DLQ messages: %w", err)
	}

	count := 0
	for _, msg := range messages {
		if err := d.messageStore.DeleteDLQMessage(ctx, dlqTopic, msg.ID); err != nil {
			continue
		}
		count++
	}

	return count, nil
}

// GetDLQStats returns statistics about the DLQ.
func (d *DLQManager) GetDLQStats(ctx context.Context, queueName string) (*DLQStats, error) {
	dlqTopic := "$queue/dlq/" + strings.TrimPrefix(queueName, "$queue/")

	messages, err := d.messageStore.ListDLQ(ctx, dlqTopic, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to list DLQ messages: %w", err)
	}

	stats := &DLQStats{
		QueueName:     queueName,
		DLQTopic:      dlqTopic,
		TotalMessages: int64(len(messages)),
		ByReason:      make(map[string]int64),
	}

	for _, msg := range messages {
		reason := msg.FailureReason
		if reason == "" {
			reason = "unknown"
		}
		stats.ByReason[reason]++
	}

	return stats, nil
}

// DLQStats holds statistics about a dead letter queue.
type DLQStats struct {
	QueueName     string
	DLQTopic      string
	TotalMessages int64
	ByReason      map[string]int64
}

// DLQAlert represents an alert sent when a message is moved to DLQ.
type DLQAlert struct {
	QueueName     string        `json:"queue_name"`
	DLQTopic      string        `json:"dlq_topic"`
	MessageID     string        `json:"message_id"`
	FailureReason string        `json:"failure_reason"`
	RetryCount    int           `json:"retry_count"`
	FirstAttempt  time.Time     `json:"first_attempt"`
	MovedToDLQAt  time.Time     `json:"moved_to_dlq_at"`
	TotalDuration time.Duration `json:"total_duration"`
}

// AlertHandler defines the interface for sending DLQ alerts.
type AlertHandler interface {
	Send(webhookURL string, alert *DLQAlert) error
}

// HTTPAlertHandler sends alerts via HTTP webhooks.
type HTTPAlertHandler struct {
	client  *http.Client
	timeout time.Duration
}

// NewHTTPAlertHandler creates a new HTTP alert handler.
func NewHTTPAlertHandler(timeout time.Duration) *HTTPAlertHandler {
	return &HTTPAlertHandler{
		client: &http.Client{
			Timeout: timeout,
		},
		timeout: timeout,
	}
}

// Send sends an alert to the configured webhook URL.
func (h *HTTPAlertHandler) Send(webhookURL string, alert *DLQAlert) error {
	payload, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("failed to marshal alert: %w", err)
	}

	resp, err := h.client.Post(webhookURL, "application/json", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to send webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned error status: %d", resp.StatusCode)
	}

	return nil
}

// NoOpAlertHandler is a no-op implementation for testing.
type NoOpAlertHandler struct{}

// Send does nothing.
func (n *NoOpAlertHandler) Send(webhookURL string, alert *DLQAlert) error {
	return nil
}
