// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"time"

	"github.com/absmach/fluxmq/message"
)

// PublishRequest encapsulates publish data for queue routing.
type PublishRequest struct {
	Source              message.SourceMetadata
	Trace               message.TraceMetadata
	Topic               string
	Payload             []byte
	Key                 []byte
	Headers             map[string][]byte
	Properties          map[string]string
	ContentType         string
	ContentEncoding     string
	ResponseTopic       string
	CorrelationData     []byte
	PayloadFormat       *byte
	MessageExpiry       *uint32
	PublishedAt         time.Time
	ExpiresAt           time.Time
	ForwardTargetQueues []string
}

// PublishMode controls how the queue manager should handle a publish.
type PublishMode int

const (
	PublishNormal PublishMode = iota
	PublishLocal
	PublishForwarded
)
