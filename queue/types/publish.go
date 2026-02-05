// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

// PublishRequest encapsulates publish data for queue routing.
type PublishRequest struct {
	Topic      string
	Payload    []byte
	Properties map[string]string
}

// PublishMode controls how the queue manager should handle a publish.
type PublishMode int

const (
	PublishNormal PublishMode = iota
	PublishLocal
	PublishForwarded
)
