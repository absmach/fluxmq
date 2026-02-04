// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package codec

import "fmt"

// AMQP Error Codes
const (
	ContentTooLarge    = 311
	NoRoute            = 312
	NoConsumers        = 313
	ConnectionForced   = 320
	InvalidPath        = 402
	AccessRefused      = 403
	NotFound           = 404
	ResourceLocked     = 405
	PreconditionFailed = 406
	FrameError         = 501
	SyntaxError        = 502
	CommandInvalid     = 503
	ChannelError       = 504
	UnexpectedFrame    = 505
	ResourceError      = 506
	NotAllowed         = 530
	NotImplemented     = 540
	InternalError      = 541
)

// Error represents an AMQP error.
type Error struct {
	Code    int
	Message string
	Err     error
}

// NewErr creates a new AMQP error.
func NewErr(code int, message string, err error) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

// Error returns the string representation of the error.
func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("amqp: %d: %s: %s", e.Code, e.Message, e.Err.Error())
	}
	return fmt.Sprintf("amqp: %d: %s", e.Code, e.Message)
}
