package messages

import "errors"

var (
	ErrInflightFull   = errors.New("inflight queue full")
	ErrQueueFull      = errors.New("offline queue full")
	ErrPacketNotFound = errors.New("packet not found in inflight")
)
