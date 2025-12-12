package messages

import "errors"

var (
	// ErrNotConnected     = errors.New("session not connected")
	// ErrAlreadyConnected = errors.New("session already connected")
	// ErrSessionExpired   = errors.New("session expired")
	// ErrSessionTakeover  = errors.New("session taken over by another connection")
	ErrInflightFull   = errors.New("inflight queue full")
	ErrQueueFull      = errors.New("offline queue full")
	ErrPacketNotFound = errors.New("packet not found in inflight")
)
