package core

// State represents the session state.
type State int

const (
	StateNew State = iota
	StateConnecting
	StateConnected
	StateDisconnecting
	StateDisconnected
)

func (s State) String() string {
	switch s {
	case StateNew:
		return "new"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateDisconnecting:
		return "disconnecting"
	case StateDisconnected:
		return "disconnected"
	default:
		return "unknown"
	}
}
