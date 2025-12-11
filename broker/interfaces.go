package broker

// import (
// 	"net"
// 	"time"

// 	packets "github.com/dborovcanin/mqtt/core/packets"
// )

// // Connection abstracts a single client connection.
// // It allows reading/writing MQTT packets regardless of the transport (TCP, WS, etc).
// // This is used internally by the broker for session management.
// type Connection interface {
// 	// ReadPacket reads the next MQTT packet from the connection.
// 	// It returns the packet or an error.
// 	ReadPacket() (packets.ControlPacket, error)

// 	// WritePacket writes an MQTT packet to the connection.
// 	WritePacket(p packets.ControlPacket) error

// 	// Close terminates the connection.
// 	Close() error

// 	// RemoteAddr returns the address of the connected client.
// 	RemoteAddr() net.Addr

// 	// SetReadDeadline sets the connection read deadline.
// 	SetReadDeadline(t time.Time) error

// 	// SetWriteDeadline sets the connection write deadline.
// 	SetWriteDeadline(t time.Time) error
// }
