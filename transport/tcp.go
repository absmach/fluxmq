package transport

import (
	"net"

	"github.com/dborovcanin/mqtt/broker"
)

// TCPFrontend implements the Frontend interface for TCP connections.
type TCPFrontend struct {
	listener net.Listener
}

// NewTCPFrontend creates a new TCPFrontend listening on the specified address.
func NewTCPFrontend(address string) (*TCPFrontend, error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return &TCPFrontend{listener: ln}, nil
}

// Serve accepts new TCP connections and passes them to the handler.
func (f *TCPFrontend) Serve(handler broker.ConnectionHandler) error {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			// Check if closed
			return err
		}
		// Wrap net.Conn into broker.StreamConnection
		streamConn := broker.NewStreamConnection(conn)
		go handler.HandleConnection(streamConn)
	}
}

// Close stops the listener.
func (f *TCPFrontend) Close() error {
	return f.listener.Close()
}

// Addr returns the listener's network address.
func (f *TCPFrontend) Addr() net.Addr {
	return f.listener.Addr()
}
