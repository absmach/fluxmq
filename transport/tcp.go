package transport

import (
	"io"
	"net"

	"github.com/dborovcanin/mqtt/broker"
	"github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
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
		// Wrap net.Conn into our Connection interface
		// We use packets.DetectProtocolVersion logic inside the connection wrapper?
		// No, Connection wrapper just reads/writes packets.
		// The sniffing might happen inside ReadPacket or initial handshake.
		// Let's create a connection wrapper that handles protocol versioning.
		tcpConn := &TCPConnection{
			Conn:   conn,
			reader: conn,
		}
		go handler.HandleConnection(tcpConn)
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

// TCPConnection wraps a net.Conn to implement the Connection interface.
type TCPConnection struct {
	net.Conn
	reader io.Reader
}

// ReadPacket reads a packet from the connection.
func (c *TCPConnection) ReadPacket() (packets.ControlPacket, error) {
	pkt, _, _, err := v3.ReadPacket(c.reader)
	return pkt, err
}

// WritePacket writes an MQTT packet to the connection.
func (c *TCPConnection) WritePacket(p packets.ControlPacket) error {
	return p.Pack(c.Conn)
}

// RemoteAddr returns the remote address.
func (c *TCPConnection) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

// Close closes the connection.
func (c *TCPConnection) Close() error {
	return c.Conn.Close()
}
