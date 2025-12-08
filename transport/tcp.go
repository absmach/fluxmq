package transport

import (
	"errors"
	"io"
	"net"

	"github.com/dborovcanin/mqtt/broker"
	packets "github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	v5 "github.com/dborovcanin/mqtt/packets/v5"
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
	// version stores the MQTT version (3 or 5) once negotiated/detected.
	version int
}

// ReadPacket reads a packet from the connection.
// It uses the sniffer for the first packet if version is unknown.
func (c *TCPConnection) ReadPacket() (packets.ControlPacket, error) {
	if c.version == 0 {
		// Detect protocol version from the first packet (CONNECT)
		ver, restored, err := packets.DetectProtocolVersion(c.reader)
		if err != nil {
			return nil, err
		}
		c.version = ver
		c.reader = restored
	}

	// Dispatch based on version
	if c.version == 5 {
		pkt, _, _, err := v5.ReadPacket(c.reader)
		return pkt, err
	} else if c.version == 4 || c.version == 3 {
		pkt, _, _, err := v3.ReadPacket(c.reader)
		return pkt, err
	}

	return nil, errors.New("unsupported protocol version")
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
