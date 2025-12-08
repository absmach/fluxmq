package adapter

import (
	"errors"
	"io"
	"net"
	"time"

	packets "github.com/dborovcanin/mqtt/packets"
)

// VirtualConnection simulates a network connection for the broker.
// It allows injecting packets (ReadPacket) and capturing responses (WritePacket).
type VirtualConnection struct {
	inbound  chan packets.ControlPacket // Packets to be read BY the broker
	outbound chan packets.ControlPacket // Packets written BY the broker
	closed   chan struct{}
}

func NewVirtualConnection() *VirtualConnection {
	return &VirtualConnection{
		inbound:  make(chan packets.ControlPacket, 10),
		outbound: make(chan packets.ControlPacket, 10),
		closed:   make(chan struct{}),
	}
}

// ReadPacket pulls from the inbound buffer (Broker reading from client)
func (vc *VirtualConnection) ReadPacket() (packets.ControlPacket, error) {
	select {
	case p, ok := <-vc.inbound:
		if !ok {
			return nil, io.EOF
		}
		return p, nil
	case <-vc.closed:
		return nil, io.EOF
	}
}

// WritePacket pushes to the outbound buffer (Broker writing to client)
func (vc *VirtualConnection) WritePacket(p packets.ControlPacket) error {
	select {
	case vc.outbound <- p:
		return nil
	case <-vc.closed:
		return io.ErrClosedPipe
	}
}

func (vc *VirtualConnection) Close() error {
	select {
	case <-vc.closed:
		// already closed
	default:
		close(vc.closed)
	}
	return nil
}

func (vc *VirtualConnection) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0}
}

// Inject pushes a packet for the broker to read.
func (vc *VirtualConnection) Inject(p packets.ControlPacket) error {
	select {
	case vc.inbound <- p:
		return nil
	case <-vc.closed:
		return io.ErrClosedPipe
	}
}

// Capture waits for a packet from the broker.
func (vc *VirtualConnection) Capture(timeout time.Duration) (packets.ControlPacket, error) {
	select {
	case p := <-vc.outbound:
		return p, nil
	case <-time.After(timeout):
		return nil, errors.New("timeout capturing packet")
	case <-vc.closed:
		return nil, io.EOF
	}
}
