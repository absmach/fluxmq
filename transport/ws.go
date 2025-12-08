package transport

import (
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/dborovcanin/mqtt/broker"
	packets "github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	v5 "github.com/dborovcanin/mqtt/packets/v5"
	"golang.org/x/net/websocket"
)

// WSFrontend implements Frontend for WebSockets.
type WSFrontend struct {
	addr    string
	server  *http.Server
	handler broker.ConnectionHandler
}

// NewWSFrontend creates a new WSFrontend.
func NewWSFrontend(addr string) *WSFrontend {
	return &WSFrontend{addr: addr}
}

// Serve starts the HTTP server.
func (f *WSFrontend) Serve(handler broker.ConnectionHandler) error {
	f.handler = handler
	mux := http.NewServeMux()
	// MQTT over Websocket usually uses /mqtt path
	mux.Handle("/mqtt", websocket.Handler(f.handleWS))

	f.server = &http.Server{
		Addr:    f.addr,
		Handler: mux,
	}
	return f.server.ListenAndServe()
}

func (f *WSFrontend) handleWS(ws *websocket.Conn) {
	// Set binary mode
	ws.PayloadType = websocket.BinaryFrame

	// Wrap connection
	conn := &WSConnection{
		Conn:   ws,
		reader: ws,
	}

	// Pass to handler
	// Note: this handler blocks until connection closes.
	// broker.Server.HandleConnection handles the reading loop.
	f.handler.HandleConnection(conn)
}

// Close stops the server.
func (f *WSFrontend) Close() error {
	if f.server != nil {
		return f.server.Close()
	}
	return nil
}

// Addr returns the listener address.
func (f *WSFrontend) Addr() net.Addr {
	// This is tricky because http.Server doesn't easily expose the listener if ListenAndServe is used.
	// For now return dummy or try to capture listener.
	return &net.TCPAddr{}
}

// WSConnection wraps websocket.Conn
type WSConnection struct {
	*websocket.Conn
	reader  io.Reader
	version int
}

func (c *WSConnection) ReadPacket() (packets.ControlPacket, error) {
	if c.version == 0 {
		ver, restored, err := packets.DetectProtocolVersion(c.reader)
		if err != nil {
			return nil, err
		}
		c.version = ver
		c.reader = restored
	}

	if c.version == 5 {
		pkt, _, _, err := v5.ReadPacket(c.reader)
		return pkt, err
	} else if c.version == 4 || c.version == 3 {
		pkt, _, _, err := v3.ReadPacket(c.reader)
		return pkt, err
	}

	return nil, errors.New("unsupported protocol version")
}

func (c *WSConnection) WritePacket(p packets.ControlPacket) error {
	// We wrap Encode() result into a Write.
	// Pack() writes to io.Writer. websocket.Conn is io.Writer.
	return p.Pack(c.Conn)
}

func (c *WSConnection) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

func (c *WSConnection) Close() error {
	return c.Conn.Close()
}
