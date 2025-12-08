package adapter

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/dborovcanin/mqtt/broker"
	packets "github.com/dborovcanin/mqtt/packets"
	v5 "github.com/dborovcanin/mqtt/packets/v5"
)

// HTTPAdapter listens for HTTP POST requests and converts them to MQTT messages.
type HTTPAdapter struct {
	addr    string
	Handler broker.ConnectionHandler
	server  *http.Server
}

func NewHTTPAdapter(addr string) *HTTPAdapter {
	return &HTTPAdapter{addr: addr}
}

func (h *HTTPAdapter) Serve(handler broker.ConnectionHandler) error {
	h.Handler = handler
	mux := http.NewServeMux()
	mux.HandleFunc("/publish", h.handlePublish)

	h.server = &http.Server{
		Addr:    h.addr,
		Handler: mux,
	}

	return h.server.ListenAndServe()
}

func (h *HTTPAdapter) Close() error {
	if h.server != nil {
		return h.server.Close()
	}
	return nil
}

func (h *HTTPAdapter) Addr() net.Addr {
	// dummy
	return &net.TCPAddr{}
}

func (h *HTTPAdapter) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "Missing topic query parameter", http.StatusBadRequest)
		return
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// Create a virtual connection
	vc := NewVirtualConnection()

	// Spin up the handler (Server) in a goroutine
	go h.Handler.HandleConnection(vc)

	// Simulate Client Flow
	// 1. CONNECT
	connectPkt := &v5.Connect{
		FixedHeader: packets.FixedHeader{PacketType: packets.ConnectType},
		ClientID:    "http-" + fmt.Sprintf("%d", time.Now().UnixNano()),
		CleanStart:  true,
		KeepAlive:   60,
	}
	vc.Inject(connectPkt)

	// 2. Expect CONNACK
	_, err = vc.Capture(5 * time.Second)
	if err != nil {
		http.Error(w, "Broker handshake failed", http.StatusBadGateway)
		vc.Close()
		return
	}

	// 3. PUBLISH
	pubPkt := &v5.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
		TopicName:   topic,
		Payload:     payload,
	}
	vc.Inject(pubPkt)

	// 4. DISCONNECT
	discPkt := &v5.Disconnect{
		FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
	}
	vc.Inject(discPkt)

	// 5. Close
	// Give some time for packet to be processed?
	// In QoS 0, there is no ack.
	// Broker processes HandleConnection loop.
	// Ideally we wait for broker to process?
	// We can rely on channel buffer?

	// For now simple close.
	time.Sleep(10 * time.Millisecond)
	vc.Close()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Published"))
}
