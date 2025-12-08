package v3

import (
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/packets"
)

// Disconnect represents the MQTT V3.1.1 DISCONNECT packet.
type Disconnect struct {
	packets.FixedHeader
}

func (d *Disconnect) String() string {
	return fmt.Sprintf("%s\n", d.FixedHeader)
}

func (d *Disconnect) Type() byte {
	return packets.DisconnectType
}

func (d *Disconnect) Encode() []byte {
	d.FixedHeader.RemainingLength = 0
	return d.FixedHeader.Encode()
}

func (d *Disconnect) Unpack(r io.Reader) error {
	return nil
}

func (d *Disconnect) Pack(w io.Writer) error {
	_, err := w.Write(d.Encode())
	return err
}

func (d *Disconnect) Details() packets.Details {
	return packets.Details{Type: packets.DisconnectType}
}
