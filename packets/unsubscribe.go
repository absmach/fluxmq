package packets

import (
	"bytes"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

// Unsubscribe is an internal representation of the fields of the UNSUBSCRIBE MQTT packet.
type Unsubscribe struct {
	FixedHeader
	ID     uint16
	Topics []string
}

func (pkt *Unsubscribe) String() string {
	return fmt.Sprintf("%s\npacket_id: %d\n", pkt.FixedHeader, pkt.ID)
}

func (pkt *Unsubscribe) Write(w io.Writer) error {
	var body bytes.Buffer
	var err error
	body.Write(codec.EncodeUint16(pkt.ID))
	for _, topic := range pkt.Topics {
		body.Write(codec.EncodeString(topic))
	}
	pkt.FixedHeader.RemainingLength = body.Len()
	packet := pkt.FixedHeader.pack()
	packet.Write(body.Bytes())
	_, err = packet.WriteTo(w)

	return err
}

// Unpack decodes the details of a ControlPacket after the fixed
// header has been read
func (pkt *Unsubscribe) Unpack(b io.Reader) error {
	var err error
	pkt.ID, err = codec.DecodeUint16(b)
	if err != nil {
		return err
	}

	for topic, err := codec.DecodeString(b); err == nil && topic != ""; topic, err = codec.DecodeString(b) {
		pkt.Topics = append(pkt.Topics, topic)
	}

	return err
}

// Details returns a struct containing the Qos and packet_id of this control packet.
func (pkt *Unsubscribe) Details() Details {
	return Details{Qos: 1, ID: pkt.ID}
}
