package packets

import (
	"io"
)

// PingReq is an internal representation of the fields of the PINGREQ MQTT packet
type PingReq struct {
	FixedHeader
}

func (pkt *PingReq) String() string {
	return pkt.FixedHeader.String()
}

func (pkt *PingReq) Pack(w io.Writer) error {
	bytes := pkt.FixedHeader.Encode()
	_, err := w.Write(bytes)

	return err
}

func (pkt *PingReq) Unpack(r io.Reader, v byte) error {
	return nil
}

func (pkt *PingReq) Details() Details {
	return Details{Type: PingReqType, ID: 0, Qos: 0}
}
