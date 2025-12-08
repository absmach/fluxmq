// Package pool provides sync.Pool-based allocators for MQTT packets.
// Use this package in high-throughput scenarios (brokers) to reduce GC pressure.
//
// Usage:
//
//	pkt := pool.AcquirePublish()
//	defer pool.ReleasePublish(pkt)
//	// use pkt...
//
// Important: Never use a packet after releasing it back to the pool.
package pool

import (
	"sync"

	"github.com/dborovcanin/mqtt/packets"
)

var (
	connectPool = sync.Pool{
		New: func() any {
			return &packets.Connect{
				FixedHeader: packets.FixedHeader{PacketType: packets.ConnectType},
			}
		},
	}

	connAckPool = sync.Pool{
		New: func() any {
			return &packets.ConnAck{
				FixedHeader: packets.FixedHeader{PacketType: packets.ConnAckType},
			}
		},
	}

	publishPool = sync.Pool{
		New: func() any {
			return &packets.Publish{
				FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
			}
		},
	}

	pubAckPool = sync.Pool{
		New: func() any {
			return &packets.PubAck{
				FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
			}
		},
	}

	pubRecPool = sync.Pool{
		New: func() any {
			return &packets.PubRec{
				FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
			}
		},
	}

	pubRelPool = sync.Pool{
		New: func() any {
			return &packets.PubRel{
				FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
			}
		},
	}

	pubCompPool = sync.Pool{
		New: func() any {
			return &packets.PubComp{
				FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
			}
		},
	}

	subscribePool = sync.Pool{
		New: func() any {
			return &packets.Subscribe{
				FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
			}
		},
	}

	subAckPool = sync.Pool{
		New: func() any {
			return &packets.SubAck{
				FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
			}
		},
	}

	unsubscribePool = sync.Pool{
		New: func() any {
			return &packets.Unsubscribe{
				FixedHeader: packets.FixedHeader{PacketType: packets.UnsubscribeType, QoS: 1},
			}
		},
	}

	unsubAckPool = sync.Pool{
		New: func() any {
			return &packets.UnSubAck{
				FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
			}
		},
	}

	pingReqPool = sync.Pool{
		New: func() any {
			return &packets.PingReq{
				FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType},
			}
		},
	}

	pingRespPool = sync.Pool{
		New: func() any {
			return &packets.PingResp{
				FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
			}
		},
	}

	disconnectPool = sync.Pool{
		New: func() any {
			return &packets.Disconnect{
				FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
			}
		},
	}

	authPool = sync.Pool{
		New: func() any {
			return &packets.Auth{
				FixedHeader: packets.FixedHeader{PacketType: packets.AuthType},
			}
		},
	}
)

// AcquireConnect gets a Connect packet from the pool.
func AcquireConnect() *packets.Connect {
	return connectPool.Get().(*packets.Connect)
}

// ReleaseConnect returns a Connect packet to the pool.
// The packet must not be used after calling this function.
func ReleaseConnect(pkt *packets.Connect) {
	pkt.Reset()
	connectPool.Put(pkt)
}

// AcquireConnAck gets a ConnAck packet from the pool.
func AcquireConnAck() *packets.ConnAck {
	return connAckPool.Get().(*packets.ConnAck)
}

// ReleaseConnAck returns a ConnAck packet to the pool.
func ReleaseConnAck(pkt *packets.ConnAck) {
	pkt.Reset()
	connAckPool.Put(pkt)
}

// AcquirePublish gets a Publish packet from the pool.
func AcquirePublish() *packets.Publish {
	return publishPool.Get().(*packets.Publish)
}

// ReleasePublish returns a Publish packet to the pool.
func ReleasePublish(pkt *packets.Publish) {
	pkt.Reset()
	publishPool.Put(pkt)
}

// AcquirePubAck gets a PubAck packet from the pool.
func AcquirePubAck() *packets.PubAck {
	return pubAckPool.Get().(*packets.PubAck)
}

// ReleasePubAck returns a PubAck packet to the pool.
func ReleasePubAck(pkt *packets.PubAck) {
	pkt.Reset()
	pubAckPool.Put(pkt)
}

// AcquirePubRec gets a PubRec packet from the pool.
func AcquirePubRec() *packets.PubRec {
	return pubRecPool.Get().(*packets.PubRec)
}

// ReleasePubRec returns a PubRec packet to the pool.
func ReleasePubRec(pkt *packets.PubRec) {
	pkt.Reset()
	pubRecPool.Put(pkt)
}

// AcquirePubRel gets a PubRel packet from the pool.
func AcquirePubRel() *packets.PubRel {
	return pubRelPool.Get().(*packets.PubRel)
}

// ReleasePubRel returns a PubRel packet to the pool.
func ReleasePubRel(pkt *packets.PubRel) {
	pkt.Reset()
	pubRelPool.Put(pkt)
}

// AcquirePubComp gets a PubComp packet from the pool.
func AcquirePubComp() *packets.PubComp {
	return pubCompPool.Get().(*packets.PubComp)
}

// ReleasePubComp returns a PubComp packet to the pool.
func ReleasePubComp(pkt *packets.PubComp) {
	pkt.Reset()
	pubCompPool.Put(pkt)
}

// AcquireSubscribe gets a Subscribe packet from the pool.
func AcquireSubscribe() *packets.Subscribe {
	return subscribePool.Get().(*packets.Subscribe)
}

// ReleaseSubscribe returns a Subscribe packet to the pool.
func ReleaseSubscribe(pkt *packets.Subscribe) {
	pkt.Reset()
	subscribePool.Put(pkt)
}

// AcquireSubAck gets a SubAck packet from the pool.
func AcquireSubAck() *packets.SubAck {
	return subAckPool.Get().(*packets.SubAck)
}

// ReleaseSubAck returns a SubAck packet to the pool.
func ReleaseSubAck(pkt *packets.SubAck) {
	pkt.Reset()
	subAckPool.Put(pkt)
}

// AcquireUnsubscribe gets an Unsubscribe packet from the pool.
func AcquireUnsubscribe() *packets.Unsubscribe {
	return unsubscribePool.Get().(*packets.Unsubscribe)
}

// ReleaseUnsubscribe returns an Unsubscribe packet to the pool.
func ReleaseUnsubscribe(pkt *packets.Unsubscribe) {
	pkt.Reset()
	unsubscribePool.Put(pkt)
}

// AcquireUnsubAck gets an UnsubAck packet from the pool.
func AcquireUnsubAck() *packets.UnSubAck {
	return unsubAckPool.Get().(*packets.UnSubAck)
}

// ReleaseUnsubAck returns an UnsubAck packet to the pool.
func ReleaseUnsubAck(pkt *packets.UnSubAck) {
	pkt.Reset()
	unsubAckPool.Put(pkt)
}

// AcquirePingReq gets a PingReq packet from the pool.
func AcquirePingReq() *packets.PingReq {
	return pingReqPool.Get().(*packets.PingReq)
}

// ReleasePingReq returns a PingReq packet to the pool.
func ReleasePingReq(pkt *packets.PingReq) {
	pkt.Reset()
	pingReqPool.Put(pkt)
}

// AcquirePingResp gets a PingResp packet from the pool.
func AcquirePingResp() *packets.PingResp {
	return pingRespPool.Get().(*packets.PingResp)
}

// ReleasePingResp returns a PingResp packet to the pool.
func ReleasePingResp(pkt *packets.PingResp) {
	pkt.Reset()
	pingRespPool.Put(pkt)
}

// AcquireDisconnect gets a Disconnect packet from the pool.
func AcquireDisconnect() *packets.Disconnect {
	return disconnectPool.Get().(*packets.Disconnect)
}

// ReleaseDisconnect returns a Disconnect packet to the pool.
func ReleaseDisconnect(pkt *packets.Disconnect) {
	pkt.Reset()
	disconnectPool.Put(pkt)
}

// AcquireAuth gets an Auth packet from the pool.
func AcquireAuth() *packets.Auth {
	return authPool.Get().(*packets.Auth)
}

// ReleaseAuth returns an Auth packet to the pool.
func ReleaseAuth(pkt *packets.Auth) {
	pkt.Reset()
	authPool.Put(pkt)
}

// AcquireByType gets a packet of the specified type from the appropriate pool.
// Returns nil for unknown packet types.
func AcquireByType(packetType byte) packets.ControlPacket {
	switch packetType {
	case packets.ConnectType:
		return AcquireConnect()
	case packets.ConnAckType:
		return AcquireConnAck()
	case packets.PublishType:
		return AcquirePublish()
	case packets.PubAckType:
		return AcquirePubAck()
	case packets.PubRecType:
		return AcquirePubRec()
	case packets.PubRelType:
		return AcquirePubRel()
	case packets.PubCompType:
		return AcquirePubComp()
	case packets.SubscribeType:
		return AcquireSubscribe()
	case packets.SubAckType:
		return AcquireSubAck()
	case packets.UnsubscribeType:
		return AcquireUnsubscribe()
	case packets.UnsubAckType:
		return AcquireUnsubAck()
	case packets.PingReqType:
		return AcquirePingReq()
	case packets.PingRespType:
		return AcquirePingResp()
	case packets.DisconnectType:
		return AcquireDisconnect()
	case packets.AuthType:
		return AcquireAuth()
	}
	return nil
}

// Release returns a packet to its appropriate pool based on type.
// This is useful when you have a ControlPacket interface and need to release it.
func Release(pkt packets.ControlPacket) {
	switch p := pkt.(type) {
	case *packets.Connect:
		ReleaseConnect(p)
	case *packets.ConnAck:
		ReleaseConnAck(p)
	case *packets.Publish:
		ReleasePublish(p)
	case *packets.PubAck:
		ReleasePubAck(p)
	case *packets.PubRec:
		ReleasePubRec(p)
	case *packets.PubRel:
		ReleasePubRel(p)
	case *packets.PubComp:
		ReleasePubComp(p)
	case *packets.Subscribe:
		ReleaseSubscribe(p)
	case *packets.SubAck:
		ReleaseSubAck(p)
	case *packets.Unsubscribe:
		ReleaseUnsubscribe(p)
	case *packets.UnSubAck:
		ReleaseUnsubAck(p)
	case *packets.PingReq:
		ReleasePingReq(p)
	case *packets.PingResp:
		ReleasePingResp(p)
	case *packets.Disconnect:
		ReleaseDisconnect(p)
	case *packets.Auth:
		ReleaseAuth(p)
	}
}
