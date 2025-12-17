// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package pool

import (
	"sync"

	v3 "github.com/absmach/mqtt/core/packets/v3"
)

var (
	connectPool = sync.Pool{
		New: func() any {
			return &v3.Connect{
				FixedHeader: v3.FixedHeader{PacketType: v3.ConnectType},
			}
		},
	}

	connAckPool = sync.Pool{
		New: func() any {
			return &v3.ConnAck{
				FixedHeader: v3.FixedHeader{PacketType: v3.ConnAckType},
			}
		},
	}

	publishPool = sync.Pool{
		New: func() any {
			return &v3.Publish{
				FixedHeader: v3.FixedHeader{PacketType: v3.PublishType},
			}
		},
	}

	pubAckPool = sync.Pool{
		New: func() any {
			return &v3.PubAck{
				FixedHeader: v3.FixedHeader{PacketType: v3.PubAckType},
			}
		},
	}

	pubRecPool = sync.Pool{
		New: func() any {
			return &v3.PubRec{
				FixedHeader: v3.FixedHeader{PacketType: v3.PubRecType},
			}
		},
	}

	pubRelPool = sync.Pool{
		New: func() any {
			return &v3.PubRel{
				FixedHeader: v3.FixedHeader{PacketType: v3.PubRelType, QoS: 1},
			}
		},
	}

	pubCompPool = sync.Pool{
		New: func() any {
			return &v3.PubComp{
				FixedHeader: v3.FixedHeader{PacketType: v3.PubCompType},
			}
		},
	}

	subscribePool = sync.Pool{
		New: func() any {
			return &v3.Subscribe{
				FixedHeader: v3.FixedHeader{PacketType: v3.SubscribeType, QoS: 1},
			}
		},
	}

	subAckPool = sync.Pool{
		New: func() any {
			return &v3.SubAck{
				FixedHeader: v3.FixedHeader{PacketType: v3.SubAckType},
			}
		},
	}

	unsubscribePool = sync.Pool{
		New: func() any {
			return &v3.Unsubscribe{
				FixedHeader: v3.FixedHeader{PacketType: v3.UnsubscribeType, QoS: 1},
			}
		},
	}

	unsubAckPool = sync.Pool{
		New: func() any {
			return &v3.UnSubAck{
				FixedHeader: v3.FixedHeader{PacketType: v3.UnsubAckType},
			}
		},
	}

	pingReqPool = sync.Pool{
		New: func() any {
			return &v3.PingReq{
				FixedHeader: v3.FixedHeader{PacketType: v3.PingReqType},
			}
		},
	}

	pingRespPool = sync.Pool{
		New: func() any {
			return &v3.PingResp{
				FixedHeader: v3.FixedHeader{PacketType: v3.PingRespType},
			}
		},
	}

	disconnectPool = sync.Pool{
		New: func() any {
			return &v3.Disconnect{
				FixedHeader: v3.FixedHeader{PacketType: v3.DisconnectType},
			}
		},
	}
)

// AcquireConnect gets a Connect packet from the pool.
func AcquireConnect() *v3.Connect {
	return connectPool.Get().(*v3.Connect)
}

// ReleaseConnect returns a Connect packet to the pool.
// The packet must not be used after calling this function.
func ReleaseConnect(pkt *v3.Connect) {
	pkt.Reset()
	connectPool.Put(pkt)
}

// AcquireConnAck gets a ConnAck packet from the pool.
func AcquireConnAck() *v3.ConnAck {
	return connAckPool.Get().(*v3.ConnAck)
}

// ReleaseConnAck returns a ConnAck packet to the pool.
func ReleaseConnAck(pkt *v3.ConnAck) {
	pkt.Reset()
	connAckPool.Put(pkt)
}

// AcquirePublish gets a Publish packet from the pool.
func AcquirePublish() *v3.Publish {
	return publishPool.Get().(*v3.Publish)
}

// ReleasePublish returns a Publish packet to the pool.
func ReleasePublish(pkt *v3.Publish) {
	pkt.Reset()
	publishPool.Put(pkt)
}

// AcquirePubAck gets a PubAck packet from the pool.
func AcquirePubAck() *v3.PubAck {
	return pubAckPool.Get().(*v3.PubAck)
}

// ReleasePubAck returns a PubAck packet to the pool.
func ReleasePubAck(pkt *v3.PubAck) {
	pkt.Reset()
	pubAckPool.Put(pkt)
}

// AcquirePubRec gets a PubRec packet from the pool.
func AcquirePubRec() *v3.PubRec {
	return pubRecPool.Get().(*v3.PubRec)
}

// ReleasePubRec returns a PubRec packet to the pool.
func ReleasePubRec(pkt *v3.PubRec) {
	pkt.Reset()
	pubRecPool.Put(pkt)
}

// AcquirePubRel gets a PubRel packet from the pool.
func AcquirePubRel() *v3.PubRel {
	return pubRelPool.Get().(*v3.PubRel)
}

// ReleasePubRel returns a PubRel packet to the pool.
func ReleasePubRel(pkt *v3.PubRel) {
	pkt.Reset()
	pubRelPool.Put(pkt)
}

// AcquirePubComp gets a PubComp packet from the pool.
func AcquirePubComp() *v3.PubComp {
	return pubCompPool.Get().(*v3.PubComp)
}

// ReleasePubComp returns a PubComp packet to the pool.
func ReleasePubComp(pkt *v3.PubComp) {
	pkt.Reset()
	pubCompPool.Put(pkt)
}

// AcquireSubscribe gets a Subscribe packet from the pool.
func AcquireSubscribe() *v3.Subscribe {
	return subscribePool.Get().(*v3.Subscribe)
}

// ReleaseSubscribe returns a Subscribe packet to the pool.
func ReleaseSubscribe(pkt *v3.Subscribe) {
	pkt.Reset()
	subscribePool.Put(pkt)
}

// AcquireSubAck gets a SubAck packet from the pool.
func AcquireSubAck() *v3.SubAck {
	return subAckPool.Get().(*v3.SubAck)
}

// ReleaseSubAck returns a SubAck packet to the pool.
func ReleaseSubAck(pkt *v3.SubAck) {
	pkt.Reset()
	subAckPool.Put(pkt)
}

// AcquireUnsubscribe gets an Unsubscribe packet from the pool.
func AcquireUnsubscribe() *v3.Unsubscribe {
	return unsubscribePool.Get().(*v3.Unsubscribe)
}

// ReleaseUnsubscribe returns an Unsubscribe packet to the pool.
func ReleaseUnsubscribe(pkt *v3.Unsubscribe) {
	pkt.Reset()
	unsubscribePool.Put(pkt)
}

// AcquireUnsubAck gets an UnsubAck packet from the pool.
func AcquireUnsubAck() *v3.UnSubAck {
	return unsubAckPool.Get().(*v3.UnSubAck)
}

// ReleaseUnsubAck returns an UnsubAck packet to the pool.
func ReleaseUnsubAck(pkt *v3.UnSubAck) {
	pkt.Reset()
	unsubAckPool.Put(pkt)
}

// AcquirePingReq gets a PingReq packet from the pool.
func AcquirePingReq() *v3.PingReq {
	return pingReqPool.Get().(*v3.PingReq)
}

// ReleasePingReq returns a PingReq packet to the pool.
func ReleasePingReq(pkt *v3.PingReq) {
	pkt.Reset()
	pingReqPool.Put(pkt)
}

// AcquirePingResp gets a PingResp packet from the pool.
func AcquirePingResp() *v3.PingResp {
	return pingRespPool.Get().(*v3.PingResp)
}

// ReleasePingResp returns a PingResp packet to the pool.
func ReleasePingResp(pkt *v3.PingResp) {
	pkt.Reset()
	pingRespPool.Put(pkt)
}

// AcquireDisconnect gets a Disconnect packet from the pool.
func AcquireDisconnect() *v3.Disconnect {
	return disconnectPool.Get().(*v3.Disconnect)
}

// ReleaseDisconnect returns a Disconnect packet to the pool.
func ReleaseDisconnect(pkt *v3.Disconnect) {
	pkt.Reset()
	disconnectPool.Put(pkt)
}

// AcquireByType gets a packet of the specified type from the appropriate pool.
// Returns nil for unknown packet types.
func AcquireByType(packetType byte) v3.ControlPacket {
	switch packetType {
	case v3.ConnectType:
		return AcquireConnect()
	case v3.ConnAckType:
		return AcquireConnAck()
	case v3.PublishType:
		return AcquirePublish()
	case v3.PubAckType:
		return AcquirePubAck()
	case v3.PubRecType:
		return AcquirePubRec()
	case v3.PubRelType:
		return AcquirePubRel()
	case v3.PubCompType:
		return AcquirePubComp()
	case v3.SubscribeType:
		return AcquireSubscribe()
	case v3.SubAckType:
		return AcquireSubAck()
	case v3.UnsubscribeType:
		return AcquireUnsubscribe()
	case v3.UnsubAckType:
		return AcquireUnsubAck()
	case v3.PingReqType:
		return AcquirePingReq()
	case v3.PingRespType:
		return AcquirePingResp()
	case v3.DisconnectType:
		return AcquireDisconnect()
	}
	return nil
}

// Release returns a packet to its appropriate pool based on type.
// This is useful when you have a ControlPacket interface and need to release it.
func Release(pkt v3.ControlPacket) {
	switch p := pkt.(type) {
	case *v3.Connect:
		ReleaseConnect(p)
	case *v3.ConnAck:
		ReleaseConnAck(p)
	case *v3.Publish:
		ReleasePublish(p)
	case *v3.PubAck:
		ReleasePubAck(p)
	case *v3.PubRec:
		ReleasePubRec(p)
	case *v3.PubRel:
		ReleasePubRel(p)
	case *v3.PubComp:
		ReleasePubComp(p)
	case *v3.Subscribe:
		ReleaseSubscribe(p)
	case *v3.SubAck:
		ReleaseSubAck(p)
	case *v3.Unsubscribe:
		ReleaseUnsubscribe(p)
	case *v3.UnSubAck:
		ReleaseUnsubAck(p)
	case *v3.PingReq:
		ReleasePingReq(p)
	case *v3.PingResp:
		ReleasePingResp(p)
	case *v3.Disconnect:
		ReleaseDisconnect(p)
	}
}
