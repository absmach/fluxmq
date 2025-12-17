// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package pool

import (
	"sync"

	v5 "github.com/absmach/mqtt/core/packets/v5"
)

var (
	connectPool = sync.Pool{
		New: func() any {
			return &v5.Connect{
				FixedHeader: v5.FixedHeader{PacketType: v5.ConnectType},
			}
		},
	}

	connAckPool = sync.Pool{
		New: func() any {
			return &v5.ConnAck{
				FixedHeader: v5.FixedHeader{PacketType: v5.ConnAckType},
			}
		},
	}

	publishPool = sync.Pool{
		New: func() any {
			return &v5.Publish{
				FixedHeader: v5.FixedHeader{PacketType: v5.PublishType},
			}
		},
	}

	pubAckPool = sync.Pool{
		New: func() any {
			return &v5.PubAck{
				FixedHeader: v5.FixedHeader{PacketType: v5.PubAckType},
			}
		},
	}

	pubRecPool = sync.Pool{
		New: func() any {
			return &v5.PubRec{
				FixedHeader: v5.FixedHeader{PacketType: v5.PubRecType},
			}
		},
	}

	pubRelPool = sync.Pool{
		New: func() any {
			return &v5.PubRel{
				FixedHeader: v5.FixedHeader{PacketType: v5.PubRelType, QoS: 1},
			}
		},
	}

	pubCompPool = sync.Pool{
		New: func() any {
			return &v5.PubComp{
				FixedHeader: v5.FixedHeader{PacketType: v5.PubCompType},
			}
		},
	}

	subscribePool = sync.Pool{
		New: func() any {
			return &v5.Subscribe{
				FixedHeader: v5.FixedHeader{PacketType: v5.SubscribeType, QoS: 1},
			}
		},
	}

	subAckPool = sync.Pool{
		New: func() any {
			return &v5.SubAck{
				FixedHeader: v5.FixedHeader{PacketType: v5.SubAckType},
			}
		},
	}

	unsubscribePool = sync.Pool{
		New: func() any {
			return &v5.Unsubscribe{
				FixedHeader: v5.FixedHeader{PacketType: v5.UnsubscribeType, QoS: 1},
			}
		},
	}

	unsubAckPool = sync.Pool{
		New: func() any {
			return &v5.UnSubAck{
				FixedHeader: v5.FixedHeader{PacketType: v5.UnsubAckType},
			}
		},
	}

	pingReqPool = sync.Pool{
		New: func() any {
			return &v5.PingReq{
				FixedHeader: v5.FixedHeader{PacketType: v5.PingReqType},
			}
		},
	}

	pingRespPool = sync.Pool{
		New: func() any {
			return &v5.PingResp{
				FixedHeader: v5.FixedHeader{PacketType: v5.PingRespType},
			}
		},
	}

	disconnectPool = sync.Pool{
		New: func() any {
			return &v5.Disconnect{
				FixedHeader: v5.FixedHeader{PacketType: v5.DisconnectType},
			}
		},
	}

	authPool = sync.Pool{
		New: func() any {
			return &v5.Auth{
				FixedHeader: v5.FixedHeader{PacketType: v5.AuthType},
			}
		},
	}
)

// AcquireConnect gets a Connect packet from the pool.
func AcquireConnect() *v5.Connect {
	return connectPool.Get().(*v5.Connect)
}

// ReleaseConnect returns a Connect packet to the pool.
// The packet must not be used after calling this function.
func ReleaseConnect(pkt *v5.Connect) {
	pkt.Reset()
	connectPool.Put(pkt)
}

// AcquireConnAck gets a ConnAck packet from the pool.
func AcquireConnAck() *v5.ConnAck {
	return connAckPool.Get().(*v5.ConnAck)
}

// ReleaseConnAck returns a ConnAck packet to the pool.
func ReleaseConnAck(pkt *v5.ConnAck) {
	pkt.Reset()
	connAckPool.Put(pkt)
}

// AcquirePublish gets a Publish packet from the pool.
func AcquirePublish() *v5.Publish {
	return publishPool.Get().(*v5.Publish)
}

// ReleasePublish returns a Publish packet to the pool.
func ReleasePublish(pkt *v5.Publish) {
	pkt.Reset()
	publishPool.Put(pkt)
}

// AcquirePubAck gets a PubAck packet from the pool.
func AcquirePubAck() *v5.PubAck {
	return pubAckPool.Get().(*v5.PubAck)
}

// ReleasePubAck returns a PubAck packet to the pool.
func ReleasePubAck(pkt *v5.PubAck) {
	pkt.Reset()
	pubAckPool.Put(pkt)
}

// AcquirePubRec gets a PubRec packet from the pool.
func AcquirePubRec() *v5.PubRec {
	return pubRecPool.Get().(*v5.PubRec)
}

// ReleasePubRec returns a PubRec packet to the pool.
func ReleasePubRec(pkt *v5.PubRec) {
	pkt.Reset()
	pubRecPool.Put(pkt)
}

// AcquirePubRel gets a PubRel packet from the pool.
func AcquirePubRel() *v5.PubRel {
	return pubRelPool.Get().(*v5.PubRel)
}

// ReleasePubRel returns a PubRel packet to the pool.
func ReleasePubRel(pkt *v5.PubRel) {
	pkt.Reset()
	pubRelPool.Put(pkt)
}

// AcquirePubComp gets a PubComp packet from the pool.
func AcquirePubComp() *v5.PubComp {
	return pubCompPool.Get().(*v5.PubComp)
}

// ReleasePubComp returns a PubComp packet to the pool.
func ReleasePubComp(pkt *v5.PubComp) {
	pkt.Reset()
	pubCompPool.Put(pkt)
}

// AcquireSubscribe gets a Subscribe packet from the pool.
func AcquireSubscribe() *v5.Subscribe {
	return subscribePool.Get().(*v5.Subscribe)
}

// ReleaseSubscribe returns a Subscribe packet to the pool.
func ReleaseSubscribe(pkt *v5.Subscribe) {
	pkt.Reset()
	subscribePool.Put(pkt)
}

// AcquireSubAck gets a SubAck packet from the pool.
func AcquireSubAck() *v5.SubAck {
	return subAckPool.Get().(*v5.SubAck)
}

// ReleaseSubAck returns a SubAck packet to the pool.
func ReleaseSubAck(pkt *v5.SubAck) {
	pkt.Reset()
	subAckPool.Put(pkt)
}

// AcquireUnsubscribe gets an Unsubscribe packet from the pool.
func AcquireUnsubscribe() *v5.Unsubscribe {
	return unsubscribePool.Get().(*v5.Unsubscribe)
}

// ReleaseUnsubscribe returns an Unsubscribe packet to the pool.
func ReleaseUnsubscribe(pkt *v5.Unsubscribe) {
	pkt.Reset()
	unsubscribePool.Put(pkt)
}

// AcquireUnsubAck gets an UnsubAck packet from the pool.
func AcquireUnsubAck() *v5.UnSubAck {
	return unsubAckPool.Get().(*v5.UnSubAck)
}

// ReleaseUnsubAck returns an UnsubAck packet to the pool.
func ReleaseUnsubAck(pkt *v5.UnSubAck) {
	pkt.Reset()
	unsubAckPool.Put(pkt)
}

// AcquirePingReq gets a PingReq packet from the pool.
func AcquirePingReq() *v5.PingReq {
	return pingReqPool.Get().(*v5.PingReq)
}

// ReleasePingReq returns a PingReq packet to the pool.
func ReleasePingReq(pkt *v5.PingReq) {
	pkt.Reset()
	pingReqPool.Put(pkt)
}

// AcquirePingResp gets a PingResp packet from the pool.
func AcquirePingResp() *v5.PingResp {
	return pingRespPool.Get().(*v5.PingResp)
}

// ReleasePingResp returns a PingResp packet to the pool.
func ReleasePingResp(pkt *v5.PingResp) {
	pkt.Reset()
	pingRespPool.Put(pkt)
}

// AcquireDisconnect gets a Disconnect packet from the pool.
func AcquireDisconnect() *v5.Disconnect {
	return disconnectPool.Get().(*v5.Disconnect)
}

// ReleaseDisconnect returns a Disconnect packet to the pool.
func ReleaseDisconnect(pkt *v5.Disconnect) {
	pkt.Reset()
	disconnectPool.Put(pkt)
}

// AcquireAuth gets an Auth packet from the pool.
func AcquireAuth() *v5.Auth {
	return authPool.Get().(*v5.Auth)
}

// ReleaseAuth returns an Auth packet to the pool.
func ReleaseAuth(pkt *v5.Auth) {
	pkt.Reset()
	authPool.Put(pkt)
}

// AcquireByType gets a packet of the specified type from the appropriate pool.
// Returns nil for unknown packet types.
func AcquireByType(packetType byte) v5.ControlPacket {
	switch packetType {
	case v5.ConnectType:
		return AcquireConnect()
	case v5.ConnAckType:
		return AcquireConnAck()
	case v5.PublishType:
		return AcquirePublish()
	case v5.PubAckType:
		return AcquirePubAck()
	case v5.PubRecType:
		return AcquirePubRec()
	case v5.PubRelType:
		return AcquirePubRel()
	case v5.PubCompType:
		return AcquirePubComp()
	case v5.SubscribeType:
		return AcquireSubscribe()
	case v5.SubAckType:
		return AcquireSubAck()
	case v5.UnsubscribeType:
		return AcquireUnsubscribe()
	case v5.UnsubAckType:
		return AcquireUnsubAck()
	case v5.PingReqType:
		return AcquirePingReq()
	case v5.PingRespType:
		return AcquirePingResp()
	case v5.DisconnectType:
		return AcquireDisconnect()
	case v5.AuthType:
		return AcquireAuth()
	}
	return nil
}

// Release returns a packet to its appropriate pool based on type.
// This is useful when you have a ControlPacket interface and need to release it.
func Release(pkt v5.ControlPacket) {
	switch p := pkt.(type) {
	case *v5.Connect:
		ReleaseConnect(p)
	case *v5.ConnAck:
		ReleaseConnAck(p)
	case *v5.Publish:
		ReleasePublish(p)
	case *v5.PubAck:
		ReleasePubAck(p)
	case *v5.PubRec:
		ReleasePubRec(p)
	case *v5.PubRel:
		ReleasePubRel(p)
	case *v5.PubComp:
		ReleasePubComp(p)
	case *v5.Subscribe:
		ReleaseSubscribe(p)
	case *v5.SubAck:
		ReleaseSubAck(p)
	case *v5.Unsubscribe:
		ReleaseUnsubscribe(p)
	case *v5.UnSubAck:
		ReleaseUnsubAck(p)
	case *v5.PingReq:
		ReleasePingReq(p)
	case *v5.PingResp:
		ReleasePingResp(p)
	case *v5.Disconnect:
		ReleaseDisconnect(p)
	case *v5.Auth:
		ReleaseAuth(p)
	}
}
