package pool

import (
	"testing"

	v5 "github.com/absmach/mqtt/core/packets/v5"
)

func TestPublishPoolAcquireRelease(t *testing.T) {
	// Acquire
	pkt := AcquirePublish()
	if pkt == nil {
		t.Fatal("AcquirePublish returned nil")
	}
	if pkt.FixedHeader.PacketType != v5.PublishType {
		t.Errorf("PacketType: got %d, want %d", pkt.FixedHeader.PacketType, v5.PublishType)
	}

	// Modify
	pkt.TopicName = "test/topic"
	pkt.Payload = []byte("hello")
	pkt.QoS = 1
	pkt.ID = 123

	// Release
	ReleasePublish(pkt)

	// Acquire again - should be reset
	pkt2 := AcquirePublish()
	if pkt2.TopicName != "" {
		t.Errorf("TopicName not reset: got %q", pkt2.TopicName)
	}
	if pkt2.Payload != nil {
		t.Errorf("Payload not reset: got %v", pkt2.Payload)
	}
	if pkt2.QoS != 0 {
		t.Errorf("QoS not reset: got %d", pkt2.QoS)
	}
	if pkt2.ID != 0 {
		t.Errorf("ID not reset: got %d", pkt2.ID)
	}
	ReleasePublish(pkt2)
}

func TestConnectPoolAcquireRelease(t *testing.T) {
	pkt := AcquireConnect()
	if pkt == nil {
		t.Fatal("AcquireConnect returned nil")
	}

	pkt.ClientID = "test-client"
	pkt.CleanStart = true
	pkt.KeepAlive = 60

	ReleaseConnect(pkt)

	pkt2 := AcquireConnect()
	if pkt2.ClientID != "" {
		t.Errorf("ClientID not reset: got %q", pkt2.ClientID)
	}
	if pkt2.CleanStart {
		t.Error("CleanStart not reset")
	}
	if pkt2.KeepAlive != 0 {
		t.Errorf("KeepAlive not reset: got %d", pkt2.KeepAlive)
	}
	ReleaseConnect(pkt2)
}

func TestSubscribePoolAcquireRelease(t *testing.T) {
	pkt := AcquireSubscribe()
	if pkt == nil {
		t.Fatal("AcquireSubscribe returned nil")
	}

	pkt.ID = 1
	pkt.Opts = append(pkt.Opts, v5.SubOption{Topic: "test", MaxQoS: 1})

	ReleaseSubscribe(pkt)

	pkt2 := AcquireSubscribe()
	if pkt2.ID != 0 {
		t.Errorf("ID not reset: got %d", pkt2.ID)
	}
	if len(pkt2.Opts) != 0 {
		t.Errorf("Opts not reset: got %v", pkt2.Opts)
	}
	ReleaseSubscribe(pkt2)
}

func TestAcquireByType(t *testing.T) {
	tests := []struct {
		packetType byte
		wantNil    bool
	}{
		{v5.ConnectType, false},
		{v5.ConnAckType, false},
		{v5.PublishType, false},
		{v5.PubAckType, false},
		{v5.PubRecType, false},
		{v5.PubRelType, false},
		{v5.PubCompType, false},
		{v5.SubscribeType, false},
		{v5.SubAckType, false},
		{v5.UnsubscribeType, false},
		{v5.UnsubAckType, false},
		{v5.PingReqType, false},
		{v5.PingRespType, false},
		{v5.DisconnectType, false},
		{v5.AuthType, false},
		{99, true}, // Unknown type
	}

	for _, tt := range tests {
		pkt := AcquireByType(tt.packetType)
		if tt.wantNil && pkt != nil {
			t.Errorf("AcquireByType(%d): expected nil, got %T", tt.packetType, pkt)
		}
		if !tt.wantNil && pkt == nil {
			t.Errorf("AcquireByType(%d): expected packet, got nil", tt.packetType)
		}
		if pkt != nil {
			Release(pkt)
		}
	}
}

func TestRelease(t *testing.T) {
	// Test that Release correctly routes to the right pool
	pkts := []v5.ControlPacket{
		AcquireConnect(),
		AcquireConnAck(),
		AcquirePublish(),
		AcquirePubAck(),
		AcquireSubscribe(),
		AcquireDisconnect(),
	}

	for _, pkt := range pkts {
		// Should not panic
		Release(pkt)
	}
}

func BenchmarkPublishPoolAcquireRelease(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pkt := AcquirePublish()
		pkt.TopicName = "test/topic"
		pkt.Payload = []byte("hello world")
		ReleasePublish(pkt)
	}
}

func BenchmarkPublishNewAlloc(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pkt := &v5.Publish{
			FixedHeader: v5.FixedHeader{PacketType: v5.PublishType},
			TopicName:   "test/topic",
			Payload:     []byte("hello world"),
		}
		_ = pkt
	}
}

func BenchmarkBufferPoolSmall(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := AcquireSmallBuffer()
		*buf = append(*buf, "hello world"...)
		ReleaseSmallBuffer(buf)
	}
}

func BenchmarkBufferPoolMedium(b *testing.B) {
	b.ReportAllocs()
	data := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		buf := AcquireMediumBuffer()
		*buf = append(*buf, data...)
		ReleaseMediumBuffer(buf)
	}
}

func BenchmarkBufferNewAlloc(b *testing.B) {
	b.ReportAllocs()
	data := make([]byte, 1024)
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, 4096)
		buf = append(buf, data...)
		_ = buf
	}
}
