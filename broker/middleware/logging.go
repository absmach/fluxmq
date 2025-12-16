package middleware

import (
	"log/slog"
	"time"

	"github.com/absmach/mqtt/broker"
	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/core/packets"
	"github.com/absmach/mqtt/session"
)

var _ broker.Handler = (*loggingMiddleware)(nil)

type loggingMiddleware struct {
	logger *slog.Logger
	next   broker.Handler
}

// NewLogging creates logging middleware that wraps a broker handler.
func NewLogging(handler broker.Handler, logger *slog.Logger) broker.Handler {
	return &loggingMiddleware{logger, handler}
}

// HandleConnect logs CONNECT packet details.
// func (lm *loggingMiddleware) HandleConnection(conn core.Connection) {
// 	lm.next.HandleConnection(conn)
// }

// HandleConnect logs CONNECT packet details.
func (lm *loggingMiddleware) HandleConnect(conn core.Connection, pkt packets.ControlPacket) (err error) {
	defer func(begin time.Time) {
		lm.logger.Info("HandleConnect",
			slog.String("remote_addr", conn.RemoteAddr().String()),
			slog.String("duration", time.Since(begin).String()),
			slog.Any("error", err),
		)
	}(time.Now())
	// fmt.Println("handle conn")
	return lm.next.HandleConnect(conn, pkt)
}

// HandlePublish logs PUBLISH packet details.
func (lm *loggingMiddleware) HandlePublish(s *session.Session, pkt packets.ControlPacket) (err error) {
	defer func(begin time.Time) {
		lm.logger.Info("HandlePublish",
			slog.String("client_id", s.ID),
			slog.String("duration", time.Since(begin).String()),
			slog.Any("error", err),
		)
	}(time.Now())

	return lm.next.HandlePublish(s, pkt)
}

// HandlePubAck logs PUBACK packet details.
func (lm *loggingMiddleware) HandlePubAck(s *session.Session, pkt packets.ControlPacket) (err error) {
	defer func(begin time.Time) {
		lm.logger.Info("HandlePubAck",
			slog.String("client_id", s.ID),
			slog.String("duration", time.Since(begin).String()),
			slog.Any("error", err),
		)
	}(time.Now())
	return lm.next.HandlePubAck(s, pkt)
}

// HandlePubRec logs PUBREC packet details.
func (lm *loggingMiddleware) HandlePubRec(s *session.Session, pkt packets.ControlPacket) (err error) {
	defer func(begin time.Time) {
		lm.logger.Info("HandlePubRec",
			slog.String("client_id", s.ID),
			slog.String("duration", time.Since(begin).String()),
			slog.Any("error", err),
		)
	}(time.Now())

	return lm.next.HandlePubRec(s, pkt)
}

// HandlePubRel logs PUBREL packet details.
func (lm *loggingMiddleware) HandlePubRel(s *session.Session, pkt packets.ControlPacket) (err error) {
	defer func(begin time.Time) {
		lm.logger.Info("HandlePubRel",
			slog.String("client_id", s.ID),
			slog.String("duration", time.Since(begin).String()),
			slog.Any("error", err),
		)
	}(time.Now())

	return lm.next.HandlePubRel(s, pkt)
}

// HandlePubComp logs PUBCOMP packet details.
func (lm *loggingMiddleware) HandlePubComp(s *session.Session, pkt packets.ControlPacket) (err error) {
	defer func(begin time.Time) {
		lm.logger.Info("HandlePubComp",
			slog.String("client_id", s.ID),
			slog.String("duration", time.Since(begin).String()),
			slog.Any("error", err),
		)
	}(time.Now())

	return lm.next.HandlePubComp(s, pkt)
}

// HandleSubscribe logs SUBSCRIBE packet details.
func (lm *loggingMiddleware) HandleSubscribe(s *session.Session, pkt packets.ControlPacket) (err error) {
	defer func(begin time.Time) {
		lm.logger.Info("HandleSubscribe",
			slog.String("client_id", s.ID),
			slog.String("duration", time.Since(begin).String()),
			slog.Any("error", err),
		)
	}(time.Now())

	return lm.next.HandleSubscribe(s, pkt)
}

// HandleUnsubscribe logs UNSUBSCRIBE packet details.
func (lm *loggingMiddleware) HandleUnsubscribe(s *session.Session, pkt packets.ControlPacket) (err error) {
	defer func(begin time.Time) {
		lm.logger.Info("HandleUnsubscribe",
			slog.String("client_id", s.ID),
			slog.String("duration", time.Since(begin).String()),
			slog.Any("error", err),
		)
	}(time.Now())

	return lm.next.HandleUnsubscribe(s, pkt)
}

// HandlePingReq logs PINGREQ details.
func (lm *loggingMiddleware) HandlePingReq(s *session.Session) (err error) {
	defer func(begin time.Time) {
		lm.logger.Debug("HandlePingReq",
			slog.String("client_id", s.ID),
			slog.String("duration", time.Since(begin).String()),
			slog.Any("error", err),
		)
	}(time.Now())

	return lm.next.HandlePingReq(s)
}

// HandleDisconnect logs DISCONNECT packet details.
func (lm *loggingMiddleware) HandleDisconnect(s *session.Session, pkt packets.ControlPacket) (err error) {
	defer func(begin time.Time) {
		lm.logger.Info("HandleDisconnect",
			slog.String("client_id", s.ID),
			slog.String("duration", time.Since(begin).String()),
			slog.Any("error", err),
		)
	}(time.Now())

	return lm.next.HandleDisconnect(s, pkt)
}

// HandleAuth logs AUTH packet details.
func (lm *loggingMiddleware) HandleAuth(s *session.Session, pkt packets.ControlPacket) (err error) {
	defer func(begin time.Time) {
		lm.logger.Info("HandleAuth",
			slog.String("client_id", s.ID),
			slog.String("duration", time.Since(begin).String()),
			slog.Any("error", err),
		)
	}(time.Now())

	return lm.next.HandleAuth(s, pkt)
}
