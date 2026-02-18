// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"fmt"
	"sync"

	"github.com/absmach/fluxmq/amqp1/performatives"
	"github.com/absmach/fluxmq/topics"
)

const (
	initialWindow            = uint32(65535)
	windowReplenishThreshold = initialWindow / 2
)

// Session represents an AMQP session (mapped to a channel pair).
type Session struct {
	conn     *Connection
	localCh  uint16
	remoteCh uint16

	links     map[uint32]*Link
	linksMu   sync.RWMutex
	handleMax uint32

	// Session-level flow control (protected by windowMu)
	nextIncomingID       uint32 // next expected incoming delivery ID
	nextOutgoingID       uint32 // next outgoing delivery ID
	incomingWindow       uint32 // our advertised incoming window
	outgoingWindow       uint32 // our advertised outgoing window capacity
	remoteIncomingWindow uint32 // remote's incoming window
	remoteOutgoingWindow uint32 // remote's outgoing window
	remoteNextIncomingID uint32 // remote's next-incoming-id from their Flow
	windowMu             sync.Mutex
}

func newSession(c *Connection, localCh, remoteCh uint16, handleMax uint32) *Session {
	return &Session{
		conn:           c,
		localCh:        localCh,
		remoteCh:       remoteCh,
		links:          make(map[uint32]*Link),
		handleMax:      handleMax,
		incomingWindow: initialWindow,
		outgoingWindow: initialWindow,
	}
}

// initWindows initializes window state from the remote Begin frame.
func (s *Session) initWindows(remoteBegin *performatives.Begin) {
	s.windowMu.Lock()
	defer s.windowMu.Unlock()
	s.nextIncomingID = remoteBegin.NextOutgoingID
	s.remoteIncomingWindow = remoteBegin.IncomingWindow
	s.remoteOutgoingWindow = remoteBegin.OutgoingWindow
}

// consumeOutgoingWindow attempts to consume one unit of outgoing window.
// Returns the allocated delivery ID and true if successful, or 0 and false
// if the outgoing window or the remote incoming window is exhausted.
func (s *Session) consumeOutgoingWindow() (uint32, bool) {
	s.windowMu.Lock()
	defer s.windowMu.Unlock()

	if s.outgoingWindow == 0 || s.remoteIncomingWindow == 0 {
		return 0, false
	}

	id := s.nextOutgoingID
	s.nextOutgoingID++
	s.outgoingWindow--
	s.remoteIncomingWindow--
	return id, true
}

// trackIncomingTransfer updates incoming window tracking for a received transfer.
// Returns false if the delivery ID is out of window.
func (s *Session) trackIncomingTransfer(deliveryID *uint32) bool {
	s.windowMu.Lock()

	if s.incomingWindow == 0 {
		s.windowMu.Unlock()
		return false
	}

	s.incomingWindow--
	if deliveryID != nil {
		s.nextIncomingID = *deliveryID + 1
	}

	needReplenish := s.incomingWindow < windowReplenishThreshold
	var nextIn, inWin, nextOut, outWin uint32
	if needReplenish {
		s.incomingWindow = initialWindow
		nextIn = s.nextIncomingID
		inWin = s.incomingWindow
		nextOut = s.nextOutgoingID
		outWin = s.outgoingWindow
	}
	s.windowMu.Unlock()

	if needReplenish {
		s.sendSessionFlow(nextIn, inWin, nextOut, outWin)
	}
	return true
}

// updateRemoteFlow updates session windows from a received session-level Flow.
func (s *Session) updateRemoteFlow(flow *performatives.Flow) {
	s.windowMu.Lock()
	defer s.windowMu.Unlock()

	if flow.NextIncomingID != nil {
		s.remoteNextIncomingID = *flow.NextIncomingID
	}
	s.remoteIncomingWindow = flow.IncomingWindow
	s.remoteOutgoingWindow = flow.OutgoingWindow
}

// sessionFlowState returns current window state for inclusion in outgoing Flow frames.
func (s *Session) sessionFlowState() (nextIncomingID, incomingWindow, nextOutgoingID, outgoingWindow uint32) {
	s.windowMu.Lock()
	defer s.windowMu.Unlock()
	return s.nextIncomingID, s.incomingWindow, s.nextOutgoingID, s.outgoingWindow
}

func (s *Session) sendSessionFlow(nextIn, inWin, nextOut, outWin uint32) {
	flow := &performatives.Flow{
		NextIncomingID: &nextIn,
		IncomingWindow: inWin,
		NextOutgoingID: nextOut,
		OutgoingWindow: outWin,
	}
	body, err := flow.Encode()
	if err != nil {
		s.conn.logger.Error("failed to encode session flow", "error", err)
		return
	}
	if err := s.conn.conn.WritePerformative(s.localCh, body); err != nil {
		s.conn.logger.Error("failed to send session flow", "error", err)
	}
}

func (s *Session) handleAttach(attach *performatives.Attach) error {
	// Check handle limit
	if attach.Handle > s.handleMax {
		resp := &performatives.Detach{
			Handle: attach.Handle,
			Closed: true,
			Error: &performatives.Error{
				Condition:   performatives.ErrNotAllowed,
				Description: fmt.Sprintf("handle %d exceeds max %d", attach.Handle, s.handleMax),
			},
		}
		body, err := resp.Encode()
		if err != nil {
			return err
		}
		return s.conn.conn.WritePerformative(s.localCh, body)
	}

	link := newLink(s, attach)

	s.linksMu.Lock()
	// Link steal: if handle is already in use, detach the old link
	if old, exists := s.links[attach.Handle]; exists {
		s.linksMu.Unlock()
		old.detach()
		s.linksMu.Lock()
	}
	s.links[attach.Handle] = link
	s.linksMu.Unlock()

	s.conn.broker.stats.IncrementLinks()
	if m := s.conn.broker.metrics; m != nil {
		m.RecordLinkAttached()
	}

	// Send attach response
	respSource := attach.Source
	respTarget := attach.Target

	// For management receiver links with dynamic source, assign a reply address
	if link.isManagement && attach.Role && attach.Source != nil && attach.Source.Dynamic {
		respSource = &performatives.Source{
			Address: fmt.Sprintf("$management/reply/%d", attach.Handle),
			Dynamic: false,
		}
		link.address = respSource.Address
	}

	resp := &performatives.Attach{
		Name:   attach.Name,
		Handle: attach.Handle,
		Role:   !attach.Role, // mirror the role
		Source: respSource,
		Target: respTarget,
	}

	// If we are the sender side, set initial delivery count
	if !attach.Role { // client is sender -> we are receiver
		resp.InitialDeliveryCount = 0
	}

	body, err := resp.Encode()
	if err != nil {
		return err
	}
	if err := s.conn.conn.WritePerformative(s.localCh, body); err != nil {
		return err
	}

	if link.isManagement {
		// Management links: sender links get credit, receiver links are used for responses
		if !attach.Role { // client is sender -> grant credit for management requests
			if err := s.grantCredit(link, 100); err != nil {
				return err
			}
		}
	} else if attach.Role { // client is receiver -> we are sender
		link.subscribe()
		s.conn.broker.stats.IncrementSubscriptions()
		if m := s.conn.broker.metrics; m != nil {
			m.RecordSubscriptionAdded()
		}
	} else {
		// Client is sender, broker is receiver — grant initial credit
		if err := s.grantCredit(link, 100); err != nil {
			return err
		}
	}

	return nil
}

func (s *Session) handleFlow(flow *performatives.Flow) {
	// Always update session-level windows
	s.updateRemoteFlow(flow)

	if flow.Handle == nil {
		return
	}

	s.linksMu.RLock()
	link := s.links[*flow.Handle]
	s.linksMu.RUnlock()

	if link == nil {
		return
	}

	// Update link credit
	if flow.LinkCredit != nil {
		link.mu.Lock()
		oldCredit := link.credit
		if flow.DeliveryCount != nil {
			link.credit = *flow.LinkCredit
			link.deliveryCount = *flow.DeliveryCount
		} else {
			link.credit = *flow.LinkCredit
		}
		link.mu.Unlock()

		// If credit was granted from zero, trigger delivery
		if oldCredit == 0 && *flow.LinkCredit > 0 {
			go link.drainPending()
		}
	}
}

func (s *Session) handleTransfer(transfer *performatives.Transfer, payload []byte) {
	if !s.trackIncomingTransfer(transfer.DeliveryID) {
		s.conn.logger.Warn("incoming transfer exceeds window, dropping")
		s.conn.broker.stats.IncrementProtocolErrors()
		if m := s.conn.broker.metrics; m != nil {
			m.RecordError("window_exceeded")
		}
		return
	}

	s.linksMu.RLock()
	link := s.links[transfer.Handle]
	s.linksMu.RUnlock()

	if link == nil {
		return
	}

	link.receiveTransfer(transfer, payload)
}

func (s *Session) handleDisposition(disp *performatives.Disposition) {
	s.linksMu.RLock()
	defer s.linksMu.RUnlock()

	for _, link := range s.links {
		link.handleDisposition(disp)
	}
}

func (s *Session) handleDetach(detach *performatives.Detach) {
	s.linksMu.Lock()
	link := s.links[detach.Handle]
	delete(s.links, detach.Handle)
	s.linksMu.Unlock()

	if link != nil {
		link.detach()
		s.conn.broker.stats.DecrementLinks()
		if m := s.conn.broker.metrics; m != nil {
			m.RecordLinkDetached()
		}
		if link.isSender {
			s.conn.broker.stats.DecrementSubscriptions()
			if m := s.conn.broker.metrics; m != nil {
				m.RecordSubscriptionRemoved()
			}
		}
	}

	// Send detach response
	resp := &performatives.Detach{
		Handle: detach.Handle,
		Closed: detach.Closed,
	}
	body, err := resp.Encode()
	if err != nil {
		s.conn.logger.Error("failed to encode detach response", "error", err)
		return
	}
	if err := s.conn.conn.WritePerformative(s.localCh, body); err != nil {
		s.conn.logger.Error("failed to send detach response", "error", err)
	}
}

func (s *Session) cleanup() {
	s.linksMu.Lock()
	links := make([]*Link, 0, len(s.links))
	for _, l := range s.links {
		links = append(links, l)
	}
	s.links = make(map[uint32]*Link)
	s.linksMu.Unlock()

	for _, l := range links {
		l.detach()
	}
}

// deliverToMatchingLinks delivers a message to all receiver links that match the topic.
func (s *Session) deliverToMatchingLinks(topic string, payload []byte, props map[string]string, qos byte) {
	s.linksMu.RLock()
	defer s.linksMu.RUnlock()

	for _, link := range s.links {
		if link.isSender && topics.TopicMatch(link.address, topic) {
			link.sendMessage(topic, payload, props, qos)
		}
	}
}

// deliverAMQPMessageToLinks delivers a pre-built AMQP message to matching links.
func (s *Session) deliverAMQPMessageToLinks(topic string, msg any, qos byte) {
	s.linksMu.RLock()
	defer s.linksMu.RUnlock()

	for _, link := range s.links {
		if link.isSender && topics.TopicMatch(link.address, topic) {
			link.sendAMQPMessage(msg, qos)
		}
	}
}

// findManagementSenderLink finds the management sender link (broker→client) in this session.
func (s *Session) findManagementSenderLink() *Link {
	s.linksMu.RLock()
	defer s.linksMu.RUnlock()

	for _, link := range s.links {
		if link.isManagement && link.isSender {
			return link
		}
	}
	return nil
}

// grantCredit sends a Flow frame granting link credit to the remote sender.
func (s *Session) grantCredit(link *Link, credit uint32) error {
	handle := link.handle
	deliveryCount := uint32(0)
	nextIn, inWin, nextOut, outWin := s.sessionFlowState()
	flow := &performatives.Flow{
		NextIncomingID: &nextIn,
		IncomingWindow: inWin,
		NextOutgoingID: nextOut,
		OutgoingWindow: outWin,
		Handle:         &handle,
		DeliveryCount:  &deliveryCount,
		LinkCredit:     &credit,
	}
	body, err := flow.Encode()
	if err != nil {
		return err
	}
	return s.conn.conn.WritePerformative(s.localCh, body)
}
