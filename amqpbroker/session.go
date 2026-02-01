// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqpbroker

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/absmach/fluxmq/amqp/performatives"
	"github.com/absmach/fluxmq/topics"
)

// Session represents an AMQP session (mapped to a channel pair).
type Session struct {
	conn     *Connection
	localCh  uint16
	remoteCh uint16

	links    map[uint32]*Link
	linksMu  sync.RWMutex
	handleMax uint32

	nextDeliveryID       atomic.Uint32
	remoteIncomingWindow uint32
	remoteOutgoingWindow uint32
}

func newSession(c *Connection, localCh, remoteCh uint16, handleMax uint32) *Session {
	return &Session{
		conn:      c,
		localCh:   localCh,
		remoteCh:  remoteCh,
		links:     make(map[uint32]*Link),
		handleMax: handleMax,
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

	// Send attach response
	resp := &performatives.Attach{
		Name:   attach.Name,
		Handle: attach.Handle,
		Role:   !attach.Role, // mirror the role (if client is sender, we are receiver and vice versa)
		Source: attach.Source,
		Target: attach.Target,
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

	// Register subscription for receiver links (where we send messages TO the client)
	if attach.Role { // client is receiver -> we are sender
		link.subscribe()
	} else {
		// Client is sender, broker is receiver — grant initial credit
		if err := s.grantCredit(link, 100); err != nil {
			return err
		}
	}

	return nil
}

func (s *Session) handleFlow(flow *performatives.Flow) {
	if flow.Handle == nil {
		// Session-level flow, update windows
		s.remoteIncomingWindow = flow.IncomingWindow
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
			// credit = link-credit - (delivery-count - remote-delivery-count)
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
	}

	// Send detach response
	resp := &performatives.Detach{
		Handle: detach.Handle,
		Closed: detach.Closed,
	}
	body, _ := resp.Encode()
	s.conn.conn.WritePerformative(s.localCh, body)
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
func (s *Session) deliverAMQPMessageToLinks(topic string, msg interface{}, qos byte) {
	s.linksMu.RLock()
	defer s.linksMu.RUnlock()

	for _, link := range s.links {
		if link.isSender && topics.TopicMatch(link.address, topic) {
			link.sendAMQPMessage(msg, qos)
		}
	}
}

// grantCredit sends a Flow frame granting link credit to the remote sender.
func (s *Session) grantCredit(link *Link, credit uint32) error {
	handle := link.handle
	deliveryCount := uint32(0)
	flow := &performatives.Flow{
		IncomingWindow: 2048,
		OutgoingWindow: 2048,
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

// allocateDeliveryID returns the next delivery ID.
func (s *Session) allocateDeliveryID() uint32 {
	return s.nextDeliveryID.Add(1) - 1
}
