// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"net"
	"time"
)

func (c *Client) loadWriteRuntime() *writeRuntime {
	if rt := c.writeRT.Load(); rt != nil {
		return rt
	}

	c.writeStateMu.RLock()
	defer c.writeStateMu.RUnlock()
	if c.conn == nil && c.stopCh == nil && c.doneCh == nil && c.writeCh == nil && c.controlWriteCh == nil && c.qos0Wake == nil && c.writeDone == nil {
		return nil
	}

	rt := &writeRuntime{
		conn:           c.conn,
		stopCh:         c.stopCh,
		doneCh:         c.doneCh,
		writeCh:        c.writeCh,
		controlWriteCh: c.controlWriteCh,
		qos0Wake:       c.qos0Wake,
		writeDone:      c.writeDone,
	}
	// Cache the snapshot so subsequent calls on the same connection cycle
	// don't allocate. CAS avoids overwriting a value stored by Connect().
	c.writeRT.CompareAndSwap(nil, rt)
	return rt
}

func (c *Client) writeLoop() {
	rt := c.loadWriteRuntime()
	if rt == nil {
		return
	}

	stopCh := rt.stopCh
	writeCh := rt.writeCh
	controlWriteCh := rt.controlWriteCh
	qos0Wake := rt.qos0Wake
	writeDone := rt.writeDone
	conn := rt.conn

	if stopCh == nil || writeCh == nil || controlWriteCh == nil || qos0Wake == nil || writeDone == nil {
		return
	}

	defer close(writeDone)

	const maxWriteBatchItems = 32
	const maxWriteBatchBytes = 256 * 1024

	batch := make([]writeRequest, 0, maxWriteBatchItems)
	buffers := make(net.Buffers, 0, maxWriteBatchItems)

	resetBatch := func() {
		batch = batch[:0]
		buffers = buffers[:0]
	}
	appendBatch := func(req writeRequest) {
		batch = append(batch, req)
		buffers = append(buffers, req.data)
	}

	writeBatch := func() bool {
		if len(batch) == 0 {
			return true
		}

		ackBatch := func(err error) {
			for _, req := range batch {
				if req.trackOutbound {
					c.releaseOutbound(req)
				}
				if req.errCh != nil {
					req.errCh <- err
				}
			}
			c.notifyDrain()
		}

		if conn == nil {
			ackBatch(ErrNotConnected)
			resetBatch()
			return true
		}

		deadline := time.Time{}
		for _, req := range batch {
			if req.deadline.IsZero() {
				continue
			}
			if deadline.IsZero() || req.deadline.Before(deadline) {
				deadline = req.deadline
			}
		}

		if !deadline.IsZero() {
			conn.SetWriteDeadline(deadline)
		}
		_, err := buffers.WriteTo(conn)
		if !deadline.IsZero() {
			conn.SetWriteDeadline(time.Time{})
		}

		ackBatch(err)
		resetBatch()
		return err == nil
	}

	collectBatch := func(first writeRequest) {
		resetBatch()
		appendBatch(first)
		totalBytes := len(first.data)

		for len(batch) < maxWriteBatchItems && totalBytes < maxWriteBatchBytes {
			select {
			case req := <-controlWriteCh:
				appendBatch(req)
				totalBytes += len(req.data)
				continue
			default:
			}

			select {
			case req := <-controlWriteCh:
				appendBatch(req)
				totalBytes += len(req.data)
			case req := <-writeCh:
				appendBatch(req)
				totalBytes += len(req.data)
			default:
				return
			}
		}
	}

	collectBufferedQoS0Batch := func() bool {
		extra := c.popBufferedQoS0Batch(maxWriteBatchItems, maxWriteBatchBytes)
		if len(extra) == 0 {
			return false
		}

		resetBatch()
		for _, req := range extra {
			appendBatch(req)
		}
		return true
	}

	for {
		// Prefer control frames whenever present.
		select {
		case <-stopCh:
			return
		case req := <-controlWriteCh:
			collectBatch(req)
			if !writeBatch() {
				return
			}
			continue
		default:
		}

		select {
		case <-stopCh:
			return
		case req := <-controlWriteCh:
			collectBatch(req)
			if !writeBatch() {
				return
			}
		case <-qos0Wake:
			if !collectBufferedQoS0Batch() {
				continue
			}
			// Keep draining buffered QoS0 writes until empty.
			if c.hasBufferedQoS0Writes() {
				select {
				case qos0Wake <- struct{}{}:
				default:
				}
			}
			if !writeBatch() {
				return
			}
		case req := <-writeCh:
			collectBatch(req)
			if !writeBatch() {
				return
			}
		}
	}
}

func (c *Client) enqueueWriteTo(req writeRequest, control bool) (retErr error) {
	reservedOutbound := false
	defer func() {
		if retErr != nil && reservedOutbound {
			c.releaseOutbound(req)
		}
	}()
	rt := c.loadWriteRuntime()
	if rt == nil {
		return ErrNotConnected
	}
	dataCh := rt.writeCh
	controlCh := rt.controlWriteCh
	sch := rt.stopCh
	done := rt.writeDone
	if dataCh == nil || controlCh == nil || sch == nil || done == nil {
		return ErrNotConnected
	}
	targetCh := dataCh
	if control {
		targetCh = controlCh
	}
	if !control && req.trackOutbound {
		if err := c.reserveOutbound(req); err != nil {
			return err
		}
		reservedOutbound = true
	}

	select {
	case <-sch:
		return ErrNotConnected
	case <-done:
		return ErrNotConnected
	default:
	}

	select {
	case targetCh <- req:
		return nil
	case <-sch:
		return ErrNotConnected
	case <-done:
		return ErrNotConnected
	}
}

func (c *Client) enqueueWrite(req writeRequest) error {
	return c.enqueueWriteTo(req, false)
}

func (c *Client) enqueueControlWrite(req writeRequest) error {
	return c.enqueueWriteTo(req, true)
}

func acquireWriteErrCh() chan error {
	return writeErrChPool.Get().(chan error)
}

func releaseWriteErrCh(ch chan error) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
	}
	writeErrChPool.Put(ch)
}

func (c *Client) queueWrite(data []byte, deadline time.Time) error {
	return c.queueWriteRequest(writeRequest{
		data:        data,
		deadline:    deadline,
		errCh:       acquireWriteErrCh(),
		pooledErrCh: true,
	}, false)
}

func (c *Client) queuePublishWrite(ctx context.Context, data []byte, deadline time.Time, msg *Message, waitForWrite bool) error {
	dropPayloadSz := len(data)
	dropTopic := ""
	dropQoS := byte(0)
	if msg != nil {
		dropTopic = msg.Topic
		dropQoS = msg.QoS
		dropPayloadSz = len(msg.Payload)
	}

	var errCh chan error
	pooled := false
	if waitForWrite {
		errCh = acquireWriteErrCh()
		pooled = true
	}

	req := writeRequest{
		data:          data,
		deadline:      deadline,
		errCh:         errCh,
		pooledErrCh:   pooled,
		ctx:           ctx,
		trackOutbound: true,
		dropTopic:     dropTopic,
		dropQoS:       dropQoS,
		dropPayloadSz: dropPayloadSz,
	}

	// QoS0 path: enqueue into shared buffered writer and return immediately.
	if !waitForWrite {
		return c.enqueueBufferedQoS0Write(req)
	}

	return c.queueWriteRequest(req, false)
}

func (c *Client) queuePublishRawWrite(ctx context.Context, data []byte, deadline time.Time, topic string, qos byte, payloadSz int, waitForWrite bool) error {
	var errCh chan error
	pooled := false
	if waitForWrite {
		errCh = acquireWriteErrCh()
		pooled = true
	}
	if payloadSz <= 0 {
		payloadSz = len(data)
	}

	req := writeRequest{
		data:          data,
		deadline:      deadline,
		errCh:         errCh,
		pooledErrCh:   pooled,
		ctx:           ctx,
		trackOutbound: true,
		dropTopic:     topic,
		dropQoS:       qos,
		dropPayloadSz: payloadSz,
	}

	if !waitForWrite {
		return c.enqueueBufferedQoS0Write(req)
	}

	return c.queueWriteRequest(req, false)
}

func (c *Client) enqueueBufferedQoS0Write(req writeRequest) (retErr error) {
	if !req.trackOutbound {
		return c.queueWriteRequest(req, false)
	}

	if err := c.reserveOutbound(req); err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			c.releaseOutbound(req)
		}
	}()

	rt := c.loadWriteRuntime()
	if rt == nil || rt.qos0Wake == nil || rt.stopCh == nil || rt.writeDone == nil {
		return ErrNotConnected
	}

	select {
	case <-rt.stopCh:
		return ErrNotConnected
	case <-rt.writeDone:
		return ErrNotConnected
	default:
	}

	c.qos0BufMu.Lock()
	c.qos0Buf = append(c.qos0Buf, req)
	c.qos0BufMu.Unlock()

	select {
	case rt.qos0Wake <- struct{}{}:
	default:
	}

	return nil
}

func (c *Client) popBufferedQoS0Batch(maxItems, maxBytes int) []writeRequest {
	if maxItems <= 0 || maxBytes <= 0 {
		return nil
	}

	c.qos0BufMu.Lock()
	defer c.qos0BufMu.Unlock()
	if len(c.qos0Buf) == 0 {
		return nil
	}

	total := 0
	n := 0
	for n < len(c.qos0Buf) && n < maxItems {
		sz := len(c.qos0Buf[n].data)
		if sz <= 0 {
			sz = 1
		}
		if n > 0 && total+sz > maxBytes {
			break
		}
		total += sz
		n++
		if total >= maxBytes {
			break
		}
	}
	if n == 0 {
		n = 1
	}

	out := c.qos0Buf[:n:n]
	c.qos0Buf = c.qos0Buf[n:]
	return out
}

func (c *Client) hasBufferedQoS0Writes() bool {
	c.qos0BufMu.Lock()
	defer c.qos0BufMu.Unlock()
	return len(c.qos0Buf) > 0
}

func (c *Client) queueControlWrite(data []byte, deadline time.Time) error {
	return c.queueWriteRequest(writeRequest{
		data:        data,
		deadline:    deadline,
		errCh:       acquireWriteErrCh(),
		pooledErrCh: true,
	}, true)
}

func (c *Client) queueWriteRequest(req writeRequest, control bool) error {
	recycleErrCh := func() {
		if req.pooledErrCh {
			releaseWriteErrCh(req.errCh)
		}
	}

	var err error
	if control {
		err = c.enqueueControlWrite(req)
	} else {
		err = c.enqueueWrite(req)
	}
	if err != nil {
		recycleErrCh()
		return err
	}
	if req.errCh == nil {
		return nil
	}

	rt := c.loadWriteRuntime()
	if rt == nil {
		recycleErrCh()
		return ErrNotConnected
	}
	sch := rt.stopCh
	done := rt.writeDone
	if sch == nil || done == nil {
		recycleErrCh()
		return ErrNotConnected
	}

	select {
	case err := <-req.errCh:
		recycleErrCh()
		return err
	case <-done:
		select {
		case err := <-req.errCh:
			recycleErrCh()
			return err
		default:
		}
		recycleErrCh()
		return ErrNotConnected
	case <-sch:
		// Prefer a completed write result if already available.
		select {
		case err := <-req.errCh:
			recycleErrCh()
			return err
		default:
			// writeLoop may still hold a reference to errCh; skip recycle.
		}
		return ErrNotConnected
	}
}

// queueControlWriteNoWait sends a control frame (ack, ping, auth response)
// without waiting for write completion.
// Returns false if the frame could not be enqueued.

func (c *Client) queueControlWriteNoWait(data []byte) (enqueued bool) {
	rt := c.loadWriteRuntime()
	if rt == nil {
		return false
	}
	ch := rt.controlWriteCh
	sch := rt.stopCh
	done := rt.writeDone
	if ch == nil || sch == nil || done == nil {
		return false
	}

	select {
	case <-sch:
		return false
	case <-done:
		return false
	case ch <- writeRequest{data: data}:
		return true
	default:
		return false
	}
}

func (c *Client) isWritePathActive() bool {
	rt := c.loadWriteRuntime()
	if rt == nil {
		return false
	}
	sch := rt.stopCh
	done := rt.writeDone
	if sch == nil || done == nil {
		return false
	}
	select {
	case <-sch:
		return false
	case <-done:
		return false
	default:
		return true
	}
}

func (c *Client) reserveOutbound(req writeRequest) error {
	if c.opts.MaxOutboundPendingMessages <= 0 && c.opts.MaxOutboundPendingBytes <= 0 {
		return nil
	}

	policy := c.opts.OutboundBackpressurePolicy
	if policy == "" {
		policy = OutboundBackpressureBlock
	}
	size := int64(len(req.data))
	if size <= 0 {
		size = 1
	}

	var deadline time.Time
	if policy == OutboundBackpressureBlockWithTimeout {
		if c.opts.OutboundBlockTimeout <= 0 {
			policy = OutboundBackpressureDropNew
		} else {
			deadline = time.Now().Add(c.opts.OutboundBlockTimeout)
		}
	}

	c.outboundMu.Lock()
	defer c.outboundMu.Unlock()

	for !c.canReserveOutboundLocked(size) {
		if req.ctx != nil {
			select {
			case <-req.ctx.Done():
				return req.ctx.Err()
			default:
			}
		}
		c.outboundMu.Unlock()
		active := c.isWritePathActive()
		c.outboundMu.Lock()
		if !active {
			return ErrNotConnected
		}

		switch policy {
		case OutboundBackpressureDropNew:
			c.reportOutboundBackpressureDrop(req)
			return ErrOutboundBackpressure
		case OutboundBackpressureBlockWithTimeout:
			remaining := time.Until(deadline)
			if remaining <= 0 {
				c.reportOutboundBackpressureDrop(req)
				return ErrOutboundBackpressure
			}
			waitFor := remaining
			if waitFor > 100*time.Millisecond {
				waitFor = 100 * time.Millisecond
			}
			timer := time.AfterFunc(waitFor, func() { c.outboundCond.Signal() })
			c.outboundCond.Wait()
			timer.Stop()
		default:
			if req.ctx == nil {
				c.outboundCond.Wait()
			} else {
				timer := time.AfterFunc(100*time.Millisecond, func() { c.outboundCond.Signal() })
				c.outboundCond.Wait()
				timer.Stop()
			}
		}
	}

	c.outboundPendingMessages++
	c.outboundPendingBytes += size
	return nil
}

func (c *Client) reportOutboundBackpressureDrop(req writeRequest) {
	c.reportAsyncError(ErrOutboundBackpressure)

	payloadSz := req.dropPayloadSz
	if payloadSz <= 0 {
		payloadSz = len(req.data)
	}

	c.reportDroppedMessage(&DroppedMessage{
		Direction:   DroppedMessageOutbound,
		Reason:      DroppedReasonOutboundBackpressure,
		Topic:       req.dropTopic,
		QoS:         req.dropQoS,
		PayloadSize: payloadSz,
		Timestamp:   time.Now().UTC(),
	})
}

func (c *Client) canReserveOutboundLocked(size int64) bool {
	if c.opts.MaxOutboundPendingMessages > 0 && c.outboundPendingMessages+1 > c.opts.MaxOutboundPendingMessages {
		return false
	}
	if c.opts.MaxOutboundPendingBytes > 0 && c.outboundPendingBytes+size > c.opts.MaxOutboundPendingBytes {
		return false
	}
	return true
}

func (c *Client) releaseOutbound(req writeRequest) {
	if c.opts.MaxOutboundPendingMessages <= 0 && c.opts.MaxOutboundPendingBytes <= 0 {
		return
	}

	size := int64(len(req.data))
	if size <= 0 {
		size = 1
	}

	c.outboundMu.Lock()
	if c.outboundPendingMessages > 0 {
		c.outboundPendingMessages--
	}
	if c.outboundPendingBytes >= size {
		c.outboundPendingBytes -= size
	} else {
		c.outboundPendingBytes = 0
	}
	c.outboundCond.Signal()
	c.outboundMu.Unlock()
	c.notifyDrain()
}

func (c *Client) writeQueueDepth() int {
	rt := c.loadWriteRuntime()
	if rt == nil {
		return 0
	}
	depth := 0
	if rt.writeCh != nil {
		depth += len(rt.writeCh)
	}
	if rt.controlWriteCh != nil {
		depth += len(rt.controlWriteCh)
	}
	c.qos0BufMu.Lock()
	depth += len(c.qos0Buf)
	c.qos0BufMu.Unlock()
	return depth
}

func (c *Client) outboundPendingDepth() int {
	c.outboundMu.Lock()
	defer c.outboundMu.Unlock()
	return c.outboundPendingMessages
}
