package main

import (
	"encoding/json"
	"testing"
)

func diagnosticSnapshot(at int64, healthy bool, ack, pending, incoming float64) snapshot {
	queue, _ := json.Marshal(map[string]any{"pending": map[string]any{"current": pending}, "acknowledgments": map[string]any{"ack": ack}})
	protocol, _ := json.Marshal(map[string]any{"mqtt": map[string]any{"messages": map[string]any{"received": incoming}}})
	return snapshot{Timestamp: at, Nodes: []node{{ID: "n", Healthy: healthy, QueueMetrics: queue, ProtocolMetrics: protocol}}, Points: map[string]point{"n": {Time: at}, "all": {Time: at}}}
}
func TestDiagnosticHistoryRatesGaugesAndResets(t *testing.T) {
	c := newCollector(nil, false)
	c.publish(diagnosticSnapshot(10000, true, 100, 9, 200))
	if _, ok := c.current.Points["n"].Metrics["queue.acknowledgments.ack"]; ok {
		t.Fatal("fabricated first-sample rate")
	}
	c.publish(diagnosticSnapshot(12500, true, 125, 7, 250))
	for _, scope := range []string{"n", "all"} {
		m := c.current.Points[scope].Metrics
		if m["queue.acknowledgments.ack"] != 10 || m["queue.pending.current"] != 7 || m["protocol.mqtt.messages.received"] != 20 {
			t.Fatalf("bad gauge or elapsed-time rate: %+v", m)
		}
	}
	c.publish(diagnosticSnapshot(14500, true, 3, 4, 5))
	if _, ok := c.current.Points["all"].Metrics["queue.acknowledgments.ack"]; ok {
		t.Fatal("counter reset rendered as rate")
	}
	c.publish(diagnosticSnapshot(16500, false, 3, 4, 5))
	if len(c.current.Points["all"].Metrics) != 0 {
		t.Fatal("unavailable node contributed metrics")
	}
	c.publish(diagnosticSnapshot(18500, true, 30, 10, 40))
	if _, ok := c.current.Points["n"].Metrics["queue.acknowledgments.ack"]; ok {
		t.Fatal("rate bridged outage")
	}
	if len(c.fullLocked().History["n"]) != 5 {
		t.Fatal("history not replayed on connection")
	}
}
func TestDiagnosticHistoryScopeAndMissingFields(t *testing.T) {
	previous := diagnosticSnapshot(10000, true, 100, 9, 200)
	next := diagnosticSnapshot(12000, true, 120, 12, 240)
	peer := node{ID: "peer", Healthy: false, QueueMetrics: json.RawMessage(`{"pending":{"current":8}}`)}
	next.Nodes = append(next.Nodes, peer)
	addDiagnosticPoints(&next, previous)
	if _, ok := next.Points["all"].Metrics["queue.pending.current"]; ok {
		t.Fatal("incomplete aggregate should be a gap")
	}
	if next.Points["n"].Metrics["queue.pending.current"] != 12 {
		t.Fatal("node scope lost")
	}
	if _, ok := next.Points["all"].Metrics["protocol.amqp.messages.received"]; ok {
		t.Fatal("absent protocol fabricated")
	}
}
