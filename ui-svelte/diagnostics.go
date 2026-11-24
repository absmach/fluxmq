package main

import "encoding/json"

// Diagnostic history is collected alongside traffic. Gauges retain their value;
// counters become per-second rates using consecutive observations of the same
// healthy node. Missing fields and counter resets produce gaps, never fake zeroes.
func diagnosticValues(n node) map[string]float64 {
	out := map[string]float64{}
	var walk func(string, any)
	walk = func(prefix string, v any) {
		switch v := v.(type) {
		case map[string]any:
			for key, item := range v {
				walk(prefix+"."+key, item)
			}
		case float64:
			out[prefix] = v
		}
	}
	for prefix, raw := range map[string]json.RawMessage{"queue": n.QueueMetrics, "protocol": n.ProtocolMetrics, "ingress": n.IngressMetrics} {
		var data any
		if json.Unmarshal(raw, &data) == nil {
			walk(prefix, data)
		}
	}
	return out
}
func diagnosticGauge(key string) bool {
	switch key {
	case "queue.pending.current", "queue.pending.high_water", "queue.acknowledgments.poison_pending", "queue.acknowledgments.poison_pending_no_destination":
		return true
	}
	for _, protocol := range []string{"mqtt", "amqp", "amqp1"} {
		for _, suffix := range []string{"connections.current", "channels", "consumers", "sessions", "links", "subscriptions", "local_principals.active_connections"} {
			if key == "protocol."+protocol+"."+suffix {
				return true
			}
		}
	}
	return false
}
func addDiagnosticPoints(s *snapshot, previous snapshot) {
	old := map[string]node{}
	for _, n := range previous.Nodes {
		old[n.ID] = n
	}
	elapsed := float64(s.Timestamp-previous.Timestamp) / 1000
	totals := map[string]float64{}
	contributors := map[string]int{}
	expected := map[string]int{}
	for _, n := range s.Nodes {
		if n.Unmonitored {
			continue
		}
		values := diagnosticValues(n)
		for key := range values {
			expected[key]++
		}
		if !n.Healthy {
			continue
		}
		p, exists := s.Points[n.ID]
		if !exists {
			continue
		}
		p.Metrics = map[string]float64{}
		prior, known := old[n.ID]
		before := diagnosticValues(prior)
		for key, v := range values {
			if !diagnosticGauge(key) {
				last, present := before[key]
				if !known || !prior.Healthy || !present || elapsed <= 0 || v < last {
					continue
				}
				v = (v - last) / elapsed
			}
			p.Metrics[key] = v
			totals[key] += v
			contributors[key]++
		}
		s.Points[n.ID] = p
	}
	all := s.Points["all"]
	all.Metrics = map[string]float64{}
	for key, v := range totals {
		if contributors[key] == expected[key] {
			all.Metrics[key] = v
		}
	}
	s.Points["all"] = all
}
