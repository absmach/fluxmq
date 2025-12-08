package broker

import "testing"

func TestRouter(t *testing.T) {
	r := NewRouter()

	sub1 := Subscription{SessionID: "s1", QoS: 1}
	sub2 := Subscription{SessionID: "s2", QoS: 0}
	sub3 := Subscription{SessionID: "s3", QoS: 2}

	r.Subscribe("a/b", sub1)
	r.Subscribe("a/+", sub2)
	r.Subscribe("a/#", sub3)

	tests := []struct {
		topic string
		want  []string // SessionIDs which should match
	}{
		{"a/b", []string{"s1", "s2", "s3"}},
		{"a/c", []string{"s2", "s3"}},
		{"a/b/c", []string{"s3"}},
		// "a/+" does not match "a/b/c". "a/#" matches "a/b/c".
		{"b/c", []string{}},
	}

	for _, tt := range tests {
		matched := r.Match(tt.topic)
		got := make(map[string]bool)
		for _, s := range matched {
			got[s.SessionID] = true
		}

		for _, id := range tt.want {
			if !got[id] {
				t.Errorf("Match(%q) missing %s", tt.topic, id)
			}
		}
		if len(matched) != len(tt.want) {
			t.Errorf("Match(%q) count = %d, want %d", tt.topic, len(matched), len(tt.want))
		}
	}
}
