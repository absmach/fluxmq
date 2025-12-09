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

func TestRouterUnsubscribe(t *testing.T) {
	r := NewRouter()

	sub1 := Subscription{SessionID: "s1", QoS: 1}
	sub2 := Subscription{SessionID: "s2", QoS: 0}
	sub3 := Subscription{SessionID: "s3", QoS: 2}

	r.Subscribe("a/b", sub1)
	r.Subscribe("a/b", sub2)
	r.Subscribe("a/+", sub3)

	// Before unsubscribe: s1, s2, s3 should match "a/b"
	matched := r.Match("a/b")
	if len(matched) != 3 {
		t.Errorf("Before unsubscribe: Match(\"a/b\") count = %d, want 3", len(matched))
	}

	// Unsubscribe s1 from "a/b"
	r.Unsubscribe("a/b", "s1")

	// After unsubscribe: only s2 and s3 should match
	matched = r.Match("a/b")
	got := make(map[string]bool)
	for _, s := range matched {
		got[s.SessionID] = true
	}

	if got["s1"] {
		t.Errorf("After unsubscribe: s1 should not match \"a/b\"")
	}
	if !got["s2"] {
		t.Errorf("After unsubscribe: s2 should still match \"a/b\"")
	}
	if !got["s3"] {
		t.Errorf("After unsubscribe: s3 should still match \"a/b\"")
	}
	if len(matched) != 2 {
		t.Errorf("After unsubscribe: Match(\"a/b\") count = %d, want 2", len(matched))
	}

	// Unsubscribe from non-existent filter should not error
	r.Unsubscribe("x/y/z", "s1")

	// Unsubscribe non-existent session should not error
	r.Unsubscribe("a/+", "nonexistent")

	// s3 should still match "a/b"
	matched = r.Match("a/b")
	if len(matched) != 2 {
		t.Errorf("After non-existent unsubscribes: Match(\"a/b\") count = %d, want 2", len(matched))
	}
}
