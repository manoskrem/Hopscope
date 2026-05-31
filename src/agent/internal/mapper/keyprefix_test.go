package mapper

import "testing"

// TestKeyPrefix locks the port to the engine's RedisMapper.KeyPrefix semantics so an
// agent-sourced Redis node lands on the exact same Topic as the in-proc provider.
func TestKeyPrefix(t *testing.T) {
	cases := []struct {
		key   string
		depth int
		want  string
	}{
		{"user:123", 1, "user:*"},
		{"order:456:item", 1, "order:*"},
		{"a:b:c", 2, "a:b:*"},
		{"a:b", 1, "a:*"},
		{"foo", 1, "keys:*"},    // no colon → sentinel
		{"", 1, "keys:*"},       // empty → sentinel
		{"user:1", 0, "user:*"}, // depth < 1 clamps to 1
		{":x", 1, ":*"},         // leading colon
		{"user:1", 5, "keys:*"}, // fewer colons than depth → sentinel
	}
	for _, c := range cases {
		if got := KeyPrefix(c.key, c.depth); got != c.want {
			t.Errorf("KeyPrefix(%q, %d) = %q, want %q", c.key, c.depth, got, c.want)
		}
	}
}
