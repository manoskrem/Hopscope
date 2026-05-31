package resp

import "testing"

func TestParse(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		wantVerb string
		wantKey  string
		wantOK   bool
	}{
		// ── RESP array form (what redis-cli actually sends) ──────────────────
		{"set", "*3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$2\r\nv1\r\n", "SET", "user:1", true},
		{"get", "*2\r\n$3\r\nGET\r\n$6\r\nuser:1\r\n", "GET", "user:1", true},
		{"del", "*2\r\n$3\r\nDEL\r\n$7\r\norder:9\r\n", "DEL", "order:9", true},
		{"publish", "*3\r\n$7\r\nPUBLISH\r\n$2\r\nch\r\n$3\r\nmsg\r\n", "PUBLISH", "ch", true},
		{"keyless ping", "*1\r\n$4\r\nPING\r\n", "PING", "", true},
		{"no-colon key", "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\nx\r\n", "SET", "foo", true},
		{"lowercase verb upcased", "*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n", "GET", "foo", true},

		// ── inline (telnet-style) fallback ───────────────────────────────────
		{"inline set", "SET user:1 v1\r\n", "SET", "user:1", true},
		{"inline get no crlf", "GET user:1", "GET", "user:1", true},
		{"inline lowercase", "del order:2\r\n", "DEL", "order:2", true},

		// ── truncation: the 256B capture may cut the value or a long key ──────
		{"value truncated", "*3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$10\r\nABC", "SET", "user:1", true},

		// ── pipelined: only the first command is parsed ──────────────────────
		{"pipelined first only", "*2\r\n$3\r\nGET\r\n$1\r\na\r\n*2\r\n$3\r\nGET\r\n$1\r\nb\r\n", "GET", "a", true},

		// ── malformed / empty → not ok ───────────────────────────────────────
		{"empty", "", "", "", false},
		{"bad count", "*abc\r\n", "", "", false},
		{"verb truncated", "*3\r\n$3\r\nSE", "", "", false},
		{"whitespace only", "   \r\n", "", "", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			verb, key, ok := Parse([]byte(c.in))
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v (verb=%q key=%q)", ok, c.wantOK, verb, key)
			}
			if !ok {
				return
			}
			if verb != c.wantVerb {
				t.Errorf("verb = %q, want %q", verb, c.wantVerb)
			}
			if key != c.wantKey {
				t.Errorf("firstKey = %q, want %q", key, c.wantKey)
			}
		})
	}
}

// TestParseNeverReturnsValue is the load-bearing privacy test: whatever the form,
// the parser must never surface the Redis value (arg2+) in either return field.
func TestParseNeverReturnsValue(t *testing.T) {
	const secret = "supersecretvalue"
	inputs := map[string]string{
		"resp":   "*3\r\n$3\r\nSET\r\n$6\r\nuser:1\r\n$16\r\n" + secret + "\r\n",
		"inline": "SET user:1 " + secret + "\r\n",
		// long value spanning what a small capture would include
		"resp big value": "*3\r\n$3\r\nSET\r\n$8\r\norder:42\r\n$16\r\n" + secret + "\r\n",
	}
	for name, in := range inputs {
		t.Run(name, func(t *testing.T) {
			verb, key, ok := Parse([]byte(in))
			if !ok {
				t.Fatalf("expected ok for %q", in)
			}
			if contains(verb, secret) || contains(key, secret) {
				t.Fatalf("value leaked: verb=%q key=%q", verb, key)
			}
		})
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
