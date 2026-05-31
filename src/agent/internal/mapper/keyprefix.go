package mapper

import "strings"

// KeyPrefix truncates a colon-namespaced Redis key to its first depth segments and
// appends ":*", bounding both cardinality and data exposure (the id segment of a key
// like "user:123" is high-cardinality and often PII).
//
// This is a byte-for-byte port of the engine's RedisMapper.KeyPrefix
// (src/engine/Hopscope.Infrastructure/Providers/Redis/RedisMapper.cs) so an
// agent-sourced Redis node lands on the exact same Topic as the in-proc provider:
//
//	("user:123", 1)       → "user:*"
//	("order:456:item", 1) → "order:*"
//	("a:b:c", 2)          → "a:b:*"
//	("foo", 1)            → "keys:*"   (no colon → sentinel)
//	("", 1)              → "keys:*"
func KeyPrefix(key string, depth int) string {
	if key == "" {
		return "keys:*"
	}
	if depth < 1 {
		depth = 1
	}

	segment := 0
	pos := 0
	for pos < len(key) {
		idx := strings.IndexByte(key[pos:], ':')
		if idx < 0 {
			break // no more colons
		}
		segment++
		pos += idx + 1 // move past the colon
		if segment >= depth {
			return key[:pos] + "*" // up-to-and-including the colon, plus "*"
		}
	}

	// Fewer colons than depth (including zero colons) → sentinel.
	return "keys:*"
}
