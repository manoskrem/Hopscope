//go:build !linux

// The agent's kernel capture is eBPF/BTF and therefore Linux-only. This stub lets the
// command compile on other platforms (so `go build ./cmd/...` works during development)
// while making the requirement explicit at runtime.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "hopscope-agent requires Linux (eBPF/BTF kernel capture).")
	os.Exit(1)
}
