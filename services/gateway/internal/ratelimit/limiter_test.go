package ratelimit

import (
	"testing"
	"time"
)

func TestKeyRateLimiter_PerAPIKeyIsolation(t *testing.T) {
	limiter := NewKeyRateLimiter(10, 3) // 10 tokens/sec, burst of 3

	// Key A consumes its burst of 3
	for i := 1; i <= 3; i++ {
		if !limiter.Allow("key-A") {
			t.Fatalf("expected request %d for key-A to be allowed", i)
		}
	}
	// 4th immediate request for Key A must be rejected
	if limiter.Allow("key-A") {
		t.Fatalf("expected 4th immediate request for key-A to be rate-limited")
	}

	// Key B has its own independent bucket and must be allowed
	if !limiter.Allow("key-B") {
		t.Fatalf("expected key-B to be allowed independently of key-A")
	}

	// Wait 120ms for Key A to refill at least 1 token (10 RPS = 1 token per 100ms)
	time.Sleep(120 * time.Millisecond)
	if !limiter.Allow("key-A") {
		t.Fatalf("expected key-A to be allowed after token refill")
	}
}
