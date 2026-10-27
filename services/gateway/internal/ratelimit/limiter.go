package ratelimit

import (
	"sync"
	"time"
)

type bucket struct {
	tokens     float64
	lastRefill time.Time
}

type KeyRateLimiter struct {
	mu          sync.Mutex
	rps         float64
	burst       float64
	buckets     map[string]*bucket
	lastCleanup time.Time
}

func NewKeyRateLimiter(rps float64, burst int) *KeyRateLimiter {
	if rps <= 0 {
		rps = 100
	}
	if burst <= 0 {
		burst = 100
	}
	return &KeyRateLimiter{
		rps:         rps,
		burst:       float64(burst),
		buckets:     make(map[string]*bucket),
		lastCleanup: time.Now(),
	}
}

func (l *KeyRateLimiter) Allow(apiKey string) bool {
	return l.AllowWithCustomLimit(apiKey, l.rps, l.burst)
}

func (l *KeyRateLimiter) AllowWithCustomLimit(bucketKey string, rps, burst float64) bool {
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	// پاک‌سازی دوره‌ای باکت‌های غیرفعال هر ۵ دقیقه برای جلوگیری از Memory Leak
	if now.Sub(l.lastCleanup) > 5*time.Minute {
		for k, v := range l.buckets {
			if now.Sub(v.lastRefill) > 10*time.Minute {
				delete(l.buckets, k)
			}
		}
		l.lastCleanup = now
	}

	b, exists := l.buckets[bucketKey]
	if !exists {
		l.buckets[bucketKey] = &bucket{
			tokens:     burst - 1,
			lastRefill: now,
		}
		return true
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * rps
		if b.tokens > burst {
			b.tokens = burst
		}
		b.lastRefill = now
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true
	}

	return false
}

func (l *KeyRateLimiter) RPS() float64 { return l.rps }
func (l *KeyRateLimiter) Burst() int   { return int(l.burst) }
