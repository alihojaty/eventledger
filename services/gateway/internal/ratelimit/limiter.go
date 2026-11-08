package ratelimit

import (
	"hash/fnv"
	"sync"
	"time"
)

const numLimiterShards = 32

type bucket struct {
	tokens     float64
	lastRefill time.Time
}

type limiterShard struct {
	mu          sync.Mutex
	buckets     map[string]*bucket
	lastCleanup time.Time
}

type KeyRateLimiter struct {
	rps    float64
	burst  float64
	shards [numLimiterShards]limiterShard
}

func NewKeyRateLimiter(rps float64, burst int) *KeyRateLimiter {
	if rps <= 0 {
		rps = 100
	}
	if burst <= 0 {
		burst = 100
	}
	now := time.Now()
	l := &KeyRateLimiter{
		rps:   rps,
		burst: float64(burst),
	}
	for i := 0; i < numLimiterShards; i++ {
		l.shards[i].buckets = make(map[string]*bucket)
		l.shards[i].lastCleanup = now
	}
	return l
}

func (l *KeyRateLimiter) shardFor(key string) *limiterShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &l.shards[h.Sum32()%numLimiterShards]
}

func (l *KeyRateLimiter) Allow(apiKey string) bool {
	return l.AllowWithCustomLimit(apiKey, l.rps, l.burst)
}

func (l *KeyRateLimiter) AllowWithCustomLimit(bucketKey string, rps, burst float64) bool {
	now := time.Now()
	shard := l.shardFor(bucketKey)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if now.Sub(shard.lastCleanup) > 5*time.Minute {
		for k, v := range shard.buckets {
			if now.Sub(v.lastRefill) > 10*time.Minute {
				delete(shard.buckets, k)
			}
		}
		shard.lastCleanup = now
	}

	b, exists := shard.buckets[bucketKey]
	if !exists {
		shard.buckets[bucketKey] = &bucket{
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
