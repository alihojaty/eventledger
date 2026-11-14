package ratelimit

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

const numLimiterShards = 32

// bucket implements a lock-free atomic token bucket using nanosecond credit accounting (GCRA/TAT).
type bucket struct {
	emptyAtNs  atomic.Int64
	lastSeenNs atomic.Int64
}

type limiterShard struct {
	mu          sync.RWMutex
	buckets     map[string]*bucket
	lastCleanup atomic.Int64
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
	nowNs := time.Now().UnixNano()
	l := &KeyRateLimiter{
		rps:   rps,
		burst: float64(burst),
	}
	for i := 0; i < numLimiterShards; i++ {
		l.shards[i].buckets = make(map[string]*bucket)
		l.shards[i].lastCleanup.Store(nowNs)
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
	if rps <= 0 || burst < 1.0 {
		return false
	}

	nowNs := time.Now().UnixNano()
	deltaNs := int64(float64(time.Second) / rps)
	if deltaNs <= 0 {
		deltaNs = 1
	}
	burstWindowNs := int64(burst * float64(deltaNs))

	shard := l.shardFor(bucketKey)

	// Fast path: Read-only lock on shard map
	shard.mu.RLock()
	b, exists := shard.buckets[bucketKey]
	shard.mu.RUnlock()

	if !exists {
		shard.mu.Lock()
		b, exists = shard.buckets[bucketKey]
		if !exists {
			b = &bucket{}
			// Initialize with (burst - 1) tokens remaining after this first request
			b.emptyAtNs.Store(nowNs - burstWindowNs + deltaNs)
			b.lastSeenNs.Store(nowNs)
			shard.buckets[bucketKey] = b
			shard.maybeCleanupLocked(nowNs)
			shard.mu.Unlock()
			return true
		}
		shard.mu.Unlock()
	}

	b.lastSeenNs.Store(nowNs)
	minEmptyAtNs := nowNs - burstWindowNs

	for {
		oldEmptyAt := b.emptyAtNs.Load()
		base := oldEmptyAt
		if base < minEmptyAtNs {
			base = minEmptyAtNs
		}
		newEmptyAt := base + deltaNs
		if newEmptyAt > nowNs {
			return false
		}
		if b.emptyAtNs.CompareAndSwap(oldEmptyAt, newEmptyAt) {
			return true
		}
	}
}

func (s *limiterShard) maybeCleanupLocked(nowNs int64) {
	last := s.lastCleanup.Load()
	if nowNs-last <= int64(5*time.Minute) {
		return
	}
	s.lastCleanup.Store(nowNs)
	expireThreshold := nowNs - int64(10*time.Minute)
	for k, v := range s.buckets {
		if v.lastSeenNs.Load() < expireThreshold {
			delete(s.buckets, k)
		}
	}
}

func (l *KeyRateLimiter) RPS() float64 { return l.rps }
func (l *KeyRateLimiter) Burst() int   { return int(l.burst) }
