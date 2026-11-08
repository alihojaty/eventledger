package handler

import (
	"context"
	"math/rand/v2"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxRetryBackoff = 1 * time.Second

// RetryUnavailableInterceptor automatically retries unary business RPCs that fail with codes.Unavailable.
// Health probes (/grpc.health.v1.Health/*) are excluded so they fail fast and do not pollute retry metrics.
func RetryUnavailableInterceptor(
	maxAttempts int,
	initialBackoff time.Duration,
	onRetry func(),
	logger *zap.Logger,
) grpc.UnaryClientInterceptor {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if initialBackoff <= 0 {
		initialBackoff = 30 * time.Millisecond
	}

	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if strings.HasPrefix(method, "/grpc.health.v1.Health/") {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		backoff := initialBackoff
		var lastErr error

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			lastErr = invoker(ctx, method, req, reply, cc, opts...)
			if lastErr == nil {
				return nil
			}

			if status.Code(lastErr) != codes.Unavailable || attempt == maxAttempts {
				return lastErr
			}

			if onRetry != nil {
				onRetry()
			}

			jitterMax := max(backoff/2, time.Millisecond)
			sleepDur := backoff + time.Duration(rand.Int64N(int64(jitterMax)))

			if logger != nil {
				logger.Warn("Transient gRPC Unavailable error; retrying idempotent request",
					zap.String("method", method),
					zap.Int("attempt", attempt),
					zap.Duration("backoff", sleepDur),
					zap.Error(lastErr),
				)
			}

			select {
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			case <-time.After(sleepDur):
				backoff *= 2
				if backoff > maxRetryBackoff {
					backoff = maxRetryBackoff
				}
			}
		}

		return lastErr
	}
}
