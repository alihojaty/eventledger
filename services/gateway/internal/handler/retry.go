package handler

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryUnavailableInterceptor automatically retries unary RPCs that fail with codes.Unavailable.
// Safe because PublishDeposit is strictly idempotent via event_id = Idempotency-Key.
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
			if logger != nil {
				logger.Warn("Transient gRPC Unavailable error; retrying idempotent request",
					zap.String("method", method),
					zap.Int("attempt", attempt),
					zap.Duration("backoff", backoff),
					zap.Error(lastErr),
				)
			}

			select {
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}

		return lastErr
	}
}
