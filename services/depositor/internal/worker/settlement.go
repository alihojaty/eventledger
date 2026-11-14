package worker

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type DepositSettler interface {
	SettlePendingDeposits(ctx context.Context) (int64, error)
}

type SettlementWorker struct {
	settler  DepositSettler
	interval time.Duration
	logger   *zap.Logger

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewSettlementWorker(settler DepositSettler, interval time.Duration, logger *zap.Logger) *SettlementWorker {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &SettlementWorker{
		settler:  settler,
		interval: interval,
		logger:   logger,
		stopCh:   make(chan struct{}),
	}
}

func (sw *SettlementWorker) Start(ctx context.Context) {
	sw.wg.Add(1)
	go func() {
		defer sw.wg.Done()

		ticker := time.NewTicker(sw.interval)
		defer ticker.Stop()

		sw.logger.Info("Periodic PL/pgSQL batch settlement worker started",
			zap.Duration("settlement_interval", sw.interval),
		)

		for {
			select {
			case <-ctx.Done():
				return
			case <-sw.stopCh:
				return
			case <-ticker.C:
				sw.runCycle(ctx)
			}
		}
	}()
}

func (sw *SettlementWorker) runCycle(ctx context.Context) {
	start := time.Now()
	settledCount, err := sw.settler.SettlePendingDeposits(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		sw.logger.Error("Periodic batch settlement cycle failed",
			zap.Error(err),
			zap.Duration("duration", time.Since(start)),
		)
		return
	}

	if settledCount > 0 {
		sw.logger.Info("Periodic batch settlement cycle completed",
			zap.Int64("settled_deposits", settledCount),
			zap.Duration("duration", time.Since(start)),
		)
	}
}

// StopAndFlush stops the periodic ticker and executes one final settlement pass
// so all committed deposits are settled into user balances before shutdown.
func (sw *SettlementWorker) StopAndFlush(ctx context.Context) {
	sw.stopOnce.Do(func() {
		close(sw.stopCh)
	})
	sw.wg.Wait()

	start := time.Now()
	settledCount, err := sw.settler.SettlePendingDeposits(ctx)
	if err != nil {
		sw.logger.Warn("Final settlement flush on shutdown encountered an error",
			zap.Error(err),
		)
		return
	}
	if settledCount > 0 {
		sw.logger.Info("Final settlement flush completed on shutdown",
			zap.Int64("settled_deposits", settledCount),
			zap.Duration("duration", time.Since(start)),
		)
	}
}
