package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type RecoveryRepository interface {
	RecoverExpired(context.Context) (int64, error)
}

// RunRecovery is process-owned and joined alongside the HTTP server and workers.
// The caller performs the initial recovery before accepting work.
func RunRecovery(ctx context.Context, repository RecoveryRepository, interval time.Duration, logger *slog.Logger) error {
	if interval <= 0 {
		return errors.New("recovery interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			count, err := repository.RecoverExpired(ctx)
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				logger.ErrorContext(ctx, "recover expired tasks failed; retrying", "error", err)
			} else if count > 0 {
				logger.Info("expired tasks recovered", "count", count)
			}
		}
	}
}
