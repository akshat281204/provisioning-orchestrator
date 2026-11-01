package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/akshat281204/provisioning-orchestrator/internal/store"
)

// heartbeatInterval is the lease renewal period: LeaseDuration/3. Renewing
// three times per lease means a slow-but-healthy step keeps its lease alive —
// an interval of a full lease would be a coin flip on where renewal lands
// relative to expiry, and anything slower is guaranteed theft.
func heartbeatInterval(lease time.Duration) time.Duration {
	h := lease / 3
	// Floor so a very short (test) lease never becomes a renewal hot loop.
	if h < 100*time.Millisecond {
		h = 100 * time.Millisecond
	}
	return h
}

// startHeartbeat renews the step's lease every LeaseDuration/3 until the
// execution context expires or the lease is lost (ErrNotFound — another
// worker reclaimed it; renewing further is pointless). The returned stop
// function blocks until the heartbeat goroutine has fully stopped, so lease
// state is quiescent before the outcome is recorded.
func (p *Pool) startHeartbeat(ctx context.Context, owner, stepID string, log *slog.Logger) (stop func()) {
	t := time.NewTicker(heartbeatInterval(p.LeaseDuration))
	done := make(chan struct{})
	stopCh := make(chan struct{})
	go func() {
		defer close(done)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-t.C:
				err := p.Store.RenewLease(ctx, stepID, owner, p.LeaseDuration)
				switch {
				case errors.Is(err, store.ErrNotFound):
					// The lease was lost (stolen or completed elsewhere):
					// stop renewing — there is nothing left to extend.
					log.Warn("worker lost the lease, stopping heartbeat",
						"step_id", stepID, "err", err)
					return
				case err != nil:
					// Transient store failure: the next tick retries.
					log.Warn("worker lease renewal failed",
						"step_id", stepID, "err", err)
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stopCh)
			t.Stop()
			<-done
		})
	}
}
