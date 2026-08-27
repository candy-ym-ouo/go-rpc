package invoke
import ("context"; "errors"; "math/rand"; "net"; "time"; "go-rpc/internal/pool"; "go-rpc/internal/protocol")
var (ErrDeadlineExceeded = errors.New("rpc deadline exceeded"); ErrSend             = errors.New("rpc send failed"); ErrReadTimeout      = errors.New("rpc read timeout"))
type RetryPolicy struct { MaxRetries int; BaseDelay  time.Duration; MaxDelay   time.Duration; Jitter     float64 }
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxRetries: 2, BaseDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second, Jitter: 0.25}
}
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt < 0 { attempt = 0 }
	base := p.BaseDelay
	if base <= 0 { base = 100 * time.Millisecond }
	maxDelay := p.MaxDelay
	if maxDelay <= 0 { maxDelay = 2 * time.Second }
	delay := base
	for i := 0; i < attempt && delay < maxDelay/2; i++ { delay *= 2 }
	if delay > maxDelay { delay = maxDelay }
	if p.Jitter > 0 {
		factor := 1 + (rand.Float64()*2-1)*p.Jitter
		delay = time.Duration(float64(delay) * factor)
	}
	if delay < 0 { return 0 }
	return delay
}
func Retryable(err error, idempotent bool) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrDeadlineExceeded) { return false }
	if errors.Is(err, pool.ErrPoolTimeout) { return true }
	if errors.Is(err, ErrSend) || errors.Is(err, ErrReadTimeout) { return idempotent }
	var rpcErr *protocol.RPCError
	if errors.As(err, &rpcErr) { return rpcErr.Status == protocol.StatusServerError || rpcErr.Status == protocol.StatusTimeout || rpcErr.Status == protocol.StatusUnavailable }
	var netErr net.Error
	return errors.As(err, &netErr)
}
func Remaining(ctx context.Context, fallback time.Duration) (time.Duration, error) {
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 { return 0, ErrDeadlineExceeded }
		if fallback <= 0 || remaining < fallback { return remaining, nil }
	}
	if fallback <= 0 { fallback = 2 * time.Second }
	return fallback, nil
}
func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
