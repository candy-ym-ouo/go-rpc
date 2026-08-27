package invoke

import (
	"errors"
	"go-rpc/internal/protocol"
	"testing"
	"time"
)

func TestBackoffAndClassification(t *testing.T) {
	p := RetryPolicy{BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond}
	if p.Backoff(2) != 40*time.Millisecond {
		t.Fatalf("unexpected delay %v", p.Backoff(2))
	}
	if !Retryable(&protocol.RPCError{Status: protocol.StatusUnavailable}, false) {
		t.Fatal("unavailable should retry")
	}
	if Retryable(errors.New("business"), true) {
		t.Fatal("plain business error should not retry")
	}
	if Retryable(ErrSend, false) {
		t.Fatal("non-idempotent send should not retry")
	}
}
