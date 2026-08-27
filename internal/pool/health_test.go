package pool

import (
	"context"
	"testing"
	"time"
)

func TestHealthLoopReapsExpiredIdleConnection(t *testing.T) {
	ln, done := testListener(t)
	cfg := DefaultConfig()
	cfg.IdleTimeout = 15 * time.Millisecond
	cfg.HealthPeriod = 5 * time.Millisecond
	p, err := New(ln.Addr().String(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Put(conn); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for p.Stats().Idle != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := p.Stats(); got.Idle != 0 || got.Active != 0 {
		t.Fatalf("expired connection not reaped: %#v", got)
	}
	_ = p.Close()
	_ = ln.Close()
	<-done
}

func TestReapExpiredDoesNotCloseKeptConnection(t *testing.T) {
	ln, done := testListener(t)
	cfg := DefaultConfig()
	cfg.Max, cfg.MaxIdle = 2, 2
	cfg.IdleTimeout = 40 * time.Millisecond
	cfg.HealthPeriod = time.Hour
	p, err := New(ln.Addr().String(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	oldConn, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	freshConn, err := p.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Put(oldConn); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := p.Put(freshConn); err != nil {
		t.Fatal(err)
	}
	p.reapExpired()
	if !freshConn.Alive() {
		t.Fatal("idle compaction closed the healthy connection")
	}
	if got := p.Stats(); got.Idle != 1 || got.Active != 1 {
		t.Fatalf("unexpected stats: %#v", got)
	}
	_ = p.Close()
	_ = ln.Close()
	<-done
}
