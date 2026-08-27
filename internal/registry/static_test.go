package registry

import (
	"context"
	"testing"
	"time"
)

func TestStaticLifecycleAndWatch(t *testing.T) {
	r := NewStatic()
	defer r.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := r.Watch(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	<-updates
	inst := Instance{Service: "hello", Addr: "127.0.0.1:1", TTL: time.Second}
	if err := r.Register(ctx, inst); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-updates:
		if len(got) != 1 || !got[0].Healthy {
			t.Fatalf("bad update: %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("watch timeout")
	}
	if err := r.Heartbeat(ctx, inst); err != nil {
		t.Fatal(err)
	}
	if got, _ := r.Discover(ctx, "hello"); len(got) != 1 {
		t.Fatalf("got %d instances", len(got))
	}
	if err := r.Deregister(ctx, inst); err != nil {
		t.Fatal(err)
	}
}

func TestWatchChannelClosesOnCancelAndRegistryClose(t *testing.T) {
	r := NewStatic()
	ctx, cancel := context.WithCancel(context.Background())
	updates, err := r.Watch(ctx, "cancelled")
	if err != nil {
		t.Fatal(err)
	}
	<-updates
	cancel()
	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("watch channel remained open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("watch cancellation did not close channel")
	}
	updates, err = r.Watch(context.Background(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	<-updates
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("watch channel remained open after registry close")
		}
	case <-time.After(time.Second):
		t.Fatal("registry close did not close watch channel")
	}
}

func TestStaticHealthAndWeightUpdatesNotifyWatchers(t *testing.T) {
	r := NewStatic()
	defer r.Close()
	ctx := context.Background()
	updates, err := r.Watch(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	<-updates
	inst := Instance{Service: "hello", Addr: "127.0.0.1:1", TTL: time.Second}
	if err := r.Register(ctx, inst); err != nil {
		t.Fatal(err)
	}
	<-updates
	if err := r.UpdateWeight(ctx, "hello", inst.Addr, 7); err != nil {
		t.Fatal(err)
	}
	if got := <-updates; len(got) != 1 || got[0].Weight != 7 {
		t.Fatalf("weight update was not published: %#v", got)
	}
	if changed := r.ReconcileHealth(time.Now().Add(3 * time.Second)); changed != 1 {
		t.Fatalf("changed=%d, want 1", changed)
	}
	if got := <-updates; len(got) != 1 || got[0].Healthy {
		t.Fatalf("expired health update was not published: %#v", got)
	}
	if err := r.Heartbeat(ctx, inst); err != nil {
		t.Fatal(err)
	}
	if got := <-updates; len(got) != 1 || !got[0].Healthy {
		t.Fatalf("heartbeat health update was not published: %#v", got)
	}
}
