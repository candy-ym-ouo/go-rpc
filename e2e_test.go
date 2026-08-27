package gorpc_test

import (
	"context"
	"go-rpc/internal/discovery"
	"go-rpc/internal/invoke"
	"go-rpc/internal/pool"
	"go-rpc/internal/registry"
	rpcserver "go-rpc/internal/server"
	"net"
	"testing"
	"time"
)

type helloRequest struct{ Name string }
type helloReply struct{ Message string }
type helloService struct{}

func (*helloService) SayHello(_ context.Context, req *helloRequest) (*helloReply, error) {
	return &helloReply{Message: "Hello, " + req.Name}, nil
}

func TestEndToEndAllCodecs(t *testing.T) {
	for _, codecName := range []string{"gob", "json", "binary"} {
		t.Run(codecName, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			reg := registry.NewStatic()
			srv := rpcserver.New(rpcserver.Config{Registry: reg, Instance: registry.Instance{Service: "hello", Addr: ln.Addr().String(), TTL: time.Minute}})
			if err := srv.Register(&helloService{}, rpcserver.WithServiceName("hello")); err != nil {
				t.Fatal(err)
			}
			serveErr := make(chan error, 1)
			go func() { serveErr <- srv.Serve(ln) }()
			deadline := time.Now().Add(time.Second)
			for {
				items, _ := reg.Discover(context.Background(), "hello")
				if len(items) > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("server registration timeout")
				}
				time.Sleep(time.Millisecond)
			}
			disc := discovery.New(reg)
			if err := disc.Start(context.Background(), "hello"); err != nil {
				t.Fatal(err)
			}
			client, err := invoke.NewClient(invoke.Config{DefaultTimeout: time.Second, DefaultCodec: codecName, Selector: "roundrobin", Retry: invoke.DefaultRetryPolicy(), Pool: pool.DefaultConfig()}, disc)
			if err != nil {
				t.Fatal(err)
			}
			var reply helloReply
			if err := client.Call(context.Background(), "hello", "SayHello", &helloRequest{Name: "Go"}, &reply, invoke.WithIdempotent(true)); err != nil {
				t.Fatal(err)
			}
			if reply.Message != "Hello, Go" {
				t.Fatalf("unexpected reply %q", reply.Message)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			started := time.Now()
			if err := srv.Shutdown(ctx); err != nil {
				t.Fatalf("shutdown with idle connection: %v", err)
			}
			cancel()
			if time.Since(started) > 500*time.Millisecond {
				t.Fatal("shutdown waited for the deadline despite an idle connection")
			}
			_ = client.Close()
			_ = disc.Close()
			if err := <-serveErr; err != nil {
				t.Fatal(err)
			}
		})
	}
}
