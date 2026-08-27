package main
import ("context"; "flag"; "fmt"; "log"; "os"; "os/signal"; "syscall"; "time"; "go-rpc/internal/registry"; "go-rpc/internal/server")
type HelloRequest struct { Name string `json:"name"` }
type HelloReply struct { Message string    `json:"message"`; At      string    `json:"at"` }
type HelloService struct{}
func (*HelloService) SayHello(_ context.Context, req *HelloRequest) (*HelloReply, error) {
	if req.Name == "" { return nil, fmt.Errorf("name is required") }
	return &HelloReply{Message: "Hello, " + req.Name + "!", At: time.Now().Format(time.RFC3339)}, nil
}
func (*HelloService) Echo(ctx context.Context, req *HelloRequest) (*HelloReply, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return &HelloReply{Message: req.Name, At: time.Now().Format(time.RFC3339)}, nil
	}
}
func main() {
	addr := flag.String("addr", ":9001", "RPC listen address")
	service := flag.String("service", "hello", "registered service name")
	weight := flag.Int("weight", 1, "service instance weight")
	ttl := flag.Duration("ttl", 10*time.Second, "registry heartbeat TTL")
	flag.Parse()
	reg := registry.NewStatic()
	srv := server.New(server.Config{Addr: *addr, Registry: reg, Instance: registry.Instance{Service: *service, Addr: *addr, Weight: *weight, TTL: *ttl}})
	if err := srv.Register(&HelloService{}, server.WithServiceName(*service), server.WithMethodConfig("Echo", "timeout=2s,idempotent,codec=json")); err != nil { log.Fatal(err) }
	errCh := make(chan error, 1)
	go func() { log.Printf("go-rpc server listening on %s", *addr); errCh <- srv.ListenAndServe() }()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errCh:
		if err != nil { log.Fatal(err) }
	case <-signals:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil { log.Printf("shutdown: %v", err) }
	}
}
