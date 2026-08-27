package main
import ("context"; "flag"; "fmt"; "log"; "net/http"; "time"; "go-rpc/internal/discovery"; "go-rpc/internal/invoke"; "go-rpc/internal/pool"; "go-rpc/internal/registry"; "go-rpc/pkg/monitor")
type HelloRequest struct { Name string `json:"name"` }
type HelloReply struct { Message string    `json:"message"`; At      string    `json:"at"` }
func main() {
	target := flag.String("target", "127.0.0.1:9001", "RPC server address")
	webAddr := flag.String("web", ":8080", "monitor HTTP address")
	name := flag.String("name", "go-rpc", "name sent to SayHello")
	codecName := flag.String("codec", "gob", "gob, json, or binary")
	flag.Parse()
	ctx := context.Background()
	reg := registry.NewStatic()
	inst := registry.Instance{Service: "hello", Addr: *target, Weight: 1, TTL: time.Hour, Healthy: true}
	if err := reg.Register(ctx, inst); err != nil { log.Fatal(err) }
	disc := discovery.New(reg)
	if err := disc.Start(ctx, "hello"); err != nil { log.Fatal(err) }
	defer disc.Close()
	config := invoke.Config{DefaultTimeout: 2 * time.Second, DefaultCodec: *codecName, Selector: "roundrobin", Retry: invoke.DefaultRetryPolicy(), Pool: pool.DefaultConfig()}
	client, err := invoke.NewClient(config, disc)
	if err != nil { log.Fatal(err) }
	defer client.Close()
	var reply HelloReply
	if err := client.Call(ctx, "hello", "SayHello", &HelloRequest{Name: *name}, &reply, invoke.WithIdempotent(true)); err != nil {
		log.Printf("call failed: %v", err)
	} else { fmt.Printf("reply: %s (%s)\n", reply.Message, reply.At) }
	ui := monitor.NewHTTPServer(monitor.DataSource{Services: func() any { return map[string]any{"services": disc.Services()} }, Pools: func() any { return client.PoolStats() }, Config: func() any { return client.ConfigSnapshot() }, SetSelector: client.SetSelector, Metrics: monitor.Default})
	log.Printf("monitor listening on http://%s", *webAddr)
	if err := http.ListenAndServe(*webAddr, ui.Handler()); err != nil { log.Fatal(err) }
}
