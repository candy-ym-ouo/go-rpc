package pool

import (
	"context"
	"net"
	"testing"
)

func testListener(t *testing.T)(net.Listener,chan struct{}){t.Helper();ln,err:=net.Listen("tcp","127.0.0.1:0");if err!=nil{t.Fatal(err)};done:=make(chan struct{});go func(){defer close(done);for{conn,err:=ln.Accept();if err!=nil{return};go func(){defer conn.Close();buf:=make([]byte,1);for{if _,err:=conn.Read(buf);err!=nil{return}}}()}}();return ln,done}
func TestGetPutReusesConnection(t *testing.T){ln,done:=testListener(t);cfg:=DefaultConfig();cfg.Max=1;cfg.MaxIdle=1;p,err:=New(ln.Addr().String(),cfg);if err!=nil{t.Fatal(err)};first,err:=p.Get(context.Background());if err!=nil{t.Fatal(err)};if err:=p.Put(first);err!=nil{t.Fatal(err)};second,err:=p.Get(context.Background());if err!=nil{t.Fatal(err)};if first!=second{t.Fatal("pool did not reuse the idle connection")};p.Discard(second);if got:=p.Stats();got.Active!=0||got.TotalCreated!=1{t.Fatalf("unexpected stats: %#v",got)};_ = p.Close();_ = ln.Close();<-done}
