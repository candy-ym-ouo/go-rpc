package server

import (
	"context"

	"go-rpc/internal/codec"
	"go-rpc/internal/protocol"
	"testing"
	"time"
)

type validRequest struct{ Value string }
type validReply struct{ Value string }
type validService struct{}

func (*validService) Echo(_ context.Context, req *validRequest) (*validReply, error) {
	return &validReply{Value: req.Value}, nil
}

type invalidService struct{}

func (*invalidService) Bad(value string) string { return value }

func TestDescribeAndMethodConfiguration(t *testing.T) {
	desc, err := describe(&validService{}, WithServiceName("echo"), WithMethodConfig("Echo", "timeout=25ms,retry=2,idempotent,codec=json"))
	if err != nil {
		t.Fatal(err)
	}
	method := desc.Methods["Echo"]
	if method == nil || method.Config.Timeout != 25*time.Millisecond || method.Config.Retry != 2 || !method.Config.Idempotent || method.Config.Codec != "json" {
		t.Fatalf("unexpected method description: %#v", method)
	}
	if _, err := describe(&invalidService{}); err == nil {
		t.Fatal("invalid method signature accepted")
	}
}

type concreteContext struct{ context.Context }
type concreteError string

func (e concreteError) Error() string { return string(e) }

type concreteContextService struct{}

func (*concreteContextService) Bad(_ concreteContext, _ *validRequest) (*validReply, error) {
	return nil, nil
}

type concreteErrorService struct{}

func (*concreteErrorService) Bad(_ context.Context, _ *validRequest) (*validReply, concreteError) {
	return nil, "bad"
}

type nilResponseService struct{}

func (*nilResponseService) Empty(_ context.Context, _ *validRequest) (*validReply, error) {
	return nil, nil
}

func TestDescribeRejectsReflectUnsafeSignatures(t *testing.T) {
	if _, err := describe(&concreteContextService{}); err == nil {
		t.Fatal("concrete context parameter accepted")
	}
	if _, err := describe(&concreteErrorService{}); err == nil {
		t.Fatal("concrete error return accepted")
	}
}

func TestDispatchRejectsNilResponse(t *testing.T) {
	s := New(Config{})
	if err := s.Register(&nilResponseService{}, WithServiceName("nil")); err != nil {
		t.Fatal(err)
	}
	c, err := codec.Get("json")
	if err != nil {
		t.Fatal(err)
	}
	body, err := c.Encode(&validRequest{Value: "x"})
	if err != nil {
		t.Fatal(err)
	}
	response := s.dispatch(context.Background(), protocol.NewRequest(1, codec.JSONID, protocol.Header{Service: "nil", Method: "Empty"}, body))
	if response.Status != protocol.StatusServerError {
		t.Fatalf("got status %v", response.Status)
	}
}
