package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	want := NewRequest(42, 1, Header{Service: "hello", Method: "Echo", TraceID: "abc", TimeoutMS: 500}, []byte("body"))
	data, err := EncodeMessage(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMessage(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if got.ReqID != want.ReqID || got.Header.Service != "hello" || string(got.Body) != "body" {
		t.Fatalf("unexpected message: %#v", got)
	}
}
func TestRejectBadMagicAndLargeBody(t *testing.T) {
	data := make([]byte, FixedHeaderLen)
	binary.BigEndian.PutUint16(data[:2], 0x1234)
	data[2] = Version
	if _, err := DecodeMessage(bytes.NewReader(data)); err == nil {
		t.Fatal("bad magic accepted")
	}
	binary.BigEndian.PutUint16(data[:2], Magic)
	binary.BigEndian.PutUint32(data[19:23], MaxBodyLen+1)
	if _, err := DecodeMessage(bytes.NewReader(data)); err == nil {
		t.Fatal("large body accepted")
	}
}
