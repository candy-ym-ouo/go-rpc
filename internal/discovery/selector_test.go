package discovery

import (
	"go-rpc/internal/registry"
	"testing"
)

func TestSelectors(t *testing.T) {
	items := []registry.Instance{{Addr: "a", Weight: 1, Healthy: true}, {Addr: "b", Weight: 3, Healthy: true}}
	for _, name := range []string{"random", "roundrobin", "weighted", "hash"} {
		s, err := NewSelector(name)
		if err != nil {
			t.Fatal(err)
		}
		first, err := s.Select(items, "key")
		if err != nil {
			t.Fatal(err)
		}
		if first.Addr == "" {
			t.Fatal("empty selection")
		}
		if name == "hash" {
			again, _ := s.Select(items, "key")
			if again.Addr != first.Addr {
				t.Fatal("hash selection is unstable")
			}
		}
	}
}
