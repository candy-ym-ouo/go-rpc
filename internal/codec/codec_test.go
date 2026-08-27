package codec

import (
	"reflect"
	"testing"
)

type sample struct {
	Name    string
	Count   int
	Enabled bool
	Values  []int
	Labels  map[string]string
}

func TestBuiltInCodecsRoundTrip(t *testing.T) {
	want := sample{Name: "rpc", Count: 7, Enabled: true, Values: []int{1, 2, 3}, Labels: map[string]string{"env": "test"}}
	for _, name := range []string{"gob", "json", "binary"} {
		t.Run(name, func(t *testing.T) {
			c, err := Get(name)
			if err != nil {
				t.Fatal(err)
			}
			data, err := c.Encode(&want)
			if err != nil {
				t.Fatal(err)
			}
			var got sample
			if err := c.Decode(data, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v want %#v", got, want)
			}
		})
	}
}

func TestJSONRejectsTrailingTopLevelValue(t *testing.T) {
	var got sample
	if err := (JSONCodec{}).Decode([]byte(`{"Name":"first"} {"Name":"second"}`), &got); err == nil {
		t.Fatal("JSON codec accepted a second top-level value")
	}
}
