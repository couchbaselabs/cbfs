package main

import (
	"testing"
)

func TestNodeAddresses(t *testing.T) {
	tests := []struct {
		in  StorageNode
		exp string
	}{
		{StorageNode{Addr: "1.2.3.4", BindAddr: ":8484"},
			"1.2.3.4:8484"},
		{StorageNode{Addr: "1.2.3.4", BindAddr: "5.6.7.8:8484"},
			"5.6.7.8:8484"},
	}

	for _, test := range tests {
		if test.in.Address() != test.exp {
			t.Errorf("Expected %v for %v, got %v",
				test.exp, test.in, test.in.Address())
			t.Fail()
		}
	}
}
