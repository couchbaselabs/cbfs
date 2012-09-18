package main

import (
	"testing"
	"time"
)

func TestNodeAddresses(t *testing.T) {
	testblob := "c4521f18b3e40291db6d4da1948ccc5776198a22"
	tests := []struct {
		in      StorageNode
		expaddr string
		expurl  string
	}{
		{StorageNode{Addr: "1.2.3.4", BindAddr: ":8484"},
			"1.2.3.4:8484",
			"http://1.2.3.4:8484/.cbfs/blob/c4521f18b3e40291db6d4da1948ccc5776198a22",
		},
		{StorageNode{Addr: "1.2.3.4", BindAddr: "5.6.7.8:8484"},
			"5.6.7.8:8484",
			"http://5.6.7.8:8484/.cbfs/blob/c4521f18b3e40291db6d4da1948ccc5776198a22",
		},
	}

	for _, test := range tests {
		if test.in.Address() != test.expaddr {
			t.Errorf("Expected %v for %v, got %v",
				test.expaddr, test.in, test.in.Address())
			t.Fail()
		}
		if test.in.BlobURL(testblob) != test.expurl {
			t.Errorf("Expected %v for %v, got %v",
				test.expurl, test.in, test.in.BlobURL(testblob))
			t.Fail()
		}
	}
}

func TestDateParsing(t *testing.T) {
	tm, err := time.Parse(time.RFC3339,
		"2012-09-17T22:12:09.894702Z")
	if err != nil {
		t.Fatalf("Error parsing time: %v", err)
	}
	exp, got := int64(1347919929894702000), tm.UnixNano()
	if exp != got {
		t.Fatalf("Expected %v, got %v", exp, got)
	}
}
