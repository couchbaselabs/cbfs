package main

import (
	"testing"
)

func TestNamedLock(t *testing.T) {
	nl := namedLock{}

	if nl.Lock("t1") == false {
		t.Fatalf("Expected initial lock to succeed")
	}

	if nl.Lock("t1") == true {
		t.Fatalf("Expected subsequent lock to fail")
	}

	if nl.Lock("t2") == false {
		t.Fatalf("Expected initial lock (2) to succeed")
	}

	if nl.Lock("t2") == true {
		t.Fatalf("Expected subsequent lock (2) to fail")
	}

	nl.Unlock("t1")
	nl.Unlock("t2")

	defer func() {
		if x := recover(); x == nil {
			t.Fatalf("Expected panic on double free")
		}
	}()
	nl.Unlock("t1")
}
