package main

import (
	"testing"
)

func TestMinusPrefix(t *testing.T) {
	aPath := "/.cbfs/blob/x"
	if minusPrefix(aPath, blobPrefix) != "x" {
		t.Fatalf(`Expected "x", got %#v"`,
			minusPrefix(aPath, blobPrefix))
	}
}
