package main

import (
	"strings"
	"testing"
)

const goodPatterns = `
*.[oa]
/x
/a/b/c
#x
.git
`

func TestPatterns(t *testing.T) {
	err := loadIgnorePatterns(strings.NewReader(goodPatterns))
	if err != nil {
		t.Fatalf("Error loading patterns: %v", err)
	}

	tests := []struct {
		path string
		exp  bool
	}{
		{"thing.c", false},
		{"thing.o", true},
		{"/a/c", false},
		{"/a/b/c", true},
		{"a/b/c", true},
		{"/a/x", false},
		{"#x", false}, // # is a comment
		{"x", true},
		{"/a/b/c/.git", true},
	}

	for _, test := range tests {
		if isIgnored(test.path) != test.exp {
			t.Errorf("Expected %v for %v", test.exp, test.path)
			t.Fail()
		}
	}
}
