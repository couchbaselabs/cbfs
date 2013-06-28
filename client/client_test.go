package cbfsclient

import (
	"testing"
)

func TestPathGen(t *testing.T) {
	c, err := New("http://cbfs:8484/")
	if err != nil {
		t.Fatalf("Error parsing thing: %v", err)
	}

	tests := map[string]string{
		"":    "http://cbfs:8484/",
		"a":   "http://cbfs:8484/a",
		"/a":  "http://cbfs:8484/a",
		"//a": "http://cbfs:8484/a",
	}

	for i, exp := range tests {
		p := c.URLFor(i)
		if p != exp {
			t.Errorf("Expected %q for %q, got %q",
				exp, i, p)
		}
	}
}
