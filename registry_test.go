package main

import (
	"fmt"
	"testing"
)

func TestServerIDValidation(t *testing.T) {
	tests := map[string]bool{
		"":           false,
		"/":          false,
		"/@thing":    false,
		"@thing":     false,
		"/something": false,
		"master":     true,
		"something":  true,
	}

	for testId, shouldBeOK := range tests {
		err := validateServerId(testId)
		isOK := err == nil
		if shouldBeOK != isOK {
			t.Errorf("Expected ok=%v for %v, got %v (%v)",
				shouldBeOK, testId, isOK, err)
			t.Fail()
		}
	}
}

func TestErrSlice(t *testing.T) {
	tests := []struct {
		e   error
		exp string
	}{
		{&errslice{}, "{Errors: }"},
		{&errslice{fmt.Errorf("a")}, "{Errors: a}"},
		{&errslice{fmt.Errorf("a"),
			fmt.Errorf("b")}, "{Errors: a, b}"},
	}

	for _, tc := range tests {
		if tc.e.Error() != tc.exp {
			t.Errorf("Expected %q, got %q for %v",
				tc.exp, tc.e.Error(), tc.e)
		}
	}
}
