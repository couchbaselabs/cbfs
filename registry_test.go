package main

import (
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
