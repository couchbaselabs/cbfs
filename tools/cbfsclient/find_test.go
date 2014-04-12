package main

import (
	"reflect"
	"testing"
)

func TestFindMatching(t *testing.T) {
	corpus := []string{
		"web/site/file.html",
		"web/site/file2.html",
		"web/site/x/x.html",
		"web/site/thing.png",
		"web/site/robots.txt",
	}

	tests := []struct {
		params []string
		exp    []string
	}{
		{nil, corpus},
		{[]string{"-name", "*"}, corpus},
		{
			[]string{"-name", "*.png"},
			[]string{"web/site/thing.png"},
		},
		{
			[]string{"-name", "thing.png"},
			[]string{"web/site/thing.png"},
		},
		{
			[]string{"-name", "*.html"},
			[]string{
				"web/site/file.html",
				"web/site/file2.html",
				"web/site/x/x.html",
			},
		},
	}

	for _, test := range tests {
		findFlags.Parse(test.params)
		matched := []string{}
		for _, fn := range corpus {
			if findNameMatches(fn) {
				matched = append(matched, fn)
			}
		}
		if !reflect.DeepEqual(matched, test.exp) {
			t.Errorf("Errored on %v. Expected %v, got %v", test.params,
				test.exp, matched)
		}
	}
}
