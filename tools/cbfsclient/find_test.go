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
		{[]string{"-name", "*"},
			[]string{
				"web/site",
				"web/site/file.html",
				"web/site/file2.html",
				"web/site/x",
				"web/site/x/x.html",
				"web/site/thing.png",
				"web/site/robots.txt",
			},
		},
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
		/* soooon
		{
			[]string{"-name", "web"},
			[]string{"web"},
		},
		*/
		{
			[]string{"-name", "x"},
			[]string{"web/site/x"},
		},
	}

	for _, test := range tests {
		findFlags.Parse(test.params)
		matched := []string{}
		matcher := newDirAndFileMatcher()
		for _, fn := range corpus {
			for _, match := range matcher.match(fn) {
				matched = append(matched, match.path)
			}
		}
		if !reflect.DeepEqual(matched, test.exp) {
			t.Errorf("Errored on %v. Expected %v, got %v", test.params,
				test.exp, matched)
		}
	}
}
