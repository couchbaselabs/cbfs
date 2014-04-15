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
		"web/site/Thing.png",
		"web/site/robots.txt",
	}

	tests := []struct {
		params []string
		exp    []string
	}{
		{nil, corpus},
		{[]string{"-name", "*"},
			[]string{
				"web",
				"web/site",
				"web/site/file.html",
				"web/site/file2.html",
				"web/site/x",
				"web/site/x/x.html",
				"web/site/Thing.png",
				"web/site/robots.txt",
			},
		},
		{[]string{"-name", "*", "-type", "f"},
			[]string{
				"web/site/file.html",
				"web/site/file2.html",
				"web/site/x/x.html",
				"web/site/Thing.png",
				"web/site/robots.txt",
			},
		},
		{[]string{"-name", "*", "-type", "d"},
			[]string{
				"web",
				"web/site",
				"web/site/x",
			},
		},
		{
			[]string{"-name", "*.png"},
			[]string{"web/site/Thing.png"},
		},
		{
			[]string{"-name", "Thing.png"},
			[]string{"web/site/Thing.png"},
		},
		{
			[]string{"-name", "*.html"},
			[]string{
				"web/site/file.html",
				"web/site/file2.html",
				"web/site/x/x.html",
			},
		},
		{
			[]string{"-name", "web"},
			[]string{"web"},
		},
		{
			[]string{"-name", "x"},
			[]string{"web/site/x"},
		},
		{
			[]string{"-name", "thing*"},
			[]string{},
		},
		{
			[]string{"-iname", "thing*"},
			[]string{"web/site/Thing.png"},
		},
	}

	for _, test := range tests {
		*findTemplate = ""
		*findTemplateFile = ""
		*findDashName = ""
		findDashType = findTypeAny

		findFlags.Parse(test.params)
		matched := []string{}
		matcher := newDirAndFileMatcher()
		for _, fn := range corpus {
			for _, match := range matcher.matches(fn) {
				matched = append(matched, match.path)
			}
		}
		if !reflect.DeepEqual(matched, test.exp) {
			t.Errorf("Errored on %v. Expected %v, got %v", test.params,
				test.exp, matched)
		}
	}
}
