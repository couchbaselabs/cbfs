package main

import (
	"reflect"
	"testing"
	"time"
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

func TestRefTimeMatcher(t *testing.T) {
	tp := func(s string) time.Time {
		rv, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatalf("Error parsing test date: %v - %v", s, err)
		}
		return rv
	}
	now := tp("2014-04-15T17:21:23.938485Z")

	tests := []struct {
		ds      string
		d       time.Duration
		matches bool
	}{
		// zeros
		{"2014-04-14T17:21:23.938485Z", 0, true},
		{"2014-04-15T17:21:23.938485Z", 0, true},
		{"2014-04-16T17:21:23.938485Z", 0, true},

		// Negative
		{"2014-04-14T17:21:23.938485Z", -time.Hour, true},
		{"2014-04-15T17:21:23.938485Z", -time.Hour, false},
		{"2014-04-16T17:21:23.938485Z", -time.Hour, false},

		// Positive
		{"2014-04-14T17:21:23.938485Z", time.Hour, false},
		{"2014-04-15T17:21:23.938485Z", time.Hour, true},
		{"2014-04-16T17:21:23.938485Z", time.Hour, true},
	}

	for _, test := range tests {
		*findDashMTime = test.d
		g := findGetRefTimeMatch(now)
		if g(tp(test.ds)) != test.matches {
			t.Errorf("Error on %v/%v got %v, expected %v",
				test.ds, test.d, g(tp(test.ds)), test.matches)
		}
	}
}
