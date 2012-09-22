package main

import (
	"sort"
	"testing"
	"time"
)

func TestNodeSorting(t *testing.T) {
	now := time.Now()
	nl := NodeList{
		StorageNode{
			name: "older",
			Time: now.Add(time.Minute * -3),
		},
		StorageNode{
			name: "newer",
			Time: now.Add(time.Second * -3),
		},
	}

	sort.Sort(nl)

	if nl[0].Time.UnixNano() < nl[1].Time.UnixNano() {
		t.Fatalf("Error:  wrong order:  %v", nl)
	}
}
