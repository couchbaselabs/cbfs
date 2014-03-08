package main

import (
	"math"
)

func filesystemFree() (int64, error) {
	return math.MaxInt64, noFSFree
}
