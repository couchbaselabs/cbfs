// +build windows !darwin !freebsd !linux !openbsd !netbsd

package main

import (
	"math"
)

func filesystemFree() (uint64, error) {
	return uint64(math.MaxFloat64), noFSFree
}
