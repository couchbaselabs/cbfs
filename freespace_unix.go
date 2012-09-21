// +build !windows

package main

import (
	"syscall"
)

func filesystemFree() (uint64, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(*root, &fs)
	return fs.Bfree * uint64(fs.Bsize), err
}
