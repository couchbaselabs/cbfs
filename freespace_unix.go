// +build !windows

package main

import (
	"syscall"
)

func filesystemFree() (int64, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(*root, &fs)
	return int64(fs.Bfree) * int64(fs.Bsize), err
}
