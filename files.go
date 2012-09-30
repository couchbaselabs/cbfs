package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

var maxStorageString = flag.String("maxSize", "",
	"Approximate maximum amount of space to allocate")
var maxStorage uint64

var spaceUsed int64
var firstReconcile = true

func storedObject(h string, l int64) {
	atomic.AddInt64(&spaceUsed, l)
}

func removedObject(h string, l int64) {
	atomic.AddInt64(&spaceUsed, -l)
	removeBlobOwnershipRecord(h, serverId)
}

func removeObject(h string) error {
	fn := hashFilename(*root, h)
	fi, err := os.Stat(fn)
	if err != nil {
		return err
	}
	err = os.Remove(fn)
	if err == nil {
		removedObject(h, fi.Size())
	}
	return err
}

func verifyObjectHash(h string) error {
	fn := hashFilename(*root, h)
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer f.Close()

	sh := getHash()
	_, err = io.Copy(sh, f)
	if err != nil {
		return err
	}

	hstring := hex.EncodeToString(sh.Sum([]byte{}))
	if h != hstring {
		err = removeObject(h)
		if err != nil {
			log.Printf("Error removing corrupt file %v: %v", h, err)
		}
		return fmt.Errorf("Hash from disk of %v was %v", h, hstring)
	}
	return nil
}

func verifyWorker(ch chan os.FileInfo, first bool) {
	for info := range ch {
		err := verifyObjectHash(info.Name())
		if err == nil {
			recordBlobOwnership(info.Name(), info.Size(), false)
			if first {
				storedObject(info.Name(), info.Size())
			}
		} else {
			log.Printf("Invalid hash for object %v found at verification: %v",
				info.Name(), err)
			removeBlobOwnershipRecord(info.Name(), serverId)
		}
	}
}

func reconcile() error {
	explen := getHash().Size() * 2

	vch := make(chan os.FileInfo)
	defer close(vch)

	for i := 0; i < *verifyWorkers; i++ {
		go verifyWorker(vch, firstReconcile)
	}
	firstReconcile = false

	return filepath.Walk(*root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && !strings.HasPrefix(info.Name(), "tmp") &&
			len(info.Name()) == explen {

			vch <- info

			return err
		}
		return nil
	})
}
