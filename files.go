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
)

var maxStorageString = flag.String("maxSize", "",
	"Approximate maximum amount of space to allocate")
var maxStorage uint64

func hashFilename(base, hstr string) string {
	return base + "/" + hstr[:2] + "/" + hstr
}

type ReadSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

func openBlob(hstr string) (ReadSeekCloser, error) {
	return os.Open(hashFilename(*root, hstr))
}

func removeObject(h string) error {
	err := maybeRemoveBlobOwnership(h)
	if err == nil {
		err = os.Remove(hashFilename(*root, h))
	}
	return err
}

func forceRemoveObject(h string) error {
	removeBlobOwnershipRecord(h, serverId)
	return os.Remove(hashFilename(*root, h))
}

func verifyObjectHash(h string) error {
	f, err := openBlob(h)
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
		err = forceRemoveObject(h)
		if err != nil {
			log.Printf("Error removing corrupt file %v: %v", h, err)
		}
		return fmt.Errorf("Hash from disk of %v was %v", h, hstring)
	}
	return nil
}

func verifyWorker(ch chan os.FileInfo) {
	for info := range ch {
		err := verifyObjectHash(info.Name())
		if err == nil {
			recordBlobOwnership(info.Name(), info.Size(), false)
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
		go verifyWorker(vch)
	}

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
