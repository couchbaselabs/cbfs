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
	"time"
)

var maxStorageString = flag.String("maxSize", "",
	"Approximate maximum amount of space to allocate")
var maxStorage int64

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
		log.Printf("Removed local copy of %v, result=%v",
			h, errorOrSuccess(err))
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
		log.Printf("Removed corrupt file from disk: %v (was %v), result=%v",
			h, hstring, errorOrSuccess(err))
		return fmt.Errorf("Hash from disk of %v was %v", h, hstring)
	}
	return nil
}

func shouldVerifyObject(h string) bool {
	b, err := getBlobOwnership(h)
	if err != nil {
		return true
	}
	// True if we haven't checked the object in 30 days.
	return b.Nodes[serverId].Add(globalConfig.ReconcileAge).Before(time.Now())
}

func verifyWorker(ch chan os.FileInfo) {
	nl, err := findAllNodes()
	if err != nil {
		log.Printf("Couldn't find node list during verify: %v", err)
		nl = NodeList{}
	}
	for info := range ch {
		var err error
		force := false
		if shouldVerifyObject(info.Name()) {
			err = verifyObjectHash(info.Name())
			force = true
		}
		if err == nil {
			recordBlobOwnership(info.Name(), info.Size(), force)
		} else {
			log.Printf("Invalid hash for object %v found at verification: %v",
				info.Name(), err)
			removeBlobOwnershipRecord(info.Name(), serverId)
			if len(nl) > 0 {
				salvageBlob(info.Name(), "", 1, nl)
			}
		}
	}
}

func quickVerifyWorker(ch chan os.FileInfo) {
	for info := range ch {
		recordBlobOwnership(info.Name(), info.Size(), false)
	}
}

func reconcileWith(wf func(chan os.FileInfo)) error {
	explen := getHash().Size() * 2

	vch := make(chan os.FileInfo)
	defer close(vch)

	for i := 0; i < *verifyWorkers; i++ {
		go wf(vch)
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

func reconcile() error {
	return reconcileWith(verifyWorker)
}

func quickReconcile() error {
	return reconcileWith(quickVerifyWorker)
}
