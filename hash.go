package main

import (
	"crypto"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "crypto/md5"
	_ "crypto/sha1"
	_ "crypto/sha256"
	_ "crypto/sha512"
)

var hashBuilders = map[string]crypto.Hash{
	"md4":       crypto.MD4,
	"md5":       crypto.MD5,
	"sha1":      crypto.SHA1,
	"sha224":    crypto.SHA224,
	"sha256":    crypto.SHA256,
	"sha384":    crypto.SHA384,
	"sha512":    crypto.SHA512,
	"ripemd160": crypto.RIPEMD160,
}

func getHash() hash.Hash {
	h, ok := hashBuilders[globalConfig.Hash]
	if !ok {
		return nil
	}
	if !h.Available() {
		log.Printf("Hash %v is not available", globalConfig.Hash)
		return nil
	}
	return h.New()
}

type hashRecord struct {
	tmpf    *os.File
	sh      hash.Hash
	w       io.Writer
	hashin  string
	base    string
	written int64
}

func NewHashRecord(tmpdir, hashin string) (*hashRecord, error) {
	tmpf, err := ioutil.TempFile(tmpdir, "tmp")
	if err != nil {
		return nil, err
	}

	sh := getHash()

	return &hashRecord{
		tmpf:   tmpf,
		sh:     sh,
		w:      io.MultiWriter(tmpf, sh),
		hashin: hashin,
		base:   *root,
	}, nil
}

func (h *hashRecord) Write(p []byte) (n int, err error) {
	n, err = h.w.Write(p)
	if err == nil {
		h.written += int64(n)
	}
	return
}

func (h *hashRecord) Finish() (string, error) {
	err := h.tmpf.Close()
	if err != nil {
		return "", err
	}

	hs := hex.EncodeToString(h.sh.Sum([]byte{}))
	fn := hashFilename(h.base, hs)

	if h.hashin != "" && h.hashin != hs {
		return "", fmt.Errorf("Invalid hash %v != %v",
			h.hashin, hs)
	}

	err = os.Rename(h.tmpf.Name(), fn)
	if err != nil {
		os.MkdirAll(filepath.Dir(fn), 0777)
		os.Remove(fn)
		err = os.Rename(h.tmpf.Name(), fn)
		if err != nil {
			log.Printf("Error renaming %v to %v: %v",
				h.tmpf.Name(), fn, err)
			os.Remove(h.tmpf.Name())
			return "", err
		}
	}

	h.tmpf = nil

	return hs, nil
}

func (h *hashRecord) Process(r io.Reader) (string, int64, error) {
	length, err := io.Copy(h, r)
	if err != nil {
		return "", length, err
	}

	hs, err := h.Finish()
	return hs, length, err
}

func (h *hashRecord) Close() error {
	if h != nil && h.tmpf != nil {
		os.Remove(h.tmpf.Name())
		return h.tmpf.Close()
	}
	return nil
}

func cleanTmpFiles() error {
	d, err := os.Open(*root)
	if err != nil {
		return err
	}
	fi, err := d.Readdir(0)
	if err != nil {
		return err
	}
	now := time.Now()
	cleaned := 0
	for _, fn := range fi {
		cutoff := fn.ModTime().Add(1 * time.Hour)
		if strings.HasPrefix(fn.Name(), "tmp") &&
			cutoff.Before(now) {

			err = os.Remove(filepath.Join(*root, fn.Name()))
			if err == nil {
				cleaned++
			} else {
				log.Printf("Error cleaning %v: %v",
					fn.Name(), err)
			}
		}
	}
	if cleaned > 0 {
		log.Printf("Removed %v tmp files in %v",
			cleaned, time.Since(now))
	}
	return nil
}
