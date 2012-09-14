package main

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
)

var hashBuilders = map[string]func() hash.Hash{
	"sha1":   sha1.New,
	"sha256": sha256.New,
	"sha512": sha512.New,
	"md5":    md5.New,
}

func getHash() hash.Hash {
	h, ok := hashBuilders[*hashType]
	if !ok {
		return nil
	}
	return h()
}
