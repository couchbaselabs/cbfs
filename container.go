package main

import (
	"crypto/sha1"
	"encoding/hex"
	"net/http"
)

type Container struct {
}

func getContainer(req *http.Request) *Container {
	return &Container{}
}

func (c Container) shortName(k string) string {
	if len(k) < maxFilename {
		return k
	}
	h := sha1.New()
	h.Write([]byte(k))
	hs := hex.EncodeToString(h.Sum(nil))
	return "/+" + k[:truncateKeyLen] + "-" + hs
}

func (c Container) resolvePath(req *http.Request) (path string, key string) {
	path = req.URL.Path
	// Ignore /, but remove leading / from /blah
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	if len(path) > 0 && path[len(path)-1] == '/' {
		path = path + "index.html"
	} else if len(path) == 0 {
		path = "index.html"
	}

	return path, c.shortName(path)
}
