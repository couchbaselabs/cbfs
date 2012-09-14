package main

import (
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func doPut(w http.ResponseWriter, req *http.Request) {

	sh := getHash()

	tmpf, err := ioutil.TempFile(*root, "tmp")
	if err != nil {
		log.Printf("Error writing tmp file: %v", err)
		w.WriteHeader(500)
		return
	}

	length, err := io.Copy(io.MultiWriter(tmpf, sh), req.Body)
	if err != nil {
		log.Printf("Error writing data from client: %v", err)
		w.WriteHeader(500)
		return
	}

	h := hex.EncodeToString(sh.Sum([]byte{}))
	fn := hashFilename(h)

	err = os.Rename(tmpf.Name(), fn)
	if err != nil {
		os.MkdirAll(filepath.Dir(fn), 0777)
		err = os.Rename(tmpf.Name(), fn)
		if err != nil {
			log.Printf("Error renaming %v to %v: %v", tmpf.Name(), fn, err)
			w.WriteHeader(500)
			os.Remove(tmpf.Name())
			return
		}
	}

	log.Printf("Wrote %v -> %v (%#v)", req.URL.Path, h, req.Header)

	fm := fileMeta{
		req.Header,
		h,
		length,
	}

	err = storeMeta(req.URL.Path[1:], fm)
	if err != nil {
		log.Printf("Error storing file meta: %v", err)
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(204)
}

func isResponseHeader(s string) bool {
	switch s {
	case "Content-Type", "Content-Length":
		return true
	}
	return false
}

func doGet(w http.ResponseWriter, req *http.Request) {
	got := fileMeta{}
	err := couchbase.Get(req.URL.Path[1:], &got)
	if err != nil {
		log.Printf("Error getting file %v: %v", req.URL.Path, err)
		w.WriteHeader(404)
		return
	}

	for k, v := range got.Headers {
		if isResponseHeader(k) {
			w.Header()[k] = v
		}
	}

	log.Printf("Need to find blob %v", got.OID)

	f, err := os.Open(hashFilename(got.OID))
	if err != nil {
		log.Printf("Don't have hash file: %v: %v", got.OID, err)
		w.WriteHeader(500)
		return
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	if err != nil {
		log.Printf("Failed to write file: %v", err)
	}
}

func httpHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	switch req.Method {
	case "PUT":
		doPut(w, req)
	case "GET":
		doGet(w, req)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
