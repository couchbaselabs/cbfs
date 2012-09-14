package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var root = flag.String("root", "storage", "Storage location")
var hashType = flag.String("hash", "sha1", "Hash to use")

func doPut(w http.ResponseWriter, req *http.Request) {

	sh := getHash()

	tmpf, err := ioutil.TempFile(*root, "tmp")
	if err != nil {
		log.Printf("Error writing tmp file: %v", err)
		w.WriteHeader(500)
		return
	}

	_, err = io.Copy(io.MultiWriter(tmpf, sh), req.Body)
	if err != nil {
		log.Printf("Error writing data from client: %v", err)
		w.WriteHeader(500)
		return
	}

	h := hex.EncodeToString(sh.Sum([]byte{}))
	fn := *root + "/" + h
	err = os.Rename(tmpf.Name(), fn)
	if err != nil {
		log.Printf("Error renaming %v to %v: %v", tmpf.Name(), fn, err)
		w.WriteHeader(500)
		os.Remove(tmpf.Name())
		return
	}

	log.Printf("Wrote %v -> %v (%#v)", req.URL.Path, h, req.Header)

	w.WriteHeader(204)
}

func handler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	switch req.Method {
	case "PUT":
		doPut(w, req)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	addr := flag.String("bind", ":8484", "Address to bind web thing to")
	flag.Parse()

	if getHash() == nil {
		fmt.Fprintf(os.Stderr,
			"Unsupported hash specified: %v.  Supported hashes:\n",
			*hashType)
		for h := range hashBuilders {
			fmt.Fprintf(os.Stderr, " * %v\n", h)
		}
		os.Exit(1)
	}

	s := &http.Server{
		Addr:    *addr,
		Handler: http.HandlerFunc(handler),
	}
	log.Printf("Listening to web requests on %s", *addr)
	log.Fatal(s.ListenAndServe())
}
