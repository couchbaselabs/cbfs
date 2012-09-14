package main

import (
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var root = flag.String("root", "storage", "Storage location")

func doPut(w http.ResponseWriter, req *http.Request) {

	sh := sha1.New()

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

	hash := hex.EncodeToString(sh.Sum([]byte{}))
	fn := *root + "/" + hash
	err = os.Rename(tmpf.Name(), fn)
	if err != nil {
		log.Printf("Error renaming %v to %v: %v", tmpf.Name(), fn, err)
		w.WriteHeader(500)
		os.Remove(tmpf.Name())
		return
	}

	log.Printf("Wrote %v -> %v (%#v)", req.URL.Path, hash, req.Header)

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

	s := &http.Server{
		Addr:    *addr,
		Handler: http.HandlerFunc(handler),
	}
	log.Printf("Listening to web requests on %s", *addr)
	log.Fatal(s.ListenAndServe())
}
