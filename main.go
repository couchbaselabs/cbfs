package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/couchbaselabs/go-couchbase"
)

var root = flag.String("root", "storage", "Storage location")
var hashType = flag.String("hash", "sha1", "Hash to use")
var couchbaseServer = flag.String("couchbase", "", "Couchbase URL")
var couchbaseBucket = flag.String("bucket", "default", "Couchbase bucket")

type fileMeta struct {
	Name    string
	Headers http.Header
	OID     string
	Length  int64
}

func (fm fileMeta) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"oid":     fm.OID,
		"headers": map[string][]string(fm.Headers),
		"type":    "file",
		"ctype":   fm.Headers.Get("Content-Type"),
		"length":  fm.Length,
	}
	return json.Marshal(m)
}

func dbConnect() (*couchbase.Bucket, error) {
	rv, err := couchbase.GetBucket(*couchbaseServer,
		"default", *couchbaseBucket)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func storeMeta(fm fileMeta) error {
	db, err := dbConnect()
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Set(fm.Name, fm)
}

func hashFilename(hstr string) string {
	return *root + "/" + hstr[:2] + "/" + hstr
}

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
		req.URL.Path,
		req.Header,
		h,
		length,
	}

	err = storeMeta(fm)
	if err != nil {
		log.Printf("Error storing file meta: %v", err)
		w.WriteHeader(500)
		return
	}

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
