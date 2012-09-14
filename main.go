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
)

var root = flag.String("root", "storage", "Storage location")
var hashType = flag.String("hash", "sha1", "Hash to use")
var couchbaseServer = flag.String("couchbase", "", "Couchbase URL")
var couchbaseBucket = flag.String("bucket", "default", "Couchbase bucket")
var guidFile = flag.String("guidfile", ".serverguid", "Path to server identifier")

type fileMeta struct {
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

func (fm *fileMeta) UnmarshalJSON(d []byte) error {
	m := map[string]interface{}{}
	err := json.Unmarshal(d, &m)
	if err != nil {
		return err
	}

	fm.OID = m["oid"].(string)
	fm.Length = int64(m["length"].(float64))

	fm.Headers = http.Header{}
	for k, vs := range m["headers"].(map[string]interface{}) {
		for _, v := range vs.([]interface{}) {
			fm.Headers.Add(k, v.(string))
		}
	}

	return nil
}

func storeMeta(name string, fm fileMeta) error {
	return couchbase.Set(name, fm)
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
		req.Header,
		h,
		length,
	}

	err = storeMeta(req.URL.Path, fm)
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
	err := couchbase.Get(req.URL.Path, &got)
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

func handler(w http.ResponseWriter, req *http.Request) {
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

	var err error
	couchbase, err = dbConnect()
	if err != nil {
		log.Fatalf("Can't connect to couchbase: %v", err)
	}

	s := &http.Server{
		Addr:    *addr,
		Handler: http.HandlerFunc(handler),
	}
	log.Printf("Listening to web requests on %s as server %s", *addr, serverIdentifier())
	log.Fatal(s.ListenAndServe())
}
