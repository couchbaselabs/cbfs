package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
)

var bindAddr = flag.String("bind", ":8484", "Address to bind web thing to")
var root = flag.String("root", "storage", "Storage location")
var hashType = flag.String("hash", "sha1", "Hash to use")
var couchbaseServer = flag.String("couchbase", "", "Couchbase URL")
var couchbaseBucket = flag.String("bucket", "default", "Couchbase bucket")
var guidFile = flag.String("guidfile", ".serverguid",
	"Path to server identifier")
var defaultPath = flag.String("defaultPath", "/index.html",
	"Default path to fetch for / reqs")
var cachePercentage = flag.Int("cachePercent", 100,
	"Percentage of proxied requests to eagerly cache.")

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

func hashFilename(base, hstr string) string {
	return base + "/" + hstr[:2] + "/" + hstr
}

func main() {
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

	err := initServerId()
	if err != nil {
		log.Fatalf("Error initializing server ID: %v", err)
	}

	couchbase, err = dbConnect()
	if err != nil {
		log.Fatalf("Can't connect to couchbase: %v", err)
	}

	go heartbeat()
	go reconcileLoop()
	go runPeriodicJobs()

	s := &http.Server{
		Addr:    *bindAddr,
		Handler: http.HandlerFunc(httpHandler),
	}
	log.Printf("Listening to web requests on %s as server %s",
		*bindAddr, serverId)
	log.Fatal(s.ListenAndServe())
}
