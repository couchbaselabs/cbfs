package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/dustin/gomemcached/client"
	"log"
	"net/http"
	"os"
	"time"
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
	Headers  http.Header      `json:"headers"`
	OID      string           `json:"oid"`
	Length   int64            `json:"length"`
	Userdata *json.RawMessage `json:"userdata,omitempty"`
	Modified time.Time        `json:"modified"`
}

func (fm fileMeta) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"oid":      fm.OID,
		"headers":  map[string][]string(fm.Headers),
		"type":     "file",
		"ctype":    fm.Headers.Get("Content-Type"),
		"length":   fm.Length,
		"modified": fm.Modified,
	}

	if fm.Userdata != nil {
		m["userdata"] = fm.Userdata
	}
	return json.Marshal(m)
}

func mustEncode(i interface{}) []byte {
	rv, err := json.Marshal(i)
	if err != nil {
		log.Panicf("Error mustEncoding %#v: %v", i, err)
	}
	return rv
}

func storeMeta(k string, fm fileMeta) error {
	return couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		_, err := mc.CAS(vb, k, func(in []byte) ([]byte, memcached.CasOp) {
			existing := fileMeta{}
			err := json.Unmarshal(in, &existing)
			if err == nil {
				fm.Userdata = existing.Userdata
			}
			return mustEncode(&fm), memcached.CASStore
		}, 0)
		return err
	})
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

	err := adjustPeriodicJobs()
	if err != nil {
		log.Fatalf("Error adjusting periodic jobs from flags: %v", err)
	}

	err = initServerId()
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
