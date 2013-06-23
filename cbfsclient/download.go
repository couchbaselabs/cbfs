package main

import (
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbaselabs/cbfs/client"
	"github.com/dustin/go-humanize"
)

var dlFlags = flag.NewFlagSet("download", flag.ExitOnError)
var dlVerbose = dlFlags.Bool("v", false, "Verbose download")
var dlConcurrency = dlFlags.Int("c", 4, "Number of concurrent downloaders")
var dlNoop = dlFlags.Bool("n", false, "Noop")

var totalBytes int64

func saveDownload(filenames []string, oid string, r io.Reader) error {
	var w io.Writer
	if *dlNoop {
		w = ioutil.Discard
	} else {
		ws := []io.Writer{}
		for _, fn := range filenames {
			f, err := os.Create(fn)
			if err != nil {
				os.MkdirAll(filepath.Dir(fn), 0777)
				f, err = os.Create(fn)
			}
			if err != nil {
				return err
			}
			defer f.Close()
			ws = append(ws, f)
		}
		w = io.MultiWriter(ws...)
	}
	n, err := io.Copy(w, r)
	if err == nil {
		atomic.AddInt64(&totalBytes, n)
		verbose(*dlVerbose, "Downloaded %s into %v",
			humanize.Bytes(uint64(n)), strings.Join(filenames, ", "))
	} else {
		log.Printf("Error downloading %v (for %v): %v",
			oid, filenames, err)
	}

	return err
}

func downloadCommand(u string, args []string) {
	dlFlags.Parse(args)

	if dlFlags.NArg() < 2 {
		log.Fatalf("src and dest required")
	}

	src := dlFlags.Arg(0)
	destbase := dlFlags.Arg(1)

	client, err := cbfsclient.New(u)
	maybeFatal(err, "Can't build a client: %v", err)

	u = relativeUrl(u, src)
	log.Printf("Listing from %v with %v", u, client)

	things, err := cbfsclient.ListDepth(u, 4096)
	maybeFatal(err, "Can't list things: %v", err)

	start := time.Now()
	oids := []string{}
	dests := map[string][]string{}
	for fn, inf := range things.Files {
		fn = fn[len(src):]
		dests[inf.OID] = append(dests[inf.OID],
			filepath.Join(destbase, fn))
		oids = append(oids, inf.OID)
	}

	err = client.GetBlobs(*dlConcurrency,
		func(oid string, r io.Reader) error {
			return saveDownload(dests[oid], oid, r)
		}, oids...)

	maybeFatal(err, "Error getting blobs: %v", err)

	b := atomic.AddInt64(&totalBytes, 0)
	d := time.Since(start)
	verbose(*dlVerbose, "Moved %s in %v (%s/s)", humanize.Bytes(uint64(b)),
		d, humanize.Bytes(uint64(float64(b)/d.Seconds())))
}
