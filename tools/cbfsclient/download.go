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

	"sethwklein.net/go/errutil"

	"github.com/couchbaselabs/cbfs/client"
	"github.com/couchbaselabs/cbfs/tools"
	"github.com/dustin/go-humanize"
)

var dlFlags = flag.NewFlagSet("download", flag.ExitOnError)
var dlverbose = dlFlags.Bool("v", false, "verbose download")
var totalConcurrency = dlFlags.Int("ct", 4, "Total number of concurrent downloads")
var nodeConcurrency = dlFlags.Int("cn", 2, "Max concurrent downloads per node")
var dlNoop = dlFlags.Bool("n", false, "Noop")
var dlLink = dlFlags.Bool("L", false, "hard link identical content")

var totalBytes int64

func saveDownload(filenames []string, oid string, r io.Reader) (err error) {
	var w io.Writer
	switch {
	case *dlNoop:
		w = ioutil.Discard
	case *dlLink:
		basefn := filenames[0]
		f, er := os.Create(basefn)
		if er != nil {
			er = os.MkdirAll(filepath.Dir(basefn), 0777)
			if er != nil {
				return er
			}
			f, er = os.Create(basefn)
		}
		defer errutil.AppendCall(&err, f.Close)
		w = f
		for _, fn := range filenames[1:] {
			er := os.Link(basefn, fn)
			switch {
			case os.IsExist(er):
				// Don't care, we've already got it
				er = nil
			case er != nil:
				er = os.MkdirAll(filepath.Dir(fn), 0777)
				if er != nil {
					return er
				}
				er = os.Link(basefn, fn)
			default:
			}
			if er != nil {
				return er
			}
			cbfstool.Verbose(*dlverbose, "Linked %s -> %v", fn, basefn)
		}
	default:
		ws := []io.Writer{}
		for _, fn := range filenames {
			f, er := os.Create(fn)
			if er != nil {
				er = os.MkdirAll(filepath.Dir(fn), 0777)
				if er != nil {
					return er
				}
				f, er = os.Create(fn)
			}
			if er != nil {
				return er
			}
			defer errutil.AppendCall(&err, f.Close)
			ws = append(ws, f)
		}
		w = io.MultiWriter(ws...)
	}
	n, err := io.Copy(w, r)
	if err == nil {
		atomic.AddInt64(&totalBytes, n)
		cbfstool.Verbose(*dlverbose, "Downloaded %s into %v",
			humanize.Bytes(uint64(n)), strings.Join(filenames, ", "))
	} else {
		log.Printf("Error downloading %v (for %v): %v",
			oid, filenames, err)
	}

	return err
}

func downloadCommand(u string, args []string) {
	src := dlFlags.Arg(0)
	destbase := dlFlags.Arg(1)

	if destbase == "" {
		destbase = filepath.Base(src)
	}

	for len(src) > 0 && src[0] == '/' {
		src = src[1:]
	}

	initHttpMagic()

	client, err := cbfsclient.New(u)
	cbfstool.MaybeFatal(err, "Can't build a client: %v", err)

	things, err := client.ListDepth(src, 4096)
	cbfstool.MaybeFatal(err, "Can't list things: %v", err)

	start := time.Now()
	oids := []string{}
	dests := map[string][]string{}
	for fn, inf := range things.Files {
		fn = fn[len(src):]
		dests[inf.OID] = append(dests[inf.OID],
			filepath.Join(destbase, fn))
		oids = append(oids, inf.OID)
	}

	err = client.Blobs(*totalConcurrency, *nodeConcurrency,
		func(oid string, r io.Reader) error {
			return saveDownload(dests[oid], oid, r)
		}, oids...)

	cbfstool.MaybeFatal(err, "Error getting blobs: %v", err)

	b := atomic.AddInt64(&totalBytes, 0)
	d := time.Since(start)
	cbfstool.Verbose(*dlverbose, "Moved %s in %v (%s/s)", humanize.Bytes(uint64(b)),
		d, humanize.Bytes(uint64(float64(b)/d.Seconds())))
}
