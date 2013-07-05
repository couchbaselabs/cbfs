package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/couchbaselabs/cbfs/client"
	"github.com/couchbaselabs/cbfs/tools"
	"github.com/dustin/go-id3"
	"github.com/dustin/goexif/exif"
)

var uploadWg = sync.WaitGroup{}

var uploadFlags = flag.NewFlagSet("upload", flag.ExitOnError)
var uploadVerbose = uploadFlags.Bool("v", false, "Verbose")
var uploadDelete = uploadFlags.Bool("delete", false,
	"Delete locally missing items")
var uploadMeta = uploadFlags.Bool("meta", false,
	"Store meta info in userData for items")
var uploadWorkers = uploadFlags.Int("workers", 4, "Number of upload workers")
var uploadRevs = uploadFlags.Int("revs", 0,
	"Number of old revisions to keep (-1 == all)")
var uploadNoop = uploadFlags.Bool("n", false, "Dry run")
var uploadIgnore = uploadFlags.String("ignore", "",
	"Path to ignore file")
var uploadUnsafe = uploadFlags.Bool("unsafe", false,
	"Unsafe (not synchronously replicated) uploads.")
var uploadNoHash = uploadFlags.Bool("nohash", false,
	"Don't include the hash in the upload request")
var uploadExpiration = uploadFlags.Int("expire", 0,
	"Expiration time (in seconds, or abs unix time)")
var uploadRevsSet = false

var quotingReplacer = strings.NewReplacer("%", "%25",
	"?", "%3f",
	" ", "%20",
	"#", "%23")

type uploadOpType uint8

const (
	uploadFileOp = uploadOpType(iota)
	removeFileOp
	removeRecurseOp
)

func (u uploadOpType) String() string {
	switch u {
	case uploadFileOp:
		return "upload file"
	case removeFileOp:
		return "remove file"
	case removeRecurseOp:
		return "remove (recursive) file"
	}
	panic("unhandled op type")
}

type uploadReq struct {
	src        string
	dest       string
	op         uploadOpType
	remoteHash string
}

func processMP3Meta(src, dest string) (interface{}, error) {
	f, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	ifile := id3.Read(f)
	return ifile, nil
}

func processEXIFMeta(src, dest string) (interface{}, error) {
	f, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return exif.Decode(f)
}

func processMeta(client *cbfsclient.Client, src, dest string) error {
	var data interface{}
	var err error

	switch filepath.Ext(strings.ToLower(src)) {
	case ".mp3":
		data, err = processMP3Meta(src, dest)
	case ".jpg", ".jpeg":
		data, err = processEXIFMeta(src, dest)
	default:
		log.Printf("No meta info for %#v",
			filepath.Ext(strings.ToLower(src)))
	}

	if err != nil || data == nil {
		return err
	}

	b, err := json.Marshal(data)
	if err != nil {
		return err
	}

	udest, err := url.Parse(client.URLFor(dest))
	if err != nil {
		return err
	}
	udest.Path = "/.cbfs/meta" + udest.Path

	preq, err := http.NewRequest("PUT", udest.String(), bytes.NewReader(b))
	if err != nil {
		return err
	}

	preq.Header.Set("Content-Type", "application/json")

	cbfstool.Verbose(*uploadVerbose, "Uploading meta info to %v", udest)

	resp, err := http.DefaultClient.Do(preq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		return fmt.Errorf("HTTP Error:  %v", resp.Status)
	}

	return nil
}

func uploadFile(client *cbfsclient.Client, src, dest, localHash string) error {
	cbfstool.Verbose(*uploadVerbose, "Uploading %v -> %v (%v)",
		src, dest, localHash)
	if *uploadNoop {
		return nil
	}

	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	err = uploadStream(client, f, src, dest, localHash)
	if err != nil {
		return err
	}

	if *uploadMeta {
		err = processMeta(client, src, dest)
		if err != nil {
			log.Printf("Error processing meta info: %v", err)
		}
	}
	return nil
}

func uploadStream(client *cbfsclient.Client, r io.Reader,
	srcName, dest, localHash string) error {

	opts := cbfsclient.PutOptions{
		Unsafe:           *uploadUnsafe,
		Expiration:       *uploadExpiration,
		Hash:             localHash,
		ContentTransform: maybeCrypt,
	}

	if uploadRevsSet {
		opts.SetKeepRevs(*uploadRevs)
	}

	if *uploadNoHash {
		opts.Hash = ""
	}

	return client.Put(srcName, dest, r, opts)
}

// This is very similar to rm's version, but uses different channel
// signaling.
func uploadRmDir(client *cbfsclient.Client, under string) error {
	cbfstool.Verbose(*uploadVerbose, "Removing directory: %v", under)

	listing, err := client.ListDepth(under, 8192)
	if err != nil {
		return nil
	}

	r := quotingReplacer
	for fn := range listing.Files {
		cbfstool.Verbose(*uploadVerbose, "Removing file %v", fn)
		if !*uploadNoop {
			err = rmFile(client, r.Replace(fn))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func uploadRmDashR(client *cbfsclient.Client, d string) error {
	cbfstool.Verbose(*uploadVerbose, "Removing (recursively) %v", d)

	return uploadRmDir(client, d)
}

func localHash(fn string) string {
	f, err := os.Open(fn)
	if err != nil {
		return "unknown"
	}
	defer f.Close()

	h := sha1.New()
	_, err = io.Copy(h, f)
	if err != nil {
		return "unknown"
	}

	return hex.EncodeToString(h.Sum([]byte{}))
}

func uploadWorker(client *cbfsclient.Client, ch chan uploadReq) {
	defer uploadWg.Done()
	for req := range ch {
		retries := 0
		done := false
		for !done {
			var err error
			switch req.op {
			case uploadFileOp:
				lh := localHash(req.src)
				if req.remoteHash == "" {
					err = uploadFile(client, req.src, req.dest, lh)
				} else {
					if lh != req.remoteHash {
						cbfstool.Verbose(*uploadVerbose, "%v has changed, reupping",
							req.src)
						err = uploadFile(client, req.src, req.dest, lh)
					}
				}
			case removeFileOp:
				cbfstool.Verbose(*uploadVerbose, "Removing file %v", req.dest)
				if !*uploadNoop {
					err = rmFile(client, req.dest)
				}
			case removeRecurseOp:
				err = uploadRmDashR(client, req.dest)
			default:
				log.Fatalf("Unhandled case")
			}
			if err != nil {
				if retries < 3 {
					retries++
					log.Printf("Error in %v: %v... retrying",
						req.op, err)
					time.Sleep(time.Duration(retries) * time.Second)
				} else {
					log.Printf("Error in %v %v: %v",
						req.op, req.src, err)
					done = true
				}
			} else {
				done = true
			}
		}
	}
}

func syncPath(client *cbfsclient.Client, path, dest string,
	info os.FileInfo, ch chan<- uploadReq) error {

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	children, err := f.Readdir(0)
	if err != nil {
		return err
	}

	retries := 3
	serverListing, err := client.ListOrEmpty(dest)
	for err != nil && retries > 0 {
		serverListing, err = client.ListOrEmpty(dest)
		time.Sleep(time.Second)
		retries--
	}
	if err != nil {
		return err
	}

	localNames := map[string]os.FileInfo{}
	for _, c := range children {
		switch c.Mode() & os.ModeType {
		case os.ModeCharDevice, os.ModeDevice,
			os.ModeNamedPipe, os.ModeSocket, os.ModeSymlink:
			cbfstool.Verbose(*uploadVerbose, "Ignoring special file: %v - %v",
				filepath.Join(path, c.Name()), c.Mode())
		default:
			fullPath := filepath.Join(path, c.Name())
			if !isIgnored(fullPath) {
				localNames[c.Name()] = c
			} else {
				cbfstool.Verbose(*uploadVerbose, "Ignoring %v", fullPath)
			}
		}
	}

	remoteNames := map[string]bool{}
	for n := range serverListing.Files {
		if n != "" {
			remoteNames[n] = true
		}
	}
	for n := range serverListing.Dirs {
		if n != "" {
			remoteNames[n] = true
		}
	}

	// Keeping it short
	r := quotingReplacer

	missingUpstream := []string{}
	for n, fi := range localNames {
		if !(fi.IsDir() || remoteNames[n]) {
			missingUpstream = append(missingUpstream, n)
		} else if !fi.IsDir() {
			if ri, ok := serverListing.Files[n]; ok {
				ch <- uploadReq{filepath.Join(path, n),
					dest + "/" + r.Replace(n), uploadFileOp, ri.OID}
			}
		}
	}

	toRm := []string{}
	for n := range remoteNames {
		if _, ok := localNames[n]; !ok {
			toRm = append(toRm, n)
		}
	}

	if len(missingUpstream) > 0 {
		for _, m := range missingUpstream {
			ch <- uploadReq{filepath.Join(path, m),
				dest + "/" + r.Replace(m), uploadFileOp, ""}
		}
	}

	if *uploadDelete && len(toRm) > 0 {
		for _, m := range toRm {
			ch <- uploadReq{"", dest + "/" + r.Replace(m), removeFileOp, ""}
			ch <- uploadReq{"", dest + "/" + r.Replace(m), removeRecurseOp, ""}
		}
	}

	return nil
}

func syncUp(client *cbfsclient.Client, src, u string, ch chan<- uploadReq) {
	for strings.HasSuffix(u, "/") {
		u = u[:len(u)-1]
	}
	for strings.HasSuffix(src, "/") {
		src = src[:len(src)-1]
	}

	err := filepath.Walk(src,
		func(path string, info os.FileInfo, err error) error {
			if err == nil && info.IsDir() {
				if isIgnored(path) {
					cbfstool.Verbose(*uploadVerbose, "Skipping dir %v",
						path)
					return filepath.SkipDir
				}
				shortPath := path[len(src):]
				err = syncPath(client, path, u+shortPath, info, ch)
			}
			return err
		})
	cbfstool.MaybeFatal(err, "Traversal error: %v", err)
}

func uploadCommand(u string, args []string) {
	uploadFlags.Parse(args)

	initCrypto()
	initHttpMagic()

	uploadFlags.Visit(func(f *flag.Flag) {
		if f.Name == "revs" {
			uploadRevsSet = true
		}
	})

	if *uploadIgnore != "" {
		err := loadIgnorePatternsFromFile(*uploadIgnore)
		cbfstool.MaybeFatal(err, "Error loading ignores: %v", err)
	}

	client, err := cbfsclient.New(u)
	cbfstool.MaybeFatal(err, "Error setting up client: %v", err)

	srcFn := uploadFlags.Arg(0)
	dest := uploadFlags.Arg(1)
	// Special case stdin.
	if srcFn == "-" {
		err := uploadStream(client, os.Stdin, "", dest, "")
		cbfstool.MaybeFatal(err, "Error uploading stdin: %v", err)
		return
	}

	fi, err := os.Stat(srcFn)
	cbfstool.MaybeFatal(err, "Error statting %v: %v", srcFn, err)

	if fi.IsDir() {
		ch := make(chan uploadReq, 1000)

		for i := 0; i < *uploadWorkers; i++ {
			uploadWg.Add(1)
			go uploadWorker(client, ch)
		}

		start := time.Now()
		syncUp(client, srcFn, dest, ch)

		close(ch)
		log.Printf("Finished traversal in %v", time.Since(start))
		uploadWg.Wait()
		log.Printf("Finished sync in %v", time.Since(start))
	} else {
		err = uploadFile(client, srcFn, dest, localHash(srcFn))
		cbfstool.MaybeFatal(err, "Error uploading file: %v", err)
	}
}
