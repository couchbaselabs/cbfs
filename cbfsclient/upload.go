package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbaselabs/cbfs/client"
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

func recognizeTypeByName(n, def string) string {
	byname := mime.TypeByExtension(n)
	switch {
	case byname != "":
		return byname
	case strings.HasSuffix(n, ".js"):
		return "application/javascript"
	case strings.HasSuffix(n, ".json"):
		return "application/json"
	case strings.HasSuffix(n, ".css"):
		return "text/css"
	}
	return def
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

func processMeta(src, dest string) error {
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

	udest, err := url.Parse(dest)
	if err != nil {
		return err
	}
	udest.Path = "/.cbfs/meta" + udest.Path

	preq, err := http.NewRequest("PUT", udest.String(), bytes.NewReader(b))
	if err != nil {
		return err
	}

	preq.Header.Set("Content-Type", "application/json")

	if *uploadVerbose {
		log.Printf("Uploading meta info to %v", udest)
	}

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

func uploadFile(src, dest, localHash string) error {
	if *uploadVerbose {
		log.Printf("Uploading %v -> %v (%v)", src, dest, localHash)
	}
	if *uploadNoop {
		return nil
	}

	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	someBytes := make([]byte, 512)
	n, err := f.Read(someBytes)
	if err != nil && err != io.EOF {
		return err
	}
	someBytes = someBytes[:n]

	length, err := f.Seek(0, 2)
	if err != nil {
		return err
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		return err
	}

	preq, err := http.NewRequest("PUT", dest, f)
	if err != nil {
		return err
	}
	if uploadRevsSet {
		preq.Header.Set("X-CBFS-KeepRevs", strconv.Itoa(*uploadRevs))
	}
	if *uploadUnsafe {
		preq.Header.Set("X-CBFS-Unsafe", "true")
	}

	ctype := http.DetectContentType(someBytes)
	if strings.HasPrefix(ctype, "text/plain") ||
		strings.HasPrefix(ctype, "application/octet-stream") {
		ctype = recognizeTypeByName(src, ctype)
	}

	preq.Header.Set("Content-Length", strconv.FormatInt(length, 10))
	preq.Header.Set("Content-Type", ctype)
	preq.Header.Set("X-CBFS-Hash", localHash)

	resp, err := http.DefaultClient.Do(preq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		r, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("HTTP Error:  %v: %s", resp.Status, r)
	}

	if *uploadMeta {
		err = processMeta(src, dest)
		if err != nil {
			log.Printf("Error processing meta info: %v", err)
		}
	}

	return nil
}

// This is very similar to rm's version, but uses different channel
// signaling.
func uploadRmDir(baseUrl string) ([]string, error) {
	if *uploadVerbose {
		log.Printf("Removing directory: %v", baseUrl)
	}
	r := quotingReplacer
	for strings.HasSuffix(baseUrl, "/") {
		baseUrl = baseUrl[:len(baseUrl)-1]
	}

	listing, err := cbfsclient.ListOrEmpty(baseUrl)
	for err != nil {
		return []string{}, err
	}
	for fn := range listing.Files {
		if *uploadVerbose {
			log.Printf("Removing file %v/%v", baseUrl, fn)
		}
		if !*uploadNoop {
			err = rmFile(baseUrl + "/" + r.Replace(fn))
			if err != nil {
				return []string{}, err
			}
		}
	}
	children := make([]string, 0, len(listing.Dirs))
	for dn := range listing.Dirs {
		children = append(children, baseUrl+"/"+r.Replace(dn))
	}
	return children, nil
}

func uploadRmDashR(d string) error {
	if *uploadVerbose {
		log.Printf("Removing (recursively) %v", d)
	}

	children, err := uploadRmDir(d)
	if err == nil && len(children) > 0 {
		for _, child := range children {
			err = uploadRmDashR(child)
		}
	}
	return err
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

func uploadWorker(ch chan uploadReq) {
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
					err = uploadFile(req.src, req.dest, lh)
				} else {
					if lh != req.remoteHash {
						if *uploadVerbose {
							log.Printf("%v has changed, reupping",
								req.src)
						}
						err = uploadFile(req.src, req.dest, lh)
					}
				}
			case removeFileOp:
				if *uploadVerbose {
					log.Printf("Removing file %v", req.dest)
				}
				if !*uploadNoop {
					err = rmFile(req.dest)
				}
			case removeRecurseOp:
				err = uploadRmDashR(req.dest)
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

func syncPath(path, dest string, info os.FileInfo, ch chan<- uploadReq) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	children, err := f.Readdir(0)
	if err != nil {
		return err
	}

	dest = quotingReplacer.Replace(dest)

	retries := 3
	serverListing, err := cbfsclient.ListOrEmpty(dest)
	for err != nil && retries > 0 {
		serverListing, err = cbfsclient.ListOrEmpty(dest)
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
			if *uploadVerbose {
				log.Printf("Ignoring special file: %v - %v",
					filepath.Join(path, c.Name()), c.Mode())
			}
		default:
			fullPath := filepath.Join(path, c.Name())
			if !isIgnored(fullPath) {
				localNames[c.Name()] = c
			} else {
				if *uploadVerbose {
					log.Printf("Ignoring %v", fullPath)
				}
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

func syncUp(src, u string, ch chan<- uploadReq) {
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
					if *uploadVerbose {
						log.Printf("Skipping dir %v",
							path)
					}
					return filepath.SkipDir
				}
				shortPath := path[len(src):]
				err = syncPath(path, u+shortPath, info, ch)
			}
			return err
		})
	if err != nil {
		log.Fatalf("Traversal error: %v", err)
	}
}

func uploadCommand(u string, args []string) {
	uploadFlags.Parse(args)

	uploadFlags.Visit(func(f *flag.Flag) {
		if f.Name == "revs" {
			uploadRevsSet = true
		}
	})

	if uploadFlags.NArg() < 2 {
		log.Fatalf("src and dest required")
	}

	if *uploadIgnore != "" {
		err := loadIgnorePatternsFromFile(*uploadIgnore)
		if err != nil {
			log.Fatalf("Error loading ignores: %v", err)
		}
	}

	du := relativeUrl(u, uploadFlags.Arg(1))

	fi, err := os.Stat(uploadFlags.Arg(0))
	if err != nil {
		log.Fatal(err)
	}

	if fi.IsDir() {
		ch := make(chan uploadReq, 1000)

		for i := 0; i < *uploadWorkers; i++ {
			uploadWg.Add(1)
			go uploadWorker(ch)
		}

		start := time.Now()
		syncUp(uploadFlags.Arg(0), du, ch)

		close(ch)
		log.Printf("Finished traversal in %v", time.Since(start))
		uploadWg.Wait()
		log.Printf("Finished sync in %v", time.Since(start))
	} else {
		err = uploadFile(uploadFlags.Arg(0), du,
			localHash(uploadFlags.Arg(0)))
		if err != nil {
			log.Fatalf("Error uploading file: %v", err)
		}
	}
}
