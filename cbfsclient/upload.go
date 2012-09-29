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
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

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
var uploadRevsSet = false

type uploadOpType uint8

const (
	uploadFileOp = uploadOpType(iota)
	removeFileOp
	removeRecurseOp
)

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
		log.Printf("Uploading meta info to %v", udest.String())
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

func uploadFile(src, dest string) error {
	if *uploadVerbose {
		log.Printf("Uploading %v -> %v", src, dest)
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

	ctype := http.DetectContentType(someBytes)
	if strings.HasPrefix(ctype, "text/plain") ||
		strings.HasPrefix(ctype, "application/octet-stream") {
		ctype = recognizeTypeByName(src, ctype)
	}

	preq.Header.Set("Content-Type", ctype)

	resp, err := http.DefaultClient.Do(preq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		return fmt.Errorf("HTTP Error:  %v", resp.Status)
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
func uploadRmDashR(baseUrl string, ch chan uploadReq) ([]string, error) {
	for strings.HasSuffix(baseUrl, "/") {
		baseUrl = baseUrl[:len(baseUrl)-1]
	}

	listing, err := listOrEmpty(baseUrl)
	for err != nil {
		return []string{}, err
	}
	for fn := range listing.Files {
		err = rmFile(baseUrl + "/" + fn)
		if err != nil {
			return []string{}, err
		}
	}
	children := make([]string, 0, len(listing.Dirs))
	for dn := range listing.Dirs {
		children = append(children, baseUrl+"/"+dn)
	}
	return children, nil
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
				if req.remoteHash == "" {
					err = uploadFile(req.src, req.dest)
				} else {
					if localHash(req.src) != req.remoteHash {
						if *uploadVerbose {
							log.Printf("%v has changed, reupping",
								req.src)
						}
						err = uploadFile(req.src, req.dest)
					}
				}
			case removeFileOp:
				if *uploadVerbose {
					log.Printf("Removing file %v", req.dest)
				}
				err = rmFile(req.dest)
			case removeRecurseOp:
				todo := []string{req.dest}
				for err == nil && len(todo) > 0 {
					todo, err = uploadRmDashR(req.dest, ch)
				}
			default:
				log.Fatalf("Unhandled case")
			}
			if err != nil {
				if retries < 3 {
					retries++
					log.Printf("Error uploading file: %v... retrying",
						err)
					time.Sleep(time.Duration(retries) * time.Second)
				} else {
					log.Printf("Error uploading file %v: %v",
						req.src, err)
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

	retries := 3
	serverListing, err := listOrEmpty(dest)
	for err != nil && retries > 0 {
		serverListing, err = listOrEmpty(dest)
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
			localNames[c.Name()] = c
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

	missingUpstream := []string{}
	for n, fi := range localNames {
		if !(fi.IsDir() || remoteNames[n]) {
			missingUpstream = append(missingUpstream, n)
		} else if !fi.IsDir() {
			if ri, ok := serverListing.Files[n]; ok {
				ch <- uploadReq{filepath.Join(path, n),
					dest + "/" + n, uploadFileOp, ri.OID}
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
				dest + "/" + m, uploadFileOp, ""}
		}
	}

	if *uploadDelete && len(toRm) > 0 {
		for _, m := range toRm {
			ch <- uploadReq{"", dest + "/" + m, removeFileOp, ""}
			ch <- uploadReq{"", dest + "/" + m, removeRecurseOp, ""}
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
		err = uploadFile(uploadFlags.Arg(0), du)
		if err != nil {
			log.Fatalf("Error uploading file: %v", err)
		}
	}
}
