package main

import (
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"sync"
)

var wg = sync.WaitGroup{}

type uploadReq struct {
	src  string
	dest string
}

func recognizeTypeByName(n, def string) string {
	byname := mime.TypeByExtension(n)
	switch {
	case byname != "":
		return byname
	case strings.HasSuffix(n, ".js"):
		return "application/javascript"
	}
	return def
}

func uploadFile(src, dest string) error {
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
	preq.Header.Set("X-CBFS-KeepRevs", strconv.Itoa(*revs))

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
	return nil
}

func uploader(ch chan uploadReq) {
	defer wg.Done()
	for req := range ch {
		log.Printf("%v -> %v", req.src, req.dest)
		retries := 0
		done := false
		for !done {
			err := uploadFile(req.src, req.dest)
			if err != nil {
				if retries < 3 {
					retries++
					log.Printf("Error uploading file: %v... retrying",
						err)
					time.Sleep(time.Duration(retries) * time.Second)
				} else {
					log.Fatalf("Error uploading file: %v", err)
				}
			} else {
				done = true
			}
		}
	}
}

func syncUp(src, u string, ch chan<- uploadReq) {
	err := filepath.Walk(src,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			switch info.Mode() & os.ModeType {
			case os.ModeDir:
				// ignoring quietly
			case os.ModeCharDevice, os.ModeDevice,
				os.ModeNamedPipe, os.ModeSocket, os.ModeSymlink:

				log.Printf("Ignoring special file: %v", path)
			default:
				shortPath := path[len(src):]
				ch <- uploadReq{path, u + shortPath}
			}
			return err
		})
	if err != nil {
		log.Fatalf("Traversal error: %v", err)
	}
}

func uploadCommand(args []string) {
	ch := make(chan uploadReq)

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go uploader(ch)
	}

	syncUp(args[0], args[1], ch)

	close(ch)
	wg.Wait()
}
