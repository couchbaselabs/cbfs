package main

import (
	"archive/zip"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"

	cb "github.com/couchbaselabs/go-couchbase"
)

var missingFile = errors.New("missing file")

type namedFile struct {
	name string
	meta fileMeta
	err  error
}

func pathDataFetcher(wg *sync.WaitGroup, quit <-chan bool,
	in <-chan string, out chan<- *namedFile) {
	defer wg.Done()

	for {
		select {
		case s, ok := <-in:
			if !ok {
				return
			}
			ob := namedFile{name: s}
			ob.err = couchbase.Get(s, &ob.meta)
			out <- &ob
		case <-quit:
			return
		}
	}
}

func pathGenerator(from string, ch chan *namedFile,
	errs chan error, quit chan bool) {

	parts := strings.Split(from, "/")

	viewRes := struct {
		Rows []struct {
			Key []string
			Id  string
		}
		Errors []cb.ViewError
	}{}

	limit := 1000
	fetchch := make(chan string, limit)
	startKey := parts
	done := false

	wg := &sync.WaitGroup{}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go pathDataFetcher(wg, quit, fetchch, ch)
	}
	defer func() {
		close(fetchch)
		wg.Wait()
		close(ch)
		close(errs)
	}()

	for !done {
		err := couchbase.ViewCustom("cbfs", "file_browse",
			map[string]interface{}{
				"stale":    false,
				"reduce":   false,
				"limit":    limit,
				"startkey": startKey,
			}, &viewRes)
		if err != nil {
			log.Printf("View error: %v", err)
			errs <- err
			return
		}
		for _, e := range viewRes.Errors {
			select {
			case errs <- e:
			case <-quit:
				return
			}
		}

		done = len(viewRes.Rows) < limit

		for _, r := range viewRes.Rows {
			k := r.Id
			if !strings.HasPrefix(k, from) {
				done = true
				break
			}
			startKey = r.Key

			fetchch <- k
		}
	}
}

func doZipDocs(w http.ResponseWriter, req *http.Request,
	path string) {

	quit := make(chan bool)
	defer close(quit)
	ch := make(chan *namedFile)
	cherr := make(chan error)

	go pathGenerator(path, ch, cherr, quit)
	go logErrors("zip", cherr)

	w.Header().Set("Content-Type", "application/zip")
	w.WriteHeader(200)

	zw := zip.NewWriter(w)
	for nf := range ch {
		if nf.err != nil {
			log.Printf("Error on %v: %v", nf.name, nf.err)
			continue
		}

		fh := zip.FileHeader{
			Name:             nf.name,
			Method:           zip.Deflate,
			UncompressedSize: uint32(nf.meta.Length),
			Comment:          nf.meta.OID,
		}
		fh.SetModTime(nf.meta.Modified)

		zf, err := zw.CreateHeader(&fh)
		if err != nil {
			log.Printf("Error making zip file: %v", err)
			// Client will get a broken zip file
			return
		}

		err = copyBlob(zf, nf.meta.OID)
		if err != nil {
			log.Printf("Error copying blob for %v: %v",
				nf.name, err)
			// Client will get a broken zip file
			return
		}
	}
	zw.Close()
}
