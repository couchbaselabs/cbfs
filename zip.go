package main

import (
	"archive/zip"
	"log"
	"net/http"
	"strings"

	cb "github.com/couchbaselabs/go-couchbase"
)

type namedFile struct {
	name string
	meta fileMeta
}

func pathGenerator(from string, ch chan namedFile,
	errs chan cb.ViewError, quit chan bool) {
	defer close(ch)
	defer close(errs)

	parts := strings.Split(from, "/")

	viewRes := struct {
		Rows []struct {
			Key []string
			Id  string
		}
		Errors []cb.ViewError
	}{}

	limit := 100
	startKey := parts
	done := false
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
			return
		}
		for _, e := range viewRes.Errors {
			errs <- e
		}

		done = len(viewRes.Rows) < limit

		for _, r := range viewRes.Rows {
			k := r.Id
			if !strings.HasPrefix(k, from) {
				return
			}
			startKey = r.Key

			nf := namedFile{name: k}

			err = couchbase.Get(k, &nf.meta)
			if err != nil {
				log.Printf("Error fetching details of %v: %v",
					k, err)
				continue
			}

			select {
			case <-quit:
				return
			case ch <- nf:
				// We sent one.
			}
		}
	}
}

func doZipDocs(w http.ResponseWriter, req *http.Request,
	path string) {

	quit := make(chan bool)
	defer close(quit)
	ch := make(chan namedFile)
	cherr := make(chan cb.ViewError)

	go pathGenerator(path, ch, cherr, quit)

	go func() {
		for e := range cherr {
			log.Printf("View error: %v", e)
		}
	}()

	w.Header().Set("Content-Type", "application/zip")
	w.WriteHeader(200)

	zw := zip.NewWriter(w)
	for nf := range ch {
		fh := zip.FileHeader{
			Name:               nf.name,
			Method:             zip.Deflate,
			UncompressedSize64: uint64(nf.meta.Length),
			Comment:            nf.meta.OID,
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
			log.Printf("Error copying blob: %v", err)
			// Client will get a broken zip file
			return
		}
	}
	zw.Close()
}
