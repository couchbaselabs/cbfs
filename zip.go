package main

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	cb "github.com/couchbaselabs/go-couchbase"
)

var missingFile = errors.New("missing file")

type namedFile struct {
	name string
	meta fileMeta
	err  error
}

func pathGenerator(from string, ch chan *namedFile,
	errs chan error, quit chan bool) {
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

	limit := 1000
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

		paths := map[string]*namedFile{}
		keys := []string{}

		for _, r := range viewRes.Rows {
			k := r.Id
			if !strings.HasPrefix(k, from) {
				done = true
				break
			}
			startKey = r.Key

			nf := namedFile{name: k, err: missingFile}

			paths[k] = &nf
			keys = append(keys, k)
		}

		for k, res := range couchbase.GetBulk(keys) {
			ob := paths[k]
			ob.err = json.Unmarshal(res.Body, &ob.meta)
			if ob.err != nil {
				log.Printf("Error unmarshaling %v: %v", k, ob.err)
			}
			select {
			case <-quit:
				return
			case ch <- ob:
				// We sent one.
			}
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
