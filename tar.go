package main

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"log"
	"net/http"
)

func doTarDocs(c *Container, w http.ResponseWriter, req *http.Request,
	path string) {

	quit := make(chan bool)
	defer close(quit)
	ch := make(chan *namedFile)
	cherr := make(chan error)

	go c.pathGenerator(path, ch, cherr, quit)
	go logErrors("tar", cherr)

	w.Header().Set("Content-Disposition",
		fmt.Sprintf("attachment; filename=%q", archiveFilename(path, "tar")))
	w.Header().Set("Content-Type", "application/x-tar")

	if canGzip(req) {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		w = &geezyWriter{w, gz}
	}

	w.WriteHeader(200)

	tw := tar.NewWriter(w)
	for nf := range ch {
		if nf.err != nil {
			log.Printf("Error on %v: %v", nf.name, nf.err)
			continue
		}

		fh := tar.Header{
			Name:    nf.name,
			Mode:    0644,
			Size:    nf.meta.Length,
			ModTime: nf.meta.Modified,
		}

		err := tw.WriteHeader(&fh)
		if err != nil {
			log.Printf("Error writing header %#v: %v", fh, err)
			continue
		}

		err = copyBlob(tw, nf.meta.OID)
		if err != nil {
			log.Printf("Error copying blob for %v: %v",
				nf.name, err)
			// Client will get a broken tar file
			return
		}
	}
	tw.Close()
}
