package main

import (
	"archive/zip"
	"fmt"
	"log"
	"net/http"
	"strings"
)

func archiveFilename(path, ext string) string {
	parts := strings.Split(path, "/")
	filename := "cbfs-archive." + ext
	for len(parts) > 0 {
		if p := parts[len(parts)-1]; p != "" {
			filename = p + "." + ext
			break
		}
		parts = parts[:len(parts)-1]
	}
	return filename
}

func doZipDocs(c *Container, w http.ResponseWriter, req *http.Request,
	path string) {

	quit := make(chan bool)
	defer close(quit)
	ch := make(chan *namedFile)
	cherr := make(chan error)

	go c.pathGenerator(path, ch, cherr, quit)
	go logErrors("zip", cherr)

	w.Header().Set("Content-Disposition",
		fmt.Sprintf("attachment; filename=%q", archiveFilename(path, "zip")))
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
