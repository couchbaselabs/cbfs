package main

import (
	"archive/zip"
	"log"
	"net/http"
)

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
