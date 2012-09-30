package main

import (
	"encoding/json"
	"log"
	"net/http"

	cb "github.com/couchbaselabs/go-couchbase"
)

// Take a stream of 
func keyClumper(ch chan *namedFile, size int) chan []*namedFile {
	outch := make(chan []*namedFile)
	go func() {
		defer close(outch)
		res := make([]*namedFile, 0, size)
		for r := range ch {
			res = append(res, r)
			if len(res) == size {
				outch <- res
				res = make([]*namedFile, 0, size)
			}
			if len(res) > 0 {
				outch <- res
			}
		}
	}()

	return outch
}

func dofsck(w http.ResponseWriter, req *http.Request,
	path string) {

	errsOnly := req.FormValue("errsonly") != ""

	quit := make(chan bool)
	defer close(quit)
	ch := make(chan *namedFile)
	cherr := make(chan cb.ViewError)

	go pathGenerator(path, ch, cherr, quit)

	go func() {
		for e := range cherr {
			log.Printf("View error: %v", e)
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)

	e := json.NewEncoder(w)
	type status struct {
		Path  string `json:"path"`
		OID   string `json:"oid,omitempty"`
		Reps  int    `json:"reps,omitempty"`
		EType string `json:"etype,omitempty"`
		Error string `json:"error,omitempty"`
	}

	for nfc := range keyClumper(ch, 1000) {
		keys := []string{}
		fnmap := map[string]string{}
		unprocessed := map[string]string{}

		for _, nf := range nfc {
			if nf.err != nil {
				if err := e.Encode(status{
					Path:  nf.name,
					OID:   nf.meta.OID,
					EType: "file",
					Error: nf.err.Error(),
				}); err != nil {
					log.Printf("Error encoding: %v", err)
					return
				}
			}
			keys = append(keys, "/"+nf.meta.OID)
			unprocessed[nf.name] = nf.meta.OID
			fnmap[nf.meta.OID] = nf.name
		}

		for k, v := range couchbase.GetBulk(keys) {
			name := fnmap[k[1:]]
			delete(unprocessed, name)

			ownership := BlobOwnership{}
			err := json.Unmarshal(v.Body, &ownership)
			if err != nil {
				if err = e.Encode(status{
					Path:  name,
					OID:   k[1:],
					EType: "blob",
					Error: err.Error(),
				}); err != nil {
					log.Printf("Error encoding: %v", err)
					return
				}
			}

			if !errsOnly {
				if err := e.Encode(status{
					Path: name,
					OID:  k[1:],
					Reps: len(ownership.Nodes),
				}); err != nil {
					log.Printf("Error encoding: %v", err)
					return
				}
			}
		}

		for k, v := range unprocessed {
			if err := e.Encode(status{
				Path:  k,
				OID:   v,
				EType: "blob",
				Error: "not found",
			}); err != nil {
				log.Printf("Error encoding: %v", err)
				return
			}
		}
	}
}
