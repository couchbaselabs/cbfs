package main

import (
	"encoding/json"
	"log"
	"net/http"
)

// Take a stream of namedFiles and clump them into batches of at most
// the specified size
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
		}
		if len(res) > 0 {
			outch <- res
		}
	}()

	return outch
}

func dofsck(c *Container, w http.ResponseWriter, req *http.Request,
	path string) {

	errsOnly := req.FormValue("errsonly") != ""

	quit := make(chan bool)
	defer close(quit)
	ch := make(chan *namedFile)
	cherr := make(chan error)

	go pathGenerator(path, ch, cherr, quit)

	go func() {
		for e := range cherr {
			log.Printf("View error: %v", e)
		}
	}()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
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
		fnmap := map[string][]string{}
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

			a, ok := fnmap[nf.meta.OID]
			if ok {
				a = append(a, nf.name)
			} else {
				a = []string{nf.name}
			}
			fnmap[nf.meta.OID] = a
		}

		bres, err := couchbase.GetBulk(keys)
		if err != nil {
			log.Printf("Error getting bulk keys: %v", err)
			return
		}

		for k, v := range bres {
			names := fnmap[k[1:]]
			for _, name := range names {
				delete(unprocessed, name)
			}

			ownership := BlobOwnership{}
			err := json.Unmarshal(v.Body, &ownership)
			if err != nil {
				for _, name := range names {
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
			}

			if !errsOnly {
				for _, name := range names {
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
		}

		for k, v := range unprocessed {
			// If we didn't get it in the first pass, try harder.
			_, err := getBlobOwnership(v)
			if err == nil {
				log.Printf("Got %v on the second try", v)
				continue
			}
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
