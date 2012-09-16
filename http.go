package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

type BlobOwnership struct {
	OID    string               `json:"oid"`
	Length int64                `json:"length"`
	Nodes  map[string]time.Time `json:"nodes"`
	Type   string               `json:"type"`
}

func (b BlobOwnership) ResolveRemoteNodes() NodeList {
	keys := make([]string, 0, len(b.Nodes))
	for k := range b.Nodes {
		if k != serverId {
			keys = append(keys, "/"+k)
		}
	}
	resps := couchbase.GetBulk(keys)

	rv := make(NodeList, 0, len(resps))

	for _, v := range resps {
		if v.Status == gomemcached.SUCCESS {
			a := AboutNode{}
			err := json.Unmarshal(v.Body, &a)
			if err == nil {
				rv = append(rv, a)
			}
		}
	}

	sort.Sort(rv)

	return rv
}

func recordBlobOwnership(h string, l int64) error {
	k := "/" + h
	return couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		_, err := mc.CAS(vb, k, func(in []byte) []byte {
			ownership := BlobOwnership{}
			err := json.Unmarshal(in, &ownership)
			if err == nil {
				ownership.Nodes[serverId] = time.Now().UTC()
			} else {
				ownership.Nodes = map[string]time.Time{
					serverId: time.Now().UTC(),
				}
			}
			ownership.OID = h
			ownership.Length = l
			ownership.Type = "blob"

			rv, err := json.Marshal(&ownership)
			if err != nil {
				log.Fatalf("Error marshaling blob ownership: %v", err)
			}
			return rv
		}, 0)
		return err
	})
}

func recordBlobAccess(h string) {
	_, err := couchbase.Incr("/"+h+"/r", 1, 1, 0)
	if err != nil {
		log.Printf("Error incrementing counter for %v: %v", h, err)
	}

	_, err = couchbase.Incr("/"+serverId+"/r", 1, 1, 0)
	if err != nil {
		log.Printf("Error incrementing node identifier: %v", h, err)
	}
}

func putUserFile(w http.ResponseWriter, req *http.Request) {
	f, err := NewHashRecord(*root, "")
	if err != nil {
		log.Printf("Error writing tmp file: %v", err)
		w.WriteHeader(500)
		return
	}
	defer f.Close()

	h, length, err := f.Process(req.Body)
	if err != nil {
		log.Printf("Error completing blob write: %v", err)
	}

	log.Printf("Wrote %v -> %v (%#v)", req.URL.Path, h, req.Header)

	fm := fileMeta{
		req.Header,
		h,
		length,
	}

	err = storeMeta(resolvePath(req), fm)
	if err != nil {
		log.Printf("Error storing file meta: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error recording blob ownership: %v", err)
		return
	}

	err = recordBlobOwnership(h, length)
	if err != nil {
		log.Printf("Error storing blob ownership: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error recording blob ownership: %v", err)
		return
	}

	w.WriteHeader(204)
}

func putRawHash(w http.ResponseWriter, req *http.Request) {
	inputhash := ""
	form, err := url.ParseQuery(req.URL.RawQuery)
	if err == nil {
		inputhash = form.Get("oid")
	}

	if inputhash == "" {
		w.WriteHeader(400)
		w.Write([]byte("No oid specified"))
		return
	}

	f, err := NewHashRecord(*root, inputhash)
	if err != nil {
		log.Printf("Error writing tmp file: %v", err)
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	defer f.Close()

	_, length, err := f.Process(req.Body)
	if err != nil {
		log.Printf("Error linking in raw hash: %v", err)
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	err = recordBlobOwnership(inputhash, length)
	if err != nil {
		log.Printf("Error recording blob ownership: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error recording blob ownership: %v", err)
		return
	}

	w.WriteHeader(204)
}

func doPut(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/" {
		putRawHash(w, req)
	} else {
		putUserFile(w, req)
	}
}

func isResponseHeader(s string) bool {
	switch s {
	case "Content-Type", "Content-Length":
		return true
	}
	return false
}

func resolvePath(req *http.Request) string {
	path := req.URL.Path
	if path == "/" {
		path = *defaultPath
	}

	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	return path
}

func doGetUserDoc(w http.ResponseWriter, req *http.Request) {
	path := resolvePath(req)
	got := fileMeta{}
	err := couchbase.Get(path, &got)
	if err != nil {
		log.Printf("Error getting file %#v: %v", path, err)
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
		return
	}

	f, err := os.Open(hashFilename(*root, got.OID))
	if err != nil {
		getBlobFromRemote(w, got)
		return
	}
	defer f.Close()

	for k, v := range got.Headers {
		if isResponseHeader(k) {
			w.Header()[k] = v
		}
	}
	w.WriteHeader(200)

	_, err = io.Copy(w, f)
	if err != nil {
		log.Printf("Failed to write file: %v", err)
	}
	go recordBlobAccess(got.OID)
}

func getBlobFromRemote(w http.ResponseWriter, meta fileMeta) {

	// Find the owners of this blob
	ownership := BlobOwnership{}
	oidkey := "/" + meta.OID
	err := couchbase.Get(oidkey, &ownership)
	if err != nil {
		log.Printf("Missing ownership record for OID: %v", meta.OID)
		// Not sure 404 is the right response here
		w.WriteHeader(404)
		return
	}

	nl := ownership.ResolveRemoteNodes()

	// Loop through the nodes that claim to own this blob
	// If we encounter any errors along the way, try the next node
	for _, sid := range nl {
		log.Printf("Trying to get %s from %s", meta.OID, sid)

		remoteOidURL := fmt.Sprintf("http://%s/?oid=%s",
			sid.Address(), meta.OID)
		resp, err := http.Get(remoteOidURL)
		if err != nil {
			log.Printf("Error reading oid %s from node %s", meta.OID, sid)
			continue
		}

		if resp.StatusCode != 200 {
			log.Printf("Error response code %d from node %s", resp.StatusCode, sid)
			continue
		}

		// Found one, set the headers and send it.  Keep a
		// local copy for good luck.

		for k, v := range meta.Headers {
			if isResponseHeader(k) {
				w.Header()[k] = v
			}
		}
		w.WriteHeader(200)
		writeTo := io.Writer(w)
		var hw *hashRecord

		if *cachePercentage > rand.Intn(100) {
			log.Printf("Storing remotely proxied request")
			hw, err = NewHashRecord(*root, meta.OID)
			if err == nil {
				writeTo = io.MultiWriter(hw, w)
			} else {
				hw = nil
			}
		}

		length, err := io.Copy(writeTo, resp.Body)

		if err != nil {
			log.Printf("Failed to write from remote stream %v", err)
		} else {
			// A successful copy with a working hash
			// record means we should link in and record
			// our copy of this file.
			if hw != nil {
				_, err = hw.Finish()
				if err == nil {
					go recordBlobOwnership(meta.OID, length)
				}
			}
		}

		return
	}

	//if we got to this point, no node in the list actually had it
	log.Printf("Don't have hash file: %v and no remote nodes could help",
		meta.OID)
	w.WriteHeader(500)
	fmt.Fprintf(w, "Cannot locate blob %v", meta.OID)
	return
}

func doList(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
	explen := getHash().Size() * 2
	filepath.Walk(*root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && !strings.HasPrefix(info.Name(), "tmp") &&
			len(info.Name()) == explen {
			_, e := w.Write([]byte(info.Name() + "\n"))
			return e
		}
		return nil
	})
}

func doGet(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == "/" && req.FormValue("oid") != "":
		http.ServeFile(w, req, hashFilename(*root, req.FormValue("oid")))
	case req.URL.Path == "/" && req.FormValue("list") != "":
		doList(w, req)
	default:
		doGetUserDoc(w, req)
	}
}

func doDeleteOID(w http.ResponseWriter, req *http.Request) {
	oid := req.FormValue("oid")
	err := os.Remove(hashFilename(*root, oid))
	if err == nil {
		w.WriteHeader(201)
	} else {
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
	}
}

func doDeleteUserDoc(w http.ResponseWriter, req *http.Request) {
	err := couchbase.Delete(resolvePath(req))
	if err == nil {
		w.WriteHeader(201)
	} else {
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
	}
}

func doDelete(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == "/" && req.FormValue("oid") != "":
		doDeleteOID(w, req)
	default:
		doDeleteUserDoc(w, req)
	}
}

func httpHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	switch req.Method {
	case "PUT":
		doPut(w, req)
	case "GET":
		doGet(w, req)
	case "DELETE":
		doDelete(w, req)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
