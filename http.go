package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/gomemcached/client"
)

type BlobOwnership struct {
	OID    string               `json:"oid"`
	Length int64                `json:"length"`
	Nodes  map[string]time.Time `json:"nodes"`
	Type   string               `json:"type"`
}

func recordBlobOwnership(h string, l int64) error {
	sid := serverIdentifier()

	k := "/" + h
	return couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		_, err := mc.CAS(vb, k, func(in []byte) []byte {
			ownership := BlobOwnership{}
			err := json.Unmarshal(in, &ownership)
			if err == nil {
				ownership.Nodes[sid] = time.Now().UTC()
			} else {
				ownership.Nodes = map[string]time.Time{
					sid: time.Now().UTC(),
				}
			}
			ownership.OID = h
			ownership.Length = l
			ownership.Type = "blobowner"

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

	_, err = couchbase.Incr("/"+serverIdentifier()+"/r", 1, 1, 0)
	if err != nil {
		log.Printf("Error incrementing node identifier: %v", h, err)
	}
}

func putUserFile(w http.ResponseWriter, req *http.Request) {
	sh := getHash()

	tmpf, err := ioutil.TempFile(*root, "tmp")
	if err != nil {
		log.Printf("Error writing tmp file: %v", err)
		w.WriteHeader(500)
		return
	}

	length, err := io.Copy(io.MultiWriter(tmpf, sh), req.Body)
	if err != nil {
		log.Printf("Error writing data from client: %v", err)
		w.WriteHeader(500)
		return
	}

	h := hex.EncodeToString(sh.Sum([]byte{}))
	fn := hashFilename(h)

	err = os.Rename(tmpf.Name(), fn)
	if err != nil {
		os.MkdirAll(filepath.Dir(fn), 0777)
		err = os.Rename(tmpf.Name(), fn)
		if err != nil {
			log.Printf("Error renaming %v to %v: %v", tmpf.Name(), fn, err)
			w.WriteHeader(500)
			os.Remove(tmpf.Name())
			return
		}
	}

	log.Printf("Wrote %v -> %v (%#v)", req.URL.Path, h, req.Header)

	fm := fileMeta{
		req.Header,
		h,
		length,
	}

	err = storeMeta(req.URL.Path[1:], fm)
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

	tmpf, err := ioutil.TempFile(*root, "tmp")
	if err != nil {
		log.Printf("Error writing tmp file: %v", err)
		w.WriteHeader(500)
		os.Remove(tmpf.Name())
		return
	}

	sh := getHash()
	length, err := io.Copy(io.MultiWriter(tmpf, sh), req.Body)
	if err != nil {
		log.Printf("Error writing data from client: %v", err)
		w.WriteHeader(500)
		os.Remove(tmpf.Name())
		return
	}

	h := hex.EncodeToString(sh.Sum([]byte{}))
	if h != inputhash {
		w.WriteHeader(400)
		fmt.Fprintf(w, "Content hashed to %v, expected %v", h, inputhash)
		os.Remove(tmpf.Name())
		return

	}
	fn := hashFilename(h)

	err = os.Rename(tmpf.Name(), fn)
	if err != nil {
		os.MkdirAll(filepath.Dir(fn), 0777)
		err = os.Rename(tmpf.Name(), fn)
		if err != nil {
			log.Printf("Error renaming %v to %v: %v", tmpf.Name(), fn, err)
			w.WriteHeader(500)
			os.Remove(tmpf.Name())
			return
		}
	}

	err = recordBlobOwnership(h, length)
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

func doGetUserDoc(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path
	if path == "/" {
		path = *defaultPath
	}

	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	got := fileMeta{}
	err := couchbase.Get(path, &got)
	if err != nil {
		log.Printf("Error getting file %#v: %v", path, err)
		w.WriteHeader(404)
		return
	}

	log.Printf("Need to find blob %v", got.OID)

	f, err := os.Open(hashFilename(got.OID))
	if err != nil {
		getBlobFromRemote(w, got.OID)
		return;
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

func getBlobFromRemote(w http.ResponseWriter, oid string) {

	// Find the owners of this blob
	ownership := BlobOwnership{}
	oidkey := "/" + oid
	err := couchbase.Get(oidkey, &ownership)
	if err != nil {
		log.Printf("Missing ownership record for OID: %v", oid)
		// Not sure 404 is the right response here
		w.WriteHeader(404)
		return
	}

	// Loop through the nodes that claim to own this blob
	// If we encounter any errors along the way, try the next node
	for sid, _ := range ownership.Nodes {
	   log.Printf("Trying to get %s from %s", oid, sid)
		sidaddr, err := getNodeAddress(sid)
		if err != nil {
			log.Printf("Missing node record for %s", sid)
			continue
		}

		remoteOidURL := fmt.Sprintf("http://%s/?oid=%s", sidaddr, oid)
		resp, err := http.Get(remoteOidURL)
		if err != nil {
			log.Printf("Error reading oid %s from node %s", oid, sid)
			continue
		}

		if resp.StatusCode != 200 {
			log.Printf("Error response code %d from node %s", resp.StatusCode, sid)
			continue
		}

		_, err2 := io.Copy(w, resp.Body)
		if err2 != nil {
			log.Printf("Failed to write from remote stream %v", err2)
		}
		return
	}

	//if we got to this point, no node in the list actually had it
	log.Printf("Don't have hash file: %v and no remote nodes could help", oid)
	w.WriteHeader(500)
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
		http.ServeFile(w, req, hashFilename(req.FormValue("oid")))
	case req.URL.Path == "/" && req.FormValue("list") != "":
		doList(w, req)
	default:
		doGetUserDoc(w, req)
	}
}

func httpHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	switch req.Method {
	case "PUT":
		doPut(w, req)
	case "GET":
		doGet(w, req)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
