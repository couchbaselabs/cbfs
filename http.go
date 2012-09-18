package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

const (
	blobPrefix = "/.cbfs/blob/"
	metaPrefix = "/.cbfs/meta/"
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
			a := StorageNode{}
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
		_, err := mc.CAS(vb, k, func(in []byte) ([]byte, memcached.CasOp) {
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
			return mustEncode(&ownership), memcached.CASStore
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

type storInfo struct {
	node string
	hs   string
	err  error
}

// Given a Reader, we produce a new reader that will duplicate the
// stream into the next available node and reproduce that content into
// another node.  Iff that node successfully stores the content, we
// return the hash it computed.
//
// The returned Reader must be consumed until the input EOFs or is
// closed.  The returned channel may yield a storInfo struct before
// it's closed.  If it's closed without yielding a storInfo, there are
// no remote nodes available.
func altStoreFile(r io.Reader) (io.Reader, <-chan storInfo) {
	bgch := make(chan storInfo, 1)

	nodes := findRemoteNodes()
	if len(nodes) > 0 {
		r1, r2 := newMultiReader(r)
		r = r2

		go func() {
			defer close(bgch)

			rv := storInfo{node: nodes[0].Address()}

			rurl := "http://" +
				nodes[0].Address() + blobPrefix
			log.Printf("Piping secondary storage to %v",
				nodes[0].Address())
			preq, err := http.NewRequest("POST", rurl, r1)
			if err != nil {
				rv.err = err
				bgch <- rv
				return
			}

			presp, err := http.DefaultClient.Do(preq)
			if err == nil {
				if presp.StatusCode != 201 {
					rv.err = errors.New(presp.Status)
					bgch <- rv
				}
				_, err := io.Copy(ioutil.Discard, presp.Body)
				if err == nil {
					rv.hs = presp.Header.Get("X-Hash")
				}
				presp.Body.Close()
			} else {
				log.Printf("Error http'n to %v: %v", rurl, err)
				io.Copy(ioutil.Discard, r1)
			}
			rv.err = err
			bgch <- rv
		}()
	} else {
		close(bgch)
	}

	return r, bgch
}

func doPostRawBlob(w http.ResponseWriter, req *http.Request) {
	f, err := NewHashRecord(*root, "")
	if err != nil {
		log.Printf("Error writing tmp file: %v", err)
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	defer f.Close()

	sh, length, err := f.Process(req.Body)
	if err != nil {
		log.Printf("Error linking in raw hash: %v", err)
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}

	err = recordBlobOwnership(sh, length)
	if err != nil {
		log.Printf("Error recording blob ownership: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error recording blob ownership: %v", err)
		return
	}

	w.Header().Set("X-Hash", sh)

	w.WriteHeader(201)
}

func putUserFile(w http.ResponseWriter, req *http.Request) {
	f, err := NewHashRecord(*root, "")
	if err != nil {
		log.Printf("Error writing tmp file: %v", err)
		w.WriteHeader(500)
		return
	}
	defer f.Close()

	r, bgch := altStoreFile(req.Body)

	h, length, err := f.Process(r)
	if err != nil {
		log.Printf("Error completing blob write: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error completing blob write: %v", err)
		return
	}

	log.Printf("Wrote %v -> %v (%#v)", req.URL.Path, h, req.Header)

	fm := fileMeta{
		Headers:  req.Header,
		OID:      h,
		Length:   length,
		Modified: time.Now(),
	}

	err = recordBlobOwnership(h, length)
	if err != nil {
		log.Printf("Error storing blob ownership: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error recording blob ownership: %v", err)
		return
	}

	if si, hasStuff := <-bgch; hasStuff {
		if si.err != nil || si.hs != h {
			log.Printf("Error in secondary store to %v: %v",
				si.node, si.err)
			w.WriteHeader(500)
			fmt.Fprintf(w, "Error creating secondary copy: %v\n%v",
				si.err, si.hs)
			return
		}
	}

	err = storeMeta(resolvePath(req), fm)
	if err != nil {
		log.Printf("Error storing file meta: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error recording blob ownership: %v", err)
		return
	}

	w.WriteHeader(201)
}

func putRawHash(w http.ResponseWriter, req *http.Request) {
	inputhash := minusPrefix(req.URL.Path, blobPrefix)

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

	sh, length, err := f.Process(req.Body)
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

	w.Header().Set("X-Hash", sh)

	w.WriteHeader(201)
}

func doPut(w http.ResponseWriter, req *http.Request) {
	switch {
	case strings.HasPrefix(req.URL.Path, blobPrefix):
		putRawHash(w, req)
	case strings.HasPrefix(req.URL.Path, metaPrefix):
		putMeta(w, req, minusPrefix(req.URL.Path, metaPrefix))
	case strings.HasPrefix(req.URL.Path, "/.cbfs/"):
		w.WriteHeader(400)
	default:
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

func doHead(w http.ResponseWriter, req *http.Request) {
	path := resolvePath(req)
	got := fileMeta{}
	err := couchbase.Get(path, &got)
	if err != nil {
		log.Printf("Error getting file %#v: %v", path, err)
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
		return
	}

	for k, v := range got.Headers {
		if isResponseHeader(k) {
			w.Header()[k] = v
		}
	}
	w.Header().Set("Last-Modified",
		got.Modified.UTC().Format(http.TimeFormat))
	w.Header().Set("Etag", `"`+got.OID+`"`)

	w.WriteHeader(200)
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

	inm := req.Header.Get("If-None-Match")
	if len(inm) > 2 {
		inm = inm[1 : len(inm)-1]
		if got.OID == inm {
			w.WriteHeader(304)
			return
		}
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

	w.Header().Set("Etag", `"`+got.OID+`"`)

	go recordBlobAccess(got.OID)
	http.ServeContent(w, req, path, got.Modified, f)
}

func doServeRawBlob(w http.ResponseWriter, req *http.Request, oid string) {
	f, err := os.Open(hashFilename(*root, oid))
	if err != nil {
		w.WriteHeader(404)
		fmt.Fprintf(w, "Error opening blob: %v", err)
		removeBlobOwnershipRecord(oid, serverId)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "application/octet-stream")

	go recordBlobAccess(oid)
	http.ServeContent(w, req, "", time.Time{}, f)
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

		resp, err := http.Get(sid.BlobURL(meta.OID))
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

func putMeta(w http.ResponseWriter, req *http.Request, path string) {
	got := fileMeta{}
	casid := uint64(0)
	err := couchbase.Gets(path, &got, &casid)
	if err != nil {
		log.Printf("Error getting file %#v: %v", path, err)
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
		return
	}

	r := json.RawMessage{}
	err = json.NewDecoder(req.Body).Decode(&r)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	got.Userdata = &r
	b := mustEncode(&got)

	err = couchbase.Do(path, func(mc *memcached.Client, vb uint16) error {
		req := &gomemcached.MCRequest{
			Opcode:  gomemcached.SET,
			VBucket: vb,
			Key:     []byte(path),
			Cas:     casid,
			Opaque:  0,
			Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
			Body:    b}
		resp, err := mc.Send(req)
		if err != nil {
			return err
		}
		if resp.Status != gomemcached.SUCCESS {
			return resp
		}
		return nil
	})

	if err == nil {
		w.WriteHeader(201)
	} else {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
	}
}

func doGetMeta(w http.ResponseWriter, req *http.Request, path string) {
	got := fileMeta{}
	err := couchbase.Get(path, &got)
	if err != nil {
		log.Printf("Error getting file %#v: %v", path, err)
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	if got.Userdata == nil {
		w.Write([]byte("{}"))
	} else {
		w.Write(*got.Userdata)
	}
}

func doListNodes(w http.ResponseWriter, req *http.Request) {
	viewRes := struct {
		Rows []struct {
			Key   string
			Value float64
		}
	}{}

	err := couchbase.ViewCustom("cbfs", "node_size",
		map[string]interface{}{
			"group_level": 1,
		}, &viewRes)
	if err != nil {
		log.Printf("Error executing nodes view: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error generating node list: %v", err)
		return
	}

	nodeSizes := map[string]float64{}
	nodeKeys := []string{}
	for _, r := range viewRes.Rows {
		nodeSizes[r.Key] = r.Value
		nodeKeys = append(nodeKeys, "/"+r.Key)
	}

	respob := map[string]interface{}{}
	for nid, mcresp := range couchbase.GetBulk(nodeKeys) {
		if mcresp.Status != gomemcached.SUCCESS {
			log.Printf("Error fetching %v: %v", nid, mcresp)
			continue
		}

		node := StorageNode{}
		err = json.Unmarshal(mcresp.Body, &node)
		if err != nil {
			log.Printf("Error unmarshalling storage node %v: %v",
				nid, err)
			continue
		}

		nid = nid[1:]
		age := time.Since(node.Time)
		respob[nid] = map[string]interface{}{
			"size":      nodeSizes[nid],
			"addr":      node.Address(),
			"hbtime":    node.Time,
			"hbage_ms":  age.Nanoseconds() / 1e6,
			"hbage_str": age.String(),
			"hash":      node.Hash,
			"addr_raw":  node.Addr,
			"bindaddr":  node.BindAddr,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(mustEncode(respob))
}

func doGet(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == blobPrefix:
		doList(w, req)
	case req.URL.Path == "/.cbfs/nodes/":
		doListNodes(w, req)
	case strings.HasPrefix(req.URL.Path, metaPrefix):
		doGetMeta(w, req,
			minusPrefix(req.URL.Path, metaPrefix))
	case strings.HasPrefix(req.URL.Path, blobPrefix):
		doServeRawBlob(w, req, minusPrefix(req.URL.Path, blobPrefix))
	case strings.HasPrefix(req.URL.Path, "/.cbfs/"):
		w.WriteHeader(400)
	default:
		doGetUserDoc(w, req)
	}
}

func minusPrefix(s, prefix string) string {
	return s[len(prefix):]
}

func doDeleteOID(w http.ResponseWriter, req *http.Request) {
	oid := minusPrefix(req.URL.Path, blobPrefix)
	err := os.Remove(hashFilename(*root, oid))
	if err == nil {
		w.WriteHeader(204)
	} else {
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
	}
}

func doDeleteUserDoc(w http.ResponseWriter, req *http.Request) {
	err := couchbase.Delete(resolvePath(req))
	if err == nil {
		w.WriteHeader(204)
	} else {
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
	}
}

func doDelete(w http.ResponseWriter, req *http.Request) {
	switch {
	case strings.HasPrefix(req.URL.Path, blobPrefix):
		doDeleteOID(w, req)
	case strings.HasPrefix(req.URL.Path, "/.cbfs/"):
		w.WriteHeader(400)
	default:
		doDeleteUserDoc(w, req)
	}
}

func doPost(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == blobPrefix {
		doPostRawBlob(w, req)
	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func httpHandler(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	switch req.Method {
	case "PUT":
		doPut(w, req)
	case "POST":
		doPost(w, req)
	case "GET":
		doGet(w, req)
	case "HEAD":
		doHead(w, req)
	case "DELETE":
		doDelete(w, req)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
