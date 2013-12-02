package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/gomemcached"
)

const (
	blobPrefix       = "/.cbfs/blob/"
	blobInfoPath     = "/.cbfs/blob/info/"
	nodePrefix       = "/.cbfs/nodes/"
	metaPrefix       = "/.cbfs/meta/"
	proxyPrefix      = "/.cbfs/viewproxy/"
	crudproxyPrefix  = "/.cbfs/crudproxy/"
	fetchPrefix      = "/.cbfs/fetch/"
	listPrefix       = "/.cbfs/list/"
	configPrefix     = "/.cbfs/config/"
	zipPrefix        = "/.cbfs/zip/"
	tarPrefix        = "/.cbfs/tar/"
	fsckPrefix       = "/.cbfs/fsck/"
	taskPrefix       = "/.cbfs/tasks/"
	taskinfoPrefix   = "/.cbfs/tasks/info/"
	pingPrefix       = "/.cbfs/ping/"
	fileInfoPrefix   = "/.cbfs/info/file/"
	framePrefix      = "/.cbfs/info/frames/"
	markBackupPrefix = "/.cbfs/backup/mark/"
	restorePrefix    = "/.cbfs/backup/restore/"
	backupStrmPrefix = "/.cbfs/backup/stream/"
	backupPrefix     = "/.cbfs/backup/"
	quitPrefix       = "/.cbfs/exit/"
	debugPrefix      = "/.cbfs/debug/"
)

type storInfo struct {
	node string
	hs   string
	err  error
}

func sendJson(w http.ResponseWriter, req *http.Request, ob interface{}) {
	if canGzip(req) {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		w = &geezyWriter{w, gz}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	e := json.NewEncoder(w)
	err := e.Encode(ob)
	if err != nil {
		log.Printf("Error encoding JSON output: %v", err)
	}
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
func altStoreFile(name string, r io.Reader,
	length int64) (io.Reader, <-chan storInfo) {

	bgch := make(chan storInfo, 2)
	if length == -1 {
		// No alt store requested
		close(bgch)
		return r, bgch
	}

	nodes, err := findRemoteNodes()
	nodes = nodes.withAtLeast(length)
	if err == nil && len(nodes) > 0 {
		r1, r2 := newMultiReader(r)
		r = r2

		go func() {
			defer close(bgch)

			rv := storInfo{node: nodes[0].Address()}

			rurl := "http://" + nodes[0].Address() + blobPrefix
			log.Printf("Piping secondary storage of %v to %v",
				name, nodes[0])

			preq, err := http.NewRequest("POST", rurl, r1)
			if err != nil {
				r1.CloseWithError(err)
				rv.err = err
				bgch <- rv
				return
			}

			presp, err := nodes[0].Client().Do(preq)
			if err == nil {
				if presp.StatusCode != 201 {
					rv.err = errors.New(presp.Status)
					r1.CloseWithError(rv.err)
					bgch <- rv
				}
				_, err := io.Copy(ioutil.Discard, presp.Body)
				if err == nil {
					rv.hs = presp.Header.Get("X-CBFS-Hash")
				}
				presp.Body.Close()
			} else {
				log.Printf("Error http'n %v to %v: %v", name,
					rurl, err)
			}
			rv.err = err
			bgch <- rv
		}()
	} else {
		log.Printf("Doing a single-node upload: findRemote=%v, status=%v",
			nodes, errorOrSuccess(err))
		close(bgch)
	}

	return r, bgch
}

func doPostRawBlob(w http.ResponseWriter, req *http.Request) {
	f, err := NewHashRecord(*root, "")
	if err != nil {
		log.Printf("Error writing tmp file: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	defer f.Close()

	sh, length, err := f.Process(req.Body)
	if err != nil {
		log.Printf("Error linking in raw hash: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	err = recordBlobOwnership(sh, length, true)
	if err != nil {
		log.Printf("Error recording ownership of %v: %v", sh, err)
		http.Error(w, fmt.Sprintf("Error recording blob ownership: %v", err),
			500)
		return
	}

	w.Header().Set("X-CBFS-Hash", sh)

	w.WriteHeader(201)
}

func putUserFile(w http.ResponseWriter, req *http.Request) {
	if strings.Contains(req.URL.Path, "//") {
		http.Error(w,
			fmt.Sprintf("Too many slashes in the path name: %v",
				req.URL.Path), 400)
		return
	}

	fn, _ := resolvePath(req)

	f, err := NewHashRecord(*root, req.Header.Get("X-CBFS-Hash"))
	if err != nil {
		log.Printf("Error writing tmp file: %v", err)
		http.Error(w, "Error writing tmp file", 500)
		return
	}
	defer f.Close()

	l := req.ContentLength
	if l < 1 {
		// If we don't know, guess about a meg.
		l = 1024 * 1024
	}
	if t, _ := strconv.ParseBool(req.Header.Get("X-CBFS-Unsafe")); t {
		l = -1
	}
	r, bgch := altStoreFile(fn, req.Body, l)

	h, length, err := f.Process(r)
	if err != nil {
		log.Printf("Error completing blob write for %v: %v",
			req.URL.Path, err)
		http.Error(w, fmt.Sprintf("Error completing blob write: %v", err), 500)
		return
	}

	err = recordBlobOwnership(h, length, true)
	if err != nil {
		log.Printf("Error storing blob ownership of %v for %v: %v",
			h, req.URL.Path, err)
		http.Error(w, fmt.Sprintf("Error recording blob ownership: %v", err),
			500)
		return
	}

	fm := fileMeta{
		Headers:  req.Header,
		OID:      h,
		Length:   length,
		Modified: time.Now().UTC(),
	}

	// We *should* have two replicas at this point.
	replicas := 2
	if si, hasStuff := <-bgch; hasStuff {
		if si.err != nil || si.hs != h {
			log.Printf("Error in secondary store of %v to %v for %v: %v",
				h, si.node, req.URL.Path, si.err)
			http.Error(w,
				fmt.Sprintf("Error creating sync secondary copy: %v\n%v",
					si.err, si.hs), 500)

			// We do have this item now, so even if it's
			// not going to be linked to a file, we will
			// increase the replica count to the minimum
			// so we don't report underreplication.
			if globalConfig.MinReplicas > 1 {
				go increaseReplicaCount(h, length,
					globalConfig.MinReplicas-1)
			}

			return
		}
	} else {
		// In this case, the upstream couldn't find a
		// secondary for us.
		log.Printf("Singly stored %v for %v", h, req.URL.Path)
		replicas--
	}

	revs := globalConfig.DefaultVersionCount
	rheader := req.Header.Get("X-CBFS-KeepRevs")
	if rheader != "" {
		i, err := strconv.Atoi(rheader)
		if err == nil {
			revs = i
		}
	}

	exp := getExpiration(req.Header)

	err = storeMeta(fn, exp, fm, revs, req.Header)
	if err == errUploadPrecondition {
		log.Printf("Upload precondition failed: %v -> %v", fn, h)
		http.Error(w, "precondition failed", 412)
		return
	}
	if err != nil {
		log.Printf("Error storing file meta of %v -> %v: %v",
			fn, h, err)
		http.Error(w, fmt.Sprintf("Error recording blob ownership: %v", err),
			500)
		return
	}

	log.Printf("Wrote %v -> %v", req.URL.Path, h)

	if globalConfig.MinReplicas > replicas {
		// We're below min replica count.  Start fixing that
		// up immediately.
		go increaseReplicaCount(h, length,
			globalConfig.MinReplicas-replicas)
	}

	w.WriteHeader(201)
}

func putRawHash(w http.ResponseWriter, req *http.Request) {
	inputhash := minusPrefix(req.URL.Path, blobPrefix)

	if inputhash == "" {
		http.Error(w, "No oid specified", 400)
		return
	}

	f, err := NewHashRecord(*root, inputhash)
	if err != nil {
		log.Printf("Error writing tmp file: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}
	defer f.Close()

	sh, length, err := f.Process(req.Body)
	if err != nil {
		log.Printf("Error linking in raw hash: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	err = recordBlobOwnership(inputhash, length, true)
	if err != nil {
		log.Printf("Error recording blob ownership of %v: %v",
			inputhash, err)
		http.Error(w, fmt.Sprintf("Error recording blob ownership: %v", err),
			500)
		return
	}

	w.Header().Set("X-CBFS-Hash", sh)

	w.WriteHeader(201)
}

func doPut(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == configPrefix:
		putConfig(w, req)
	case strings.HasPrefix(req.URL.Path, blobPrefix):
		putRawHash(w, req)
	case strings.HasPrefix(req.URL.Path, metaPrefix):
		putMeta(w, req, minusPrefix(req.URL.Path, metaPrefix))
	case *enableCRUDProxy && strings.HasPrefix(req.URL.Path, crudproxyPrefix):
		proxyCRUDPut(w, req, minusPrefix(req.URL.Path, crudproxyPrefix))
	case strings.HasPrefix(req.URL.Path, "/.cbfs/"):
		http.Error(w, "Can't PUT here", 400)
	default:
		putUserFile(w, req)
	}
}

func isResponseHeader(s string) bool {
	switch strings.ToLower(s) {
	case "content-type":
		return true
	}
	return false
}

func resolvePath(req *http.Request) (path string, key string) {
	path = req.URL.Path
	// Ignore /, but remove leading / from /blah
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	if len(path) > 0 && path[len(path)-1] == '/' {
		path = path + "index.html"
	} else if len(path) == 0 {
		path = "index.html"
	}

	return path, shortName(path)
}

func doHeadUserFile(w http.ResponseWriter, req *http.Request) {
	path, k := resolvePath(req)
	got := fileMeta{}
	err := couchbase.Get(k, &got)
	if err != nil {
		log.Printf("Error getting file %#v: %v", path, err)
		http.Error(w, err.Error(), 404)
		return
	}

	if req.FormValue("rev") != "" {
		http.Error(w, "rev parameter not specified", 400)
		return
	}

	for k, v := range got.Headers {
		if isResponseHeader(k) {
			w.Header()[k] = v
		}
	}

	oldestRev := got.Revno
	if len(got.Previous) > 0 {
		oldestRev = got.Previous[0].Revno
	}

	w.Header().Set("X-CBFS-Revno", strconv.Itoa(got.Revno))
	w.Header().Set("X-CBFS-OldestRev", strconv.Itoa(oldestRev))
	w.Header().Set("Last-Modified",
		got.Modified.UTC().Format(http.TimeFormat))
	w.Header().Set("Etag", `"`+got.OID+`"`)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", got.Length))

	w.WriteHeader(200)
}

func doHeadRawBlob(w http.ResponseWriter, req *http.Request, oid string) {
	f, err := openBlob(oid)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("Error opening blob: %v", err), 404)
		removeBlobOwnershipRecord(oid, serverId)
		return
	}
	defer f.Close()

	length, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		http.Error(w, err.Error(), 500)
		log.Printf("Error seeking in %v: %v", oid, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%v", length))
	w.WriteHeader(200)
}

func doHead(w http.ResponseWriter, req *http.Request) {
	switch {
	case strings.HasPrefix(req.URL.Path, blobPrefix):
		doHeadRawBlob(w, req, minusPrefix(req.URL.Path, blobPrefix))
	case strings.HasPrefix(req.URL.Path, "/.cbfs/"):
		http.Error(w, "Can't HEAD here", 400)
	default:
		doHeadUserFile(w, req)
	}
}

func doGetUserDoc(w http.ResponseWriter, req *http.Request) {
	path, k := resolvePath(req)
	got := fileMeta{}
	err := couchbase.Get(k, &got)
	if err != nil {
		log.Printf("Error getting file %#v: %v", path, err)
		http.Error(w, err.Error(), 404)
		return
	}
	if got.Type != "file" {
		log.Printf("%v is not a file", path)
		http.Error(w, fmt.Sprintf("Item at %v is not a file.", path), 404)
		return
	}

	oid := got.OID
	respHeaders := got.Headers
	modified := got.Modified
	revno := got.Revno
	oldestRev := revno

	if len(got.Previous) > 0 {
		oldestRev = got.Previous[0].Revno
	}

	revnoStr := req.FormValue("rev")
	if revnoStr != "" {
		i, err := strconv.Atoi(revnoStr)
		if err != nil {
			http.Error(w, "Invalid revno", 400)
			return
		}
		revno = i

		oid = ""
		for _, rev := range got.Previous {
			if rev.Revno == revno {
				oid = rev.OID
				modified = rev.Modified
				respHeaders = rev.Headers
				break
			}
		}
		if oid == "" {
			http.Error(w,
				fmt.Sprintf("Don't have this file with rev %v", revno), 410)
			return
		}
	}

	if canGzip(req) && shouldGzip(got) {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		w = &geezyWriter{w, gz}
	}

	w.Header().Set("X-CBFS-Revno", strconv.Itoa(revno))
	w.Header().Set("X-CBFS-OldestRev", strconv.Itoa(oldestRev))

	inm := req.Header.Get("If-None-Match")
	if len(inm) > 2 {
		inm = inm[1 : len(inm)-1]
		if got.OID == inm {
			w.WriteHeader(304)
			return
		}
	}

	f, err := openBlob(oid)
	switch {
	case err == nil:
		// normal path
	case req.Header.Get("X-CBFS-LocalOnly") != "":
		// Special case, just describe where things are.
		bo, err := getBlobOwnership(oid)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		urls := bo.ResolveNodes().BlobURLs(oid)
		if len(urls) == 0 {
			http.Error(w, "No alt URLs found", 500)
			return
		}
		w.Header().Set("Location", urls[0])
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(300)
		e := json.NewEncoder(w)
		e.Encode(urls)
		return
	default:
		getBlobFromRemote(w, oid, respHeaders, *cachePercentage)
		return
	}
	defer f.Close()

	for k, v := range respHeaders {
		if isResponseHeader(k) {
			w.Header()[k] = v
		}
	}

	w.Header().Set("Etag", `"`+oid+`"`)

	go recordBlobAccess(oid)
	http.ServeContent(w, req, path, modified, f)
}

func doServeRawBlob(w http.ResponseWriter, req *http.Request, oid string) {
	f, err := openBlob(oid)
	if err != nil {
		http.Error(w, "Error opening blob: "+err.Error(), 404)
		removeBlobOwnershipRecord(oid, serverId)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "application/octet-stream")

	go recordBlobAccess(oid)
	http.ServeContent(w, req, "", time.Time{}, f)
}

func getBlobFromRemote(w http.ResponseWriter, oid string,
	respHeader http.Header, cachePerc int) error {

	// Find the owners of this blob
	ownership := BlobOwnership{}
	oidkey := "/" + oid
	err := couchbase.Get(oidkey, &ownership)
	if err != nil {
		log.Printf("Missing ownership record for %v", oid)
		// Not sure 404 is the right response here
		http.Error(w, "Can't find info for blob "+oid, 404)
		return err
	}

	nl := ownership.ResolveRemoteNodes()

	// Loop through the nodes that claim to own this blob
	// If we encounter any errors along the way, try the next node
	for _, sid := range nl {
		resp, err := sid.ClientForTransfer(ownership.Length).Get(sid.BlobURL(oid))
		if err != nil {
			log.Printf("Error reading %s from node %v: %v",
				oid, sid, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			log.Printf("Error response %v from node %v getting %v",
				resp.Status, sid, oid)
			continue
		}

		// Found one, set the headers and send it.  Keep a
		// local copy for good luck.

		for k, v := range respHeader {
			if isResponseHeader(k) {
				w.Header()[k] = v
			}
		}
		w.WriteHeader(200)
		writeTo := io.Writer(w)
		var hw *hashRecord

		if cachePerc == 100 || (cachePerc > rand.Intn(100) &&
			availableSpace() > ownership.Length) {
			hw, err = NewHashRecord(*root, oid)
			if err == nil {
				writeTo = io.MultiWriter(hw, w)
			} else {
				hw = nil
			}
		}

		length, err := io.Copy(writeTo, resp.Body)

		if err != nil {
			log.Printf("Failed to write %v from remote stream %v",
				oid, err)
			return err
		} else {
			// A successful copy with a working hash
			// record means we should link in and record
			// our copy of this file.
			if hw != nil {
				_, err = hw.Finish()
				if err == nil {
					err = recordBlobOwnership(oid, length,
						true)
				}
				log.Printf("Retrieved %v from %v: result=%v",
					oid, sid, errorOrSuccess(err))
			}
		}

		return err
	}

	//if we got to this point, no node in the list actually had it
	log.Printf("Don't have hash file: %v and no remote nodes could help",
		oid)
	http.Error(w, "Cannot locate blob "+oid, 500)
	return fmt.Errorf("Can't locate blob %v", oid)
}

func canGzip(req *http.Request) bool {
	acceptable := req.Header.Get("accept-encoding")
	return strings.Contains(acceptable, "gzip")
}

type captureResponseWriter struct {
	w          io.Writer
	hdr        http.Header
	statusCode int
}

func (c *captureResponseWriter) Header() http.Header {
	return c.hdr
}

func (c *captureResponseWriter) Write(b []byte) (int, error) {
	return c.w.Write(b)
}

func (c *captureResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(c.w, r)
}

func (c *captureResponseWriter) WriteHeader(code int) {
	c.statusCode = code
}

func doFetchDoc(w http.ResponseWriter, req *http.Request,
	path string) {

	ownership := BlobOwnership{}
	oidkey := "/" + path
	err := couchbase.Get(oidkey, &ownership)
	if err != nil {
		log.Printf("Missing ownership record for OID: %v",
			path)
		// Not sure 404 is the right response here
		http.Error(w, "Missing ownership record for OID: "+path, 404)
		return
	}

	if availableSpace() < ownership.Length {
		http.Error(w, "No free space available", 500)
		log.Printf("Someone asked me to get %v, but I'm out of space",
			path)
		return
	}

	if !maybeQueueBlobFetch(path, req.Header.Get("X-Prevnode")) {
		http.Error(w, "Queue is full. Try later.", 503)
		return
	}

	increaseSpaceUsed(ownership.Length)

	w.WriteHeader(202)
}

func doGet(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == pingPrefix:
		doPing(w, req)
	case req.URL.Path == framePrefix:
		doGetFramesData(w, req)
	case req.URL.Path == blobPrefix:
		doList(w, req)
	case req.URL.Path == nodePrefix:
		doListNodes(w, req)
	case req.URL.Path == taskinfoPrefix:
		doListTaskInfo(w, req)
	case req.URL.Path == taskPrefix:
		doListTasks(w, req)
	case req.URL.Path == configPrefix:
		doGetConfig(w, req)
	case strings.HasPrefix(req.URL.Path, backupStrmPrefix):
		doExport(w, req, minusPrefix(req.URL.Path, backupStrmPrefix))
	case req.URL.Path == backupPrefix:
		doGetBackupInfo(w, req)
	case strings.HasPrefix(req.URL.Path, fileInfoPrefix):
		doFileInfo(w, req,
			minusPrefix(req.URL.Path, fileInfoPrefix))
	case strings.HasPrefix(req.URL.Path, fetchPrefix):
		doFetchDoc(w, req,
			minusPrefix(req.URL.Path, fetchPrefix))
	case strings.HasPrefix(req.URL.Path, metaPrefix):
		doGetMeta(w, req,
			minusPrefix(req.URL.Path, metaPrefix))
	case strings.HasPrefix(req.URL.Path, blobPrefix):
		doServeRawBlob(w, req, minusPrefix(req.URL.Path, blobPrefix))
	case *enableViewProxy && strings.HasPrefix(req.URL.Path, proxyPrefix):
		proxyViewRequest(w, req, minusPrefix(req.URL.Path, proxyPrefix))
	case *enableCRUDProxy && strings.HasPrefix(req.URL.Path, crudproxyPrefix):
		proxyCRUDGet(w, req, minusPrefix(req.URL.Path, crudproxyPrefix))
	case strings.HasPrefix(req.URL.Path, listPrefix):
		doListDocs(w, req, minusPrefix(req.URL.Path, listPrefix))
	case strings.HasPrefix(req.URL.Path, zipPrefix):
		doZipDocs(w, req, minusPrefix(req.URL.Path, zipPrefix))
	case strings.HasPrefix(req.URL.Path, tarPrefix):
		doTarDocs(w, req, minusPrefix(req.URL.Path, tarPrefix))
	case strings.HasPrefix(req.URL.Path, fsckPrefix):
		dofsck(w, req, minusPrefix(req.URL.Path, fsckPrefix))
	case strings.HasPrefix(req.URL.Path, debugPrefix):
		doDebug(w, req)
	case strings.HasPrefix(req.URL.Path, "/.cbfs/"):
		http.Error(w, "Can't GET here", 400)
	default:
		doGetUserDoc(w, req)
	}
}

func minusPrefix(s, prefix string) string {
	return s[len(prefix):]
}

func doDeleteOID(w http.ResponseWriter, req *http.Request) {
	oid := minusPrefix(req.URL.Path, blobPrefix)

	err := removeObject(oid)
	if err == nil {
		w.WriteHeader(204)
	} else {
		http.Error(w, err.Error(), 404)
	}
}

func doDeleteUserDoc(w http.ResponseWriter, req *http.Request) {
	_, k := resolvePath(req)
	err := couchbase.Update(k, 0, func(in []byte) ([]byte, error) {
		existing := fileMeta{}
		err := json.Unmarshal(in, &existing)
		if !shouldStoreMeta(req.Header, err == nil, existing) {
			return in, errUploadPrecondition
		}
		return nil, nil
	})
	if err == nil {
		w.WriteHeader(204)
	} else if err == errUploadPrecondition {
		http.Error(w, "precondition failed", 412)
	} else {
		http.Error(w, err.Error(), 404)
	}
}

func doDelete(w http.ResponseWriter, req *http.Request) {
	switch {
	case strings.HasPrefix(req.URL.Path, blobPrefix):
		doDeleteOID(w, req)
	case *enableCRUDProxy && strings.HasPrefix(req.URL.Path, crudproxyPrefix):
		proxyCRUDDelete(w, req, minusPrefix(req.URL.Path, crudproxyPrefix))
	case strings.HasPrefix(req.URL.Path, "/.cbfs/"):
		http.Error(w, "Can't DELETE here", 400)
	default:
		doDeleteUserDoc(w, req)
	}
}

func doExit(w http.ResponseWriter, req *http.Request) {
	time.AfterFunc(time.Second, func() {
		log.Printf("Quitting per user request from %v",
			req.RemoteAddr)
		os.Exit(0)
	})
	w.WriteHeader(202)
}

func getExpirationFrom(h string) int {
	rv := 0
	if h != "" {
		i, err := strconv.Atoi(h)
		if err == nil {
			rv = i
		}
	}
	return rv
}

func getExpiration(hdr http.Header) int {
	return getExpirationFrom(hdr.Get("X-CBFS-Expiration"))
}

func doLinkFile(w http.ResponseWriter, req *http.Request) {
	fn := req.URL.Path
	h := req.FormValue("blob")
	t := req.FormValue("type")

	for len(fn) > 0 && fn[0] == '/' {
		fn = fn[1:]
	}

	blob, err := referenceBlob(h)
	if err != nil {
		estat := 500
		if gomemcached.IsNotFound(err) {
			estat = 404
		}
		http.Error(w, err.Error(), estat)
	}

	fm := fileMeta{
		Headers: http.Header{
			"Content-Type": []string{t},
		},
		OID:      h,
		Length:   blob.Length,
		Modified: time.Now().UTC(),
	}

	exp := getExpiration(req.Header)
	if exp != 0 {
		fm.Headers.Set("X-CBFS-Expiration", strconv.Itoa(exp))
	}

	err = maybeStoreMeta(fn, fm, exp, true)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(201)
}

func doPost(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == blobPrefix {
		doPostRawBlob(w, req)
	} else if req.URL.Path == blobInfoPath {
		doBlobInfo(w, req)
	} else if strings.HasPrefix(req.URL.Path, markBackupPrefix) {
		doMarkBackup(w, req)
	} else if strings.HasPrefix(req.URL.Path, restorePrefix) {
		doRestoreDocument(w, req, minusPrefix(req.URL.Path, restorePrefix))
	} else if strings.HasPrefix(req.URL.Path, taskPrefix) {
		doInduceTask(w, req, minusPrefix(req.URL.Path, taskPrefix))
	} else if strings.HasPrefix(req.URL.Path, backupPrefix) {
		doBackupDocs(w, req)
	} else if strings.HasPrefix(req.URL.Path, quitPrefix) {
		doExit(w, req)
	} else if strings.HasPrefix(req.URL.Path, "/.cbfs/") {
		http.Error(w, "Can't POST here", 400)
	} else {
		doLinkFile(w, req)
	}
}

func httpHandler(w http.ResponseWriter, req *http.Request) {
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
