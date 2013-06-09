package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-hashset"
	"github.com/dustin/gomemcached"

	"encoding/hex"
	"github.com/couchbaselabs/cbfs/config"
	"sync"
)

const backupKey = "/@backup"

type backupItem struct {
	Fn   string                `json:"filename"`
	Oid  string                `json:"oid"`
	When time.Time             `json:"when"`
	Conf cbfsconfig.CBFSConfig `json:"conf"`
}

type backups struct {
	Latest  backupItem   `json:"latest"`
	Backups []backupItem `json:"backups"`
}

func logDuration(m string, startTime time.Time) {
	log.Printf("Completed %v in %v", m, time.Since(startTime))
}

func streamFileMeta(w io.Writer,
	fch chan *namedFile,
	ech chan error) error {

	enc := json.NewEncoder(w)
	for {
		select {
		case f, ok := <-fch:
			if !ok {
				return nil
			}
			err := enc.Encode(map[string]interface{}{
				"path": f.name,
				"meta": f.meta,
			})
			if err != nil {
				return err
			}
		case e, ok := <-ech:
			if ok {
				return e
			}
			ech = nil
		}
	}
}

func backupTo(w io.Writer) (err error) {
	fch := make(chan *namedFile)
	ech := make(chan error)
	qch := make(chan bool)

	defer close(qch)

	defer logDuration("backup", time.Now())

	go pathGenerator("", fch, ech, qch)

	gz := gzip.NewWriter(w)
	defer func() {
		e := gz.Close()
		if err != nil {
			err = e
		}
	}()

	return streamFileMeta(gz, fch, ech)
}

func recordBackupObject() error {
	b := backups{}
	err := couchbase.Get(backupKey, &b)
	if err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(*root, ".backup.json"))
	if err != nil {
		return err
	}
	defer f.Close()
	e := json.NewEncoder(f)
	return e.Encode(&b)
}

func recordRemoteBackupObjects() {
	rn, err := findRemoteNodes()
	if err != nil {
		log.Printf("Error getting remote nodes for recording backup: %v",
			err)
		return
	}
	for _, n := range rn {
		u := fmt.Sprintf("http://%s%s",
			n.Address(), markBackupPrefix)
		c := n.Client()
		res, err := c.Post(u, "application/octet-stream", nil)
		if err != nil {
			log.Printf("Error posting to %v: %v", u, err)
			continue
		}
		res.Body.Close()
		if res.StatusCode != 204 {
			log.Printf("HTTP Error posting to %v: %v", u, res.Status)
		}
	}
}

func storeBackupObject(fn, h string) error {
	b := backups{}
	err := couchbase.Get(backupKey, &b)
	if err != nil && !gomemcached.IsNotFound(err) {
		log.Printf("Weird: %v", err)
		// return err
	}

	ob := backupItem{fn, h, time.Now().UTC(), *globalConfig}

	// Keep only backups we're pretty sure still exist.
	obn := b.Backups
	b.Backups = nil
	for _, bi := range obn {
		fm := fileMeta{}
		err := couchbase.Get(shortName(bi.Fn), &fm)
		if gomemcached.IsNotFound(err) {
			log.Printf("Dropping previous (deleted) backup: %v",
				bi.Fn)
		} else {
			b.Backups = append(b.Backups, bi)
		}
	}

	b.Latest = ob
	b.Backups = append(b.Backups, ob)

	return couchbase.Set(backupKey, 0, &b)
}

func backupToCBFS(fn string) error {
	f, err := NewHashRecord(*root, "")
	if err != nil {
		return err
	}
	defer f.Close()

	pr, pw := io.Pipe()

	go func() { pw.CloseWithError(backupTo(pw)) }()

	h, length, err := f.Process(pr)
	if err != nil {
		return err
	}

	err = recordBlobOwnership(h, length, true)
	if err != nil {
		return err
	}

	fm := fileMeta{
		OID:      h,
		Length:   length,
		Modified: time.Now().UTC(),
	}

	err = storeMeta(fn, fm, 1, nil)
	if err != nil {
		return err
	}

	err = storeBackupObject(fn, h)
	if err != nil {
		return err
	}

	err = recordBackupObject()
	if err != nil {
		log.Printf("Failed to record backup OID: %v", err)
	}

	go recordRemoteBackupObjects()

	log.Printf("Replicating backup %v.", h)
	go increaseReplicaCount(h, length, globalConfig.MinReplicas-1)

	return nil
}

func doMarkBackup(w http.ResponseWriter, req *http.Request) {
	if req.FormValue("all") == "true" {
		go recordRemoteBackupObjects()
	}
	err := recordBackupObject()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error marking backup; %v", err), 500)
		return
	}
	w.WriteHeader(204)
}

func doBackupDocs(w http.ResponseWriter, req *http.Request) {
	fn := req.FormValue("fn")
	if fn == "" {
		http.Error(w, "Missing fn parameter", 400)
		return
	}

	if bg, _ := strconv.ParseBool(req.FormValue("bg")); bg {
		go func() {
			err := backupToCBFS(fn)
			if err != nil {
				log.Printf("Error performing bg backup: %v", err)
			}
		}()
		w.WriteHeader(202)
		return
	}

	err := backupToCBFS(fn)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error performing backup: %v", err), 500)
		return
	}

	w.WriteHeader(201)
}

func doGetBackupInfo(w http.ResponseWriter, req *http.Request) {
	b := backups{}
	err := couchbase.Get(backupKey, &b)
	if err != nil {
		code := 500
		if gomemcached.IsNotFound(err) {
			code = 404
		}
		http.Error(w, err.Error(), code)
		return
	}

	w.Write(mustEncode(&b))
}

var errExists = errors.New("item exists")

func maybeStoreMeta(k string, fm fileMeta, force bool) error {
	if force {
		return couchbase.Set(k, 0, fm)
	}
	added, err := couchbase.Add(k, 0, fm)
	if err == nil && !added {
		err = errExists
	}
	return err
}

func doRestoreDocument(w http.ResponseWriter, req *http.Request, fn string) {
	d := json.NewDecoder(req.Body)
	fm := fileMeta{}
	err := d.Decode(&fm)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	for len(fn) > 0 && fn[0] == '/' {
		fn = fn[1:]
	}

	if fn == "" {
		http.Error(w, "No filename", 400)
		return
	}

	if strings.Contains(fn, "//") {
		http.Error(w,
			fmt.Sprintf("Too many slashes in the path name: %v", fn), 400)
		return
	}

	force := false
	err = maybeStoreMeta(fn, fm, force)
	switch err {
	case errExists:
		http.Error(w, err.Error(), 409)
		return
	case nil:
	default:
		log.Printf("Error storing file meta of %v -> %v: %v",
			fn, fm.OID, err)
		http.Error(w,
			fmt.Sprintf("Error recording file meta: %v", err), 500)
		return
	}

	log.Printf("Restored %v -> %v", fn, fm.OID)

	w.WriteHeader(201)
}

func loadBackupHashes(oid string) (*hashset.Hashset, int, error) {
	rv := &hashset.Hashset{}

	r := blobReader(oid)
	defer r.Close()
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, 0, err
	}
	defer gz.Close()

	d := json.NewDecoder(gz)

	visited := 0
	for {
		ob := struct {
			Meta struct {
				OID   string
				Older []struct {
					OID string
				}
			}
		}{}

		err := d.Decode(&ob)
		switch err {
		case nil:
			oid, err := hex.DecodeString(ob.Meta.OID)
			if err != nil {
				return nil, visited, err
			}
			rv.Add(oid)
			visited++
			for _, obs := range ob.Meta.Older {
				oid, err = hex.DecodeString(obs.OID)
				if err != nil {
					return nil, visited, err
				}
				rv.Add(oid)
				visited++
			}
		case io.EOF:
			return rv, visited, nil
		default:
			return nil, visited, err
		}
	}
}

func loadExistingHashes() (*hashset.Hashset, error) {
	b := backups{}
	err := couchbase.Get(backupKey, &b)
	if err != nil && !gomemcached.IsNotFound(err) {
		return nil, err
	}

	oids := make(chan string)
	hsch := make(chan *hashset.Hashset)
	visitch := make(chan int)
	errch := make(chan error)

	wg := sync.WaitGroup{}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for o := range oids {
				h, v, e := loadBackupHashes(o)
				if e == nil {
					hsch <- h
				} else {
					errch <- e
				}
				visitch <- v
			}
		}()
	}
	go func() {
		wg.Wait()
		close(hsch)
		close(visitch)
		close(errch)
	}()

	go func() {
		for _, i := range b.Backups {
			log.Printf("Loading backups from %v / %v", i.Oid, i.Fn)
			oids <- i.Oid
		}
		close(oids)
	}()

	visited := 0
	hs := &hashset.Hashset{}
	for {
		// Done getting all the things
		if hsch == nil && visitch == nil && errch == nil {
			break
		}
		select {
		case v, ok := <-visitch:
			visited += v
			if !ok {
				visitch = nil
			}
		case e, ok := <-errch:
			err = e
			if !ok {
				errch = nil
			}
		case h, ok := <-hsch:
			if ok {
				log.Printf("Got %v hashes from a backup",
					h.Len())
				hs.AddAll(h)
			} else {
				hsch = nil
			}
		}
	}

	log.Printf("Visited %v obs, kept %v", visited, hs.Len())

	return hs, err
}
