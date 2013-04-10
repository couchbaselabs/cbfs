package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dustin/gomemcached"
)

const backupKey = "/@backup"

type backupItem struct {
	Fn   string    `json:"filename"`
	Oid  string    `json:"oid"`
	When time.Time `json:"when"`
}

type backups struct {
	Latest  backupItem   `json:"latest"`
	Backups []backupItem `json:"backups"`
}

func logDuration(m string, startTime time.Time) {
	log.Printf("Completed %v in %v", m, time.Since(startTime))
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

	enc := json.NewEncoder(gz)

	for {
		select {
		case f, ok := <-fch:
			if !ok {
				return nil
			}
			log.Printf("backing up %v", f.name)
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

	panic("unreachable")
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

	ob := backupItem{fn, h, time.Now().UTC()}

	b.Latest = ob
	b.Backups = append(b.Backups, ob)

	// TODO:  Verify existing backups

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

	err = storeMeta(fn, fm, 1)
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
	err := recordBackupObject()
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error marking backup: %v", err)
	}
	w.WriteHeader(204)
}

func doBackupDocs(w http.ResponseWriter, req *http.Request) {
	fn := req.FormValue("fn")
	if fn == "" {
		w.WriteHeader(400)
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
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error performing backup: %v", err)
		return
	}

	w.WriteHeader(201)
}
