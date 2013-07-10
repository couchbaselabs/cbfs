package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"time"

	cb "github.com/couchbaselabs/go-couchbase"
	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

type BlobOwnership struct {
	OID        string               `json:"oid"`
	Length     int64                `json:"length"`
	Nodes      map[string]time.Time `json:"nodes"`
	Type       string               `json:"type"`
	Garbage    bool                 `json:"garbage"`
	Referenced time.Time            `json:"referenced"`
}

type internodeCommand uint8

const (
	removeObjectCmd = internodeCommand(iota)
	acquireObjectCmd
	fetchObjectCmd
)

type internodeTask struct {
	node     StorageNode
	cmd      internodeCommand
	oid      string
	prevNode string
}

var taskWorkers = flag.Int("taskWorkers", 4,
	"Number of blob move/removal workers.")

func (b BlobOwnership) ResolveNodes() NodeList {
	keys := make([]string, 0, len(b.Nodes))
	for k := range b.Nodes {
		keys = append(keys, "/"+k)
	}
	resps := couchbase.GetBulk(keys)

	rv := make(NodeList, 0, len(resps))

	for k, v := range resps {
		if v.Status == gomemcached.SUCCESS {
			a := StorageNode{}
			err := json.Unmarshal(v.Body, &a)
			if err == nil {
				a.name = k[1:]
				rv = append(rv, a)
			}
		}
	}

	sort.Sort(rv)

	return rv
}

// Get the most recent storer of a blob
func (b BlobOwnership) mostRecent() (string, time.Time) {
	rvnode := ""
	rvt := time.Time{}

	for node, t := range b.Nodes {
		if t.After(rvt) {
			rvnode = node
			rvt = t
		}
	}

	return rvnode, rvt
}
func (b BlobOwnership) latestReference() time.Time {
	_, t := b.mostRecent()
	if b.Referenced.After(t) {
		t = b.Referenced
	}
	return t
}

func (b BlobOwnership) ResolveRemoteNodes() NodeList {
	return b.ResolveNodes().minusLocal()
}

func getBlobs(oids []string) (map[string]BlobOwnership, error) {
	keys := make([]string, len(oids))
	for _, b := range oids {
		keys = append(keys, "/"+b)
	}

	res := map[string]BlobOwnership{}
	for k, v := range couchbase.GetBulk(keys) {
		if v.Status == gomemcached.SUCCESS {
			bo := BlobOwnership{}
			err := json.Unmarshal(v.Body, &bo)
			if err != nil {
				return res, err
			}
			res[k[1:]] = bo
		}
	}

	return res, nil
}

func getBlobOwnership(oid string) (BlobOwnership, error) {
	rv := BlobOwnership{}
	oidkey := "/" + oid
	err := couchbase.Get(oidkey, &rv)
	return rv, err
}

func blobReader(oid string) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(copyBlob(pw, oid)) }()
	return pr
}

func copyBlob(w io.Writer, oid string) error {
	f, err := openBlob(oid)
	if err == nil {
		// Doing it locally
		defer f.Close()
		_, err = io.Copy(w, f)
		return err
	} else {
		// Doing it remotely
		c := captureResponseWriter{w: w, hdr: http.Header{}}
		return getBlobFromRemote(&c, oid, http.Header{}, *cachePercentage)
	}
}

func recordBlobOwnership(h string, l int64, force bool) error {
	k := "/" + h

	err := couchbase.Update(k, 0, func(in []byte) ([]byte, error) {
		ownership := BlobOwnership{}
		err := json.Unmarshal(in, &ownership)
		if err == nil {
			if _, ok := ownership.Nodes[serverId]; ok && !force {
				// Skip it fast if it already knows us
				return nil, cb.UpdateCancel
			}
			ownership.Nodes[serverId] = time.Now().UTC()
		} else {
			ownership.Nodes = map[string]time.Time{
				serverId: time.Now().UTC(),
			}
		}
		ownership.OID = h
		ownership.Length = l
		ownership.Garbage = false
		ownership.Type = "blob"
		return json.Marshal(ownership)
	})

	if err == cb.UpdateCancel {
		err = nil
	} else {
		log.Printf("Recorded myself as an owner of %v: result=%v",
			h, errorOrSuccess(err))
	}
	return err
}

func referenceBlob(h string) (rv BlobOwnership, err error) {
	k := "/" + h
	err = couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Get(vb, k)
		if err != nil {
			return err
		}
		ownership := BlobOwnership{}
		err = json.Unmarshal(res.Body, &ownership)
		if err != nil {
			return err
		}
		ownership.Referenced = time.Now()
		ownership.Garbage = false
		rv = ownership
		req := &gomemcached.MCRequest{
			Opcode:  gomemcached.SET,
			VBucket: vb,
			Key:     []byte(k),
			Cas:     res.Cas,
			Opaque:  0,
			Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
			Body:    mustEncode(&ownership)}
		_, err = mc.Send(req)
		return err
	})
	return
}

func markGarbage(h string) error {
	k := "/" + h
	return couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		res, err := mc.Get(vb, k)
		if err != nil {
			return err
		}
		ownership := BlobOwnership{}
		err = json.Unmarshal(res.Body, &ownership)
		if err != nil {
			return err
		}
		t := ownership.latestReference()
		if time.Since(t) < time.Minute*15 {
			return errors.New("too soon")
		}
		ownership.Garbage = true
		req := &gomemcached.MCRequest{
			Opcode:  gomemcached.SET,
			VBucket: vb,
			Key:     []byte(k),
			Cas:     res.Cas,
			Opaque:  0,
			Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
			Body:    mustEncode(&ownership)}
		_, err = mc.Send(req)
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
		log.Printf("Error incrementing node identifier: %v", err)
	}
}

// Returns the number of known owners (-1 if it can't be determined)
func removeBlobOwnershipRecord(h, node string) int {
	log.Printf("Cleaning up %v from %v", h, node)
	numOwners := -1

	k := "/" + h

	err := couchbase.Update(k, 0, func(in []byte) ([]byte, error) {
		ownership := BlobOwnership{}
		if len(in) == 0 {
			return nil, cb.UpdateCancel
		}

		err := json.Unmarshal(in, &ownership)
		if err == nil {
			delete(ownership.Nodes, node)
		} else {
			log.Printf("Error unmarhaling blob removal from %s for %v: %v",
				in, h, err)
			return nil, cb.UpdateCancel
		}

		numOwners = len(ownership.Nodes)

		if len(ownership.Nodes) == 0 && node == serverId {
			return nil, nil
		}

		return json.Marshal(ownership)
	})
	log.Printf("Cleaned %v from %v, result=%v", h, node, errorOrSuccess(err))
	if err != nil && err != cb.UpdateCancel {
		numOwners = -1
	}
	if numOwners == 0 {
		log.Printf("Completed removal of %v", h)
		couchbase.Delete(k + "/r")
	}

	return numOwners
}

func errorOrSuccess(e error) string {
	if e == nil {
		return "success"
	}
	return e.Error()
}

func maybeRemoveBlobOwnership(h string) (rv error) {
	k := "/" + h
	removedLast := false

	err := couchbase.Update(k, 0, func(in []byte) ([]byte, error) {
		ownership := BlobOwnership{}
		removedLast = false

		if len(in) == 0 {
			return nil, cb.UpdateCancel
		}

		err := json.Unmarshal(in, &ownership)
		if err == nil {
			if ownership.Garbage {
				// OK
			} else if time.Since(ownership.Nodes[serverId]) < time.Hour {
				rv = errors.New("too soon")
				return nil, cb.UpdateCancel
			} else if len(ownership.Nodes)-1 < globalConfig.MinReplicas {
				rv = errors.New("Insufficient replicas")
				return nil, cb.UpdateCancel
			}
			delete(ownership.Nodes, serverId)
		} else {
			log.Printf("Error unmarhaling blob removal of %v from %s: %v",
				h, in, err)
			rv = err
			return nil, cb.UpdateCancel
		}

		if len(ownership.Nodes) == 0 {
			removedLast = true
			return nil, nil
		}

		return json.Marshal(ownership)
	})

	log.Printf("Asked to remove %v - cas=%v, result=%v", h,
		errorOrSuccess(err), errorOrSuccess(rv))
	if removedLast {
		log.Printf("Completed removal of %v", h)
		couchbase.Delete(k + "/r")
	}

	return
}

func increaseReplicaCount(oid string, length int64, by int) error {
	nl, err := findAllNodes()
	if err != nil {
		return err
	}
	onto := nl.candidatesFor(oid, NodeList{})
	if len(onto) > by {
		onto = onto[:by]
	}
	for _, n := range onto {
		log.Printf("Asking %v to acquire %v", n, oid)
		queueBlobAcquire(n, oid, "")
	}
	return nil
}

func ensureMinimumReplicaCount() error {
	nl, err := findAllNodes()
	if err != nil {
		return err
	}

	viewRes := struct {
		Rows []struct {
			Key int
			Id  string
		}
	}{}

	// Don't bother trying to replicate to more nodes than exist.
	endKey := globalConfig.MinReplicas - 1
	if globalConfig.MinReplicas > len(nl) {
		endKey = len(nl) - 1
	}

	if endKey < 1 {
		return errors.New("Not enough nodes to increase repl count.")
	}

	// Find some less replicated docs to suck in.
	err = couchbase.ViewCustom("cbfs", "repcounts",
		map[string]interface{}{
			"reduce":   false,
			"limit":    globalConfig.ReplicationCheckLimit,
			"startkey": 1,
			"endkey":   endKey,
			"stale":    false,
		},
		&viewRes)

	if err != nil {
		return err
	}

	if len(viewRes.Rows) > 0 {
		log.Printf("Increasing replica count of up to %v items",
			len(viewRes.Rows))
	} else {
		return nil
	}

	did := 0
	for _, r := range viewRes.Rows {
		todo := globalConfig.MinReplicas - r.Key
		if !salvageBlob(r.Id[1:], "", todo, nl) {
			log.Printf("Queue is full ensuring min repl count")
			break
		}
		did++
	}
	log.Printf("Increased the replica count of %v items", did)
	return nil
}

func pruneBlob(oid string, nodemap map[string]string, nl NodeList) {
	if len(nodemap) <= globalConfig.MaxReplicas {
		log.Printf("Asked to prune a blob that has too few replicas: %v",
			oid)
	}

	log.Printf("Pruning blob %v down from %v repls to %v",
		oid, len(nodemap), globalConfig.MaxReplicas)

	nm := map[string]StorageNode{}
	for _, n := range nl {
		nm[n.name] = n
	}

	remaining := len(nodemap)
	for n := range nodemap {
		if remaining <= globalConfig.MaxReplicas {
			break
		}
		remaining--
		if sn, ok := nm[n]; ok {
			queueBlobRemoval(sn, oid)
		}
	}

}

func pruneExcessiveReplicas() error {
	nl, err := findAllNodes()
	if err != nil {
		return err
	}

	viewRes := struct {
		Rows []struct {
			Id  string
			Doc struct {
				Json struct {
					Nodes map[string]string
				}
			}
		}
	}{}

	// Find some less replicated docs to suck in.
	err = couchbase.ViewCustom("cbfs", "repcounts",
		map[string]interface{}{
			"descending":   true,
			"reduce":       false,
			"include_docs": true,
			"limit":        globalConfig.ReplicationCheckLimit,
			"endkey":       globalConfig.MaxReplicas + 1,
			"stale":        false,
		},
		&viewRes)

	if err != nil {
		return err
	}

	// Short-circuit when there's nothing to clean
	if len(viewRes.Rows) == 0 {
		return nil
	} else {
		log.Printf("Decreasing replica count of %v items",
			len(viewRes.Rows))
	}

	for _, r := range viewRes.Rows {
		pruneBlob(r.Id[1:], r.Doc.Json.Nodes, nl)
	}
	return nil
}

func hasBlob(oid string) bool {
	_, err := os.Stat(hashFilename(*root, oid))
	return err == nil
}

var fetchLocks namedLock

func performFetch(oid, prev string) {
	c := captureResponseWriter{w: ioutil.Discard, hdr: http.Header{}}

	// If we already have it, we don't need it more.
	st, err := os.Stat(hashFilename(*root, oid))
	if err == nil {
		err = recordBlobOwnership(oid, st.Size(), false)
		if err != nil {
			log.Printf("Error recording fetched blob %v: %v",
				oid, err)
		}
		return
	}

	if fetchLocks.Lock(oid) {
		defer fetchLocks.Unlock(oid)
		err = getBlobFromRemote(&c, oid, http.Header{}, 100)
	} else {
		log.Printf("Not fetching remote, already in progress.")
		return
	}

	if err == nil && c.statusCode == 200 {
		if prev != "" {
			log.Printf("Removing ownership of %v from %v after takeover",
				oid, prev)
			n, err := findNode(prev)
			if err != nil {
				log.Printf("Error finding old node of %v: %v", oid, err)
				removeBlobOwnershipRecord(oid, prev)
			} else {
				log.Printf("Requesting post-move blob removal of %v from %v",
					oid, n)
				go queueBlobRemoval(n, oid)
			}
		}
	} else {
		log.Printf("Error grabbing remote object %v, got %v/%v",
			oid, c.statusCode, err)
	}
}

// Return false on unrecoverable errors (i.e. the internode queue is
// full and we need a break)
func salvageBlob(oid, deadNode string, todo int, nl NodeList) bool {
	candidates := nl.candidatesFor(oid,
		NodeList{nl.named(deadNode)})

	if len(candidates) == 0 {
		log.Printf("Couldn't find a candidate for %v!", oid)
	} else {
		rv := true
		for _, n := range candidates {
			worked := maybeQueueBlobAcquire(n, oid, deadNode)
			log.Printf("Recommending %v get a copy of %v - queued=%v",
				n, oid, worked)
			rv = rv && worked
			todo--
			if todo == 0 {
				break
			}
		}
		return rv
	}
	return true
}

var internodeTaskQueue chan internodeTask

func internodeTaskWorker() {
	for c := range internodeTaskQueue {
		switch c.cmd {
		case removeObjectCmd:
			if err := c.node.deleteBlob(c.oid); err != nil {
				log.Printf("Error deleting %v from %v: %v",
					c.oid, c.node, err)
				if c.node.IsDead() {
					log.Printf("Node is dead, cleaning %v",
						c.oid)
					removeBlobOwnershipRecord(c.oid,
						c.node.name)
				}
			}
		case acquireObjectCmd:
			if err := c.node.acquireBlob(c.oid, c.prevNode); err != nil {
				log.Printf("Error requesting acquisition of %v from %v: %v",
					c.oid, c.node, err)
			}
		case fetchObjectCmd:
			performFetch(c.oid, c.prevNode)
		default:
			log.Fatalf("Unhandled worker task: %v", c)
		}
	}
}

func initTaskQueueWorkers() {
	for i := 0; i < *taskWorkers; i++ {
		go internodeTaskWorker()
	}
}

func queueBlobRemoval(n StorageNode, oid string) {
	internodeTaskQueue <- internodeTask{
		node: n,
		cmd:  removeObjectCmd,
		oid:  oid,
	}
}

// Ask a remote node to go get a blob
func queueBlobAcquire(n StorageNode, oid string, prev string) {
	internodeTaskQueue <- internodeTask{
		node:     n,
		cmd:      acquireObjectCmd,
		oid:      oid,
		prevNode: prev,
	}
}

// Ask a remote node to go get a blob, return false if the queue is full
func maybeQueueBlobAcquire(n StorageNode, oid string, prev string) bool {
	select {
	case internodeTaskQueue <- internodeTask{
		node:     n,
		cmd:      acquireObjectCmd,
		oid:      oid,
		prevNode: prev,
	}:
		return true
	default:
		return false
	}
}

// Ask this node to go get a blob.
//
// Returns false if queue is full and the request could not be queued.
func maybeQueueBlobFetch(oid, prev string) bool {
	select {
	case internodeTaskQueue <- internodeTask{
		cmd:      fetchObjectCmd,
		oid:      oid,
		prevNode: prev,
	}:
		return true
	default:
		return false
	}
}
