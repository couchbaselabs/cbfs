package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

var heartFreq = flag.Duration("heartbeat", 10*time.Second,
	"Heartbeat frequency")
var reconcileFreq = flag.Duration("reconcile", 24*time.Hour,
	"Reconciliation frequency")
var staleNodeFreq = flag.Duration("staleNodeCheck", 5*time.Minute,
	"How frequently to check for stale nodes.")
var staleNodeLimit = flag.Duration("staleNodeLimit", 15*time.Minute,
	"How long until we clean up nodes for being too stale")
var nodeCleanCount = flag.Int("nodeCleanCount", 1000,
	"How many blobs to clean up from a dead node per period")
var verifyWorkers = flag.Int("verifyWorkers", 4,
	"Number of object verification workers.")
var garbageCollectFreq = flag.Duration("gcFreq", 5*time.Minute,
	"How frequently to check dangling blobs.")
var maxStartupObjects = flag.Int("maxStartObjs", 1000,
	"Maximum number of objects to pull on start")
var maxStartupRepls = flag.Int("maxStartRepls", 3,
	"Blob replication limit for startup objects.")
var minReplicas = flag.Int("minReplicas", 3,
	"Minimum number of replicas to try to keep")

type PeriodicJob struct {
	period time.Duration
	f      func() error
}

var periodicJobs = map[string]*PeriodicJob{
	"checkStaleNodes": &PeriodicJob{
		time.Minute * 5,
		checkStaleNodes,
	},
	"garbageCollectBlobs": &PeriodicJob{
		time.Minute * 5,
		garbageCollectBlobs,
	},
}

func adjustPeriodicJobs() error {
	periodicJobs["checkStaleNodes"].period = *staleNodeFreq
	periodicJobs["garbageCollectBlobs"].period = *garbageCollectFreq
	return nil
}

type JobMarker struct {
	Node    string    `json:"node"`
	Started time.Time `json:"started"`
	Type    string    `json:"type"`
}

// Run a named task if we know one hasn't in the last t seconds.
func runNamedGlobalTask(name string, t time.Duration, f func() error) bool {
	key := "/@" + name

	jm := JobMarker{
		Node:    serverId,
		Started: time.Now(),
		Type:    "job",
	}

	err := couchbase.Do(key, func(mc *memcached.Client, vb uint16) error {
		resp, err := mc.Add(vb, key, 0, int(t.Seconds()),
			mustEncode(&jm))
		if err != nil {
			return err
		}
		if resp.Status != gomemcached.SUCCESS {
			return fmt.Errorf("Wanted success, got %v", resp.Status)
		}
		return nil
	})

	if err == nil {
		err = f()
		if err != nil {
			log.Printf("Error running periodic task %#v: %v", name, err)
		}
		return true
	}

	return false
}

func heartbeat() {
	for {
		u, err := url.Parse(*couchbaseServer)
		c, err := net.Dial("tcp", u.Host)
		localAddr := ""
		if err == nil {
			localAddr = strings.Split(c.LocalAddr().String(), ":")[0]
			c.Close()
		}

		aboutMe := StorageNode{
			Addr:     localAddr,
			Type:     "node",
			Time:     time.Now().UTC(),
			BindAddr: *bindAddr,
			Hash:     *hashType,
		}

		err = couchbase.Set("/"+serverId, aboutMe)
		if err != nil {
			log.Printf("Failed to record a heartbeat: %v", err)
		}
		time.Sleep(*heartFreq)
	}
}

func verifyObjectHash(h string) error {
	fn := hashFilename(*root, h)
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer f.Close()

	sh := getHash()
	_, err = io.Copy(sh, f)
	if err != nil {
		return err
	}

	hstring := hex.EncodeToString(sh.Sum([]byte{}))
	if h != hstring {
		err = os.Remove(fn)
		if err != nil {
			log.Printf("Error removing corrupt file %v: %v", err)
		}
		return fmt.Errorf("Hash from disk of %v was %v", h, hstring)
	}
	return nil
}

func verifyWorker(ch chan os.FileInfo) {
	for info := range ch {
		err := verifyObjectHash(info.Name())
		if err == nil {
			recordBlobOwnership(info.Name(), info.Size())
		} else {
			log.Printf("Invalid hash for object %v found at verification: %v",
				info.Name(), err)
			removeBlobOwnershipRecord(info.Name(), serverId)
		}
	}
}

func reconcile() error {
	explen := getHash().Size() * 2

	vch := make(chan os.FileInfo)
	defer close(vch)

	for i := 0; i < *verifyWorkers; i++ {
		go verifyWorker(vch)
	}

	return filepath.Walk(*root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && !strings.HasPrefix(info.Name(), "tmp") &&
			len(info.Name()) == explen {

			vch <- info

			return err
		}
		return nil
	})
}

func reconcileLoop() {
	if *reconcileFreq == 0 {
		return
	}
	for {
		err := reconcile()
		if err != nil {
			log.Printf("Error in reconciliation loop: %v", err)
		}
		grabSomeData()
		time.Sleep(*reconcileFreq)
	}
}

// Returns the number of known owners (-1 if it can't be determined)
func removeBlobOwnershipRecord(h, node string) int {
	log.Printf("Cleaning up %v from %v", h, node)
	numOwners := -1

	k := "/" + h
	err := couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		_, err := mc.CAS(vb, k, func(in []byte) ([]byte, memcached.CasOp) {
			ownership := BlobOwnership{}
			err := json.Unmarshal(in, &ownership)
			if err == nil {
				if _, ok := ownership.Nodes[node]; !ok {
					// Skip it fast if we don't have it.
					return nil, memcached.CASQuit
				}
				delete(ownership.Nodes, node)
			} else {
				log.Printf("Error unmarhaling blob removal from %s: %v",
					in, err)
				return nil, memcached.CASQuit
			}

			var rv []byte
			op := memcached.CASStore

			numOwners = len(ownership.Nodes)

			if len(ownership.Nodes) == 0 {
				op = memcached.CASDelete
			} else {
				rv = mustEncode(&ownership)
			}

			return rv, op
		}, 0)
		return err
	})
	if err != nil && err != memcached.CASQuit {
		log.Printf("Error cleaning %v from %v: %v", node, h, err)
		numOwners = -1
	}

	return numOwners
}

func salvageBlob(oid, deadNode string, nl NodeList) {
	// Find the owners of this blob
	ownership := BlobOwnership{}
	oidkey := "/" + oid
	err := couchbase.Get(oidkey, &ownership)
	if err != nil {
		log.Printf("Missing ownership record for OID: %v", oid)
		return
	}

	owners := ownership.ResolveNodes()

	var srcCandidate, destCandidate StorageNode
	// Find a good source candidate.  Prefer myself.
	for _, node := range owners {
		if node.IsLocal() {
			srcCandidate = node
		}

		if srcCandidate.name == "" && node.name != deadNode {
			srcCandidate = node
		}
	}

	// Find a good destination candidate. Can't be a source
	// candidate.  Prefer local again.
	for _, node := range nl.minus(owners) {
		if node.IsLocal() && !srcCandidate.IsLocal() {
			destCandidate = node
		}

		if destCandidate.name == "" &&
			node.name != deadNode &&
			node.name != srcCandidate.name {

			destCandidate = node
		}
	}

	if srcCandidate.name == "" || destCandidate.name == "" {
		log.Printf("Couldn't find candidates for blob!")
	} else {
		err = srcCandidate.copyBlob(oid, destCandidate)
		if err != nil {
			log.Printf("Failed to copy: %v", err)
		}
	}
}

func cleanupNode(node string) {
	nodes, err := findAllNodes()
	if err != nil {
		log.Printf("Error finding node list, aborting clean: %v", err)
		return
	}

	log.Printf("Cleaning up node %v", node)
	vres, err := couchbase.View("cbfs", "node_blobs",
		map[string]interface{}{
			"key":    `"` + node + `"`,
			"limit":  *nodeCleanCount,
			"reduce": false,
			"stale":  false,
		})
	if err != nil {
		log.Printf("Error executing node_blobs view: %v", err)
		return
	}
	foundRows := 0
	for _, r := range vres.Rows {
		numOwners := removeBlobOwnershipRecord(r.ID[1:], node)
		foundRows++

		if numOwners < *minReplicas {
			salvageBlob(r.ID[1:], node, nodes)
		}
	}
	if foundRows == 0 && len(vres.Errors) == 0 {
		log.Printf("Removing node record: %v", node)
		err = couchbase.Delete("/" + node)
		if err != nil {
			log.Printf("Error deleting %v node record: %v", node, err)
		}
		err = couchbase.Delete("/" + node + "/r")
		if err != nil {
			log.Printf("Error deleting %v node counter: %v", node, err)
		}
	}
}

func checkStaleNodes() error {
	log.Printf("Checking stale nodes")
	nl, err := findAllNodes()
	if err != nil {
		return err
	}

	for _, node := range nl {
		d := time.Since(node.Time)

		if d > *staleNodeLimit {
			if node.IsLocal() {
				log.Printf("Would've cleaned up myself after %v",
					d)
				continue
			}
			log.Printf("  Node %v missed heartbeat schedule: %v",
				node.name, d)
			go cleanupNode(node.name)
		} else {
			log.Printf("%v is ok at %v", node.name, d)
		}
	}
	return nil
}

func garbageCollectBlobs() error {
	log.Printf("Garbage collecting blobs without any file references")

	viewRes := struct {
		Rows []struct {
			Key []string
		}
	}{}

	// we hit this view descending because we want file sorted before blob
	// the fact that we walk the list backwards hopefully not too awkward
	err := couchbase.ViewCustom("cbfs", "file_blobs",
		map[string]interface{}{
			"stale": false, "descending": true}, &viewRes)

	if err != nil {
		return err
	}

	lastBlob := ""
	count := 0
	for _, r := range viewRes.Rows {
		blobId := r.Key[0]
		typeFlag := r.Key[1]
		blobNode := r.Key[2]

		switch typeFlag {
		case "file":
			lastBlob = blobId
		case "blob":
			if blobId != lastBlob {
				go garbageCollectBlobFromNode(blobId, blobNode)
				count++
			}
		}

	}
	log.Printf("Scheduled %d blobs for deletion", count)
	return nil
}

func garbageCollectBlobFromNode(oid, sid string) {
	if sid == serverId {
		//local delete
		err := os.Remove(hashFilename(*root, oid))
		if err != nil {
			log.Printf("Error removing blob, already deleted? %v", err)
		}
	} else {
		//remote
		remote := StorageNode{}
		remotekey := "/" + sid
		err := couchbase.Get(remotekey, &remote)
		if err != nil {
			// will state node cleanup these records or should i?
			log.Printf("No record of this node")
			return
		}

		req, err := http.NewRequest("DELETE", remote.BlobURL(oid), nil)
		resp, err := http.DefaultClient.Do(req)

		if err != nil {
			log.Printf("Error deleting oid %s from node %s, %v",
				oid, sid, err)
			return
		}

		// non-obvious to me at first, but also with 404 we
		// should also remove the blob ownership
		if resp.StatusCode != 204 && resp.StatusCode != 404 {
			log.Printf("Unexpected status %v deleting %s from node %s",
				resp.Status, oid, sid)
			return
		}
	}
	removeBlobOwnershipRecord(oid, sid)
	log.Printf("Removed blob: %v from node %v", oid, sid)
}

type fetchSpec struct {
	oid  string
	node string
}

func dataInitFetchOne(h, u string) error {
	f, err := NewHashRecord(*root, h)
	if err != nil {
		return err
	}
	defer f.Close()

	resp, err := http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	h, l, err := f.Process(resp.Body)
	if err != nil {
		return err
	}
	return recordBlobOwnership(h, l)
}

func dataInitFetcher(nm map[string]StorageNode, ch <-chan fetchSpec) {
	for fs := range ch {
		node, found := nm[fs.node]
		if !found {
			log.Printf("couldn't find %v", fs.node)
			continue
		}
		log.Printf("Fetching %v from %v", fs.oid, node.BlobURL(fs.oid))
		err := dataInitFetchOne(fs.oid, node.BlobURL(fs.oid))
		if err != nil {
			log.Printf("Error fetching %v: %v", fs.oid, err)
		}
	}
}

func grabSomeData() {
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
	err := couchbase.ViewCustom("cbfs", "repcounts",
		map[string]interface{}{
			"reduce":       false,
			"include_docs": true,
			"limit":        *maxStartupObjects,
			"startkey":     1,
			"endkey":       *maxStartupRepls - 1,
		},
		&viewRes)

	if err != nil {
		log.Printf("Error finding docs to suck: %v", err)
		return
	}

	nl, err := findRemoteNodes()
	if err != nil {
		log.Printf("Error finding nodes: %v", err)
		return
	}
	nm := map[string]StorageNode{}

	for _, n := range nl {
		nm[n.name] = n
	}

	ch := make(chan fetchSpec, 1000)
	defer close(ch)

	for i := 0; i < 4; i++ {
		go dataInitFetcher(nm, ch)
	}

	for _, r := range viewRes.Rows {
		if _, ok := r.Doc.Json.Nodes[serverId]; !ok {
			for n := range r.Doc.Json.Nodes {
				if n != serverId {
					ch <- fetchSpec{r.Id[1:], n}
				}
			}
		}
	}
}

func runPeriodicJob(name string, job *PeriodicJob) {
	time.Sleep(time.Second * time.Duration(5+rand.Intn(60)))
	for {
		if runNamedGlobalTask(name, job.period, job.f) {
			log.Printf("Attempted job %v", name)
		} else {
			log.Printf("Didn't run job %v", name)
		}
		time.Sleep(job.period + time.Second)
	}
}

func runPeriodicJobs() {
	for n, j := range periodicJobs {
		go runPeriodicJob(n, j)
	}
}
