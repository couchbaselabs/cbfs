package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"

	cb "github.com/couchbaselabs/go-couchbase"
	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"

	"github.com/couchbaselabs/cbfs/config"
)

var verifyWorkers = flag.Int("verifyWorkers", 4,
	"Number of object verification workers.")
var maxStartupObjects = flag.Int("maxStartObjs", 1000,
	"Maximum number of objects to pull on start")
var maxStartupRepls = flag.Int("maxStartRepls", 3,
	"Blob replication limit for startup objects.")

type PeriodicJob struct {
	period func() time.Duration
	f      func() error
}

var periodicJobs = map[string]*PeriodicJob{}

func init() {
	periodicJobs = map[string]*PeriodicJob{
		"checkStaleNodes": &PeriodicJob{
			func() time.Duration {
				return globalConfig.StaleNodeCheckFreq
			},
			checkStaleNodes,
		},
		"garbageCollectBlobs": &PeriodicJob{
			func() time.Duration {
				return globalConfig.GCFreq
			},
			garbageCollectBlobs,
		},
		"ensureMinReplCount": &PeriodicJob{
			func() time.Duration {
				return globalConfig.UnderReplicaCheckFreq
			},
			ensureMinimumReplicaCount,
		},
		"pruneExcessiveReplicas": &PeriodicJob{
			func() time.Duration {
				return globalConfig.OverReplicaCheckFreq
			},
			pruneExcessiveReplicas,
		},
		"updateNodeSizes": &PeriodicJob{
			func() time.Duration {
				return 15 * time.Second
			},
			updateNodeSizes,
		},
	}
}

type JobMarker struct {
	Node    string    `json:"node"`
	Started time.Time `json:"started"`
	Type    string    `json:"type"`
}

// Run a named task if we know one hasn't in the last t seconds.
func runNamedGlobalTask(name string, t time.Duration, f func() error) bool {
	key := "/@" + name

	if t.Seconds() < 1 {
		log.Printf("WARN: would've run with a 0s ttl, skipping %v",
			name)
		time.Sleep(time.Second)
		return false
	}

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

func reconcileLoop() {
	if globalConfig.ReconcileFreq == 0 {
		log.Printf("Reconciliation is misconfigured")
		return
	}
	time.Sleep(time.Second * time.Duration(5+rand.Intn(60)))
	for {
		err := reconcile()
		if err != nil {
			log.Printf("Error in reconciliation loop: %v", err)
		}
		grabSomeData()
		time.Sleep(globalConfig.ReconcileFreq)
	}
}

func validateLocal() error {
	log.Printf("Validating Local Blobs")

	viewRes := struct {
		Rows []struct {
			Id string
		}
	}{}

	count := 0
	startDocId := ""
	done := false
	start := time.Now()
	for !done {
		log.Printf("  local reconcile loop at %v", startDocId)
		params := map[string]interface{}{
			"key":    serverId,
			"reduce": false,
			"limit":  globalConfig.GCLimit + 1,
		}
		if startDocId != "" {
			params["startkey_docid"] = cb.DocId(startDocId)
		}
		err := couchbase.ViewCustom("cbfs", "node_blobs", params,
			&viewRes)
		if err != nil {
			return err
		}
		done = len(viewRes.Rows) < globalConfig.GCLimit

		for _, r := range viewRes.Rows {
			startDocId = r.Id

			hash := startDocId[1:]
			if !hasBlob(hash) {
				log.Printf("Mistakenly registered with %v",
					hash)
				removeBlobOwnershipRecord(hash, serverId)
			}
			count++
		}
	}

	log.Printf("Validated %v files in %v", count, time.Since(start))
	return nil
}

func validateLocalLoop() {
	if globalConfig.LocalValidationFreq == 0 {
		log.Printf("Local validation is misconfigured")
		return
	}
	time.Sleep(time.Second * time.Duration(5+rand.Intn(60)))
	for {
		err := validateLocal()
		if err != nil {
			log.Printf("Error validating local store: %v", err)
		}
		time.Sleep(globalConfig.LocalValidationFreq)
	}
}

func cleanupNode(node string) {
	if globalConfig.NodeCleanCount < 1 {
		log.Printf("Misconfigured cleaner (on %v): %v",
			node, globalConfig)
		return
	}

	nodes, err := findAllNodes()
	if err != nil {
		log.Printf("Error finding node list, aborting clean: %v", err)
		return
	}

	log.Printf("Cleaning up node %v with count %v",
		node, globalConfig.NodeCleanCount)
	vres, err := couchbase.View("cbfs", "node_blobs",
		map[string]interface{}{
			"key":    node,
			"limit":  globalConfig.NodeCleanCount,
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

		if numOwners < globalConfig.MinReplicas {
			salvageBlob(r.ID[1:], node, nodes)
		}
	}
	log.Printf("Removed %v blobs from %v", foundRows, node)
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
		err = removeFromNodeRegistry(node)
		if err != nil {
			log.Printf("Error deleting %v from registry: %v", node, err)
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

		if d > globalConfig.StaleNodeLimit {
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

func taskRunning(taskName string) bool {
	into := map[string]interface{}{}
	err := couchbase.Get("/@"+taskName+"/running", &into)
	return err == nil
}

func relockTask(taskName string) bool {
	k := "/@" + taskName
	err := couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		resp, err := mc.Get(vb, k)
		switch {
		case err != nil:
			return err
		case resp.Status != gomemcached.SUCCESS:
			return resp
		}

		jm := JobMarker{}
		err = json.Unmarshal(resp.Body, &jm)
		if err != nil {
			return err
		}
		if jm.Node != serverId {
			return errors.New("Lost lock")
		}
		jm.Started = time.Now().UTC()
		req := &gomemcached.MCRequest{
			Opcode:  gomemcached.SET,
			VBucket: vb,
			Key:     []byte(k),
			Cas:     resp.Cas,
			Opaque:  0,
			Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
			Body:    mustEncode(&jm),
		}
		exp := periodicJobs[taskName].period().Seconds()
		binary.BigEndian.PutUint64(req.Extras, uint64(exp))

		resp, err = mc.Send(req)
		switch {
		case err != nil:
			return err
		case resp.Status != gomemcached.SUCCESS:
			return resp
		}
		return nil
	})

	return err == nil
}

func runMarkedTask(name, excl string, f func() error) error {
	for taskRunning(excl) {
		time.Sleep(5 * time.Second)
	}

	if !relockTask(name) {
		log.Printf("We lost the lock for %v", name)
		return nil
	}

	taskKey := "/@" + name + "/running"
	err := couchbase.Set(taskKey, 3600,
		map[string]interface{}{
			"node": serverId,
			"time": time.Now().UTC(),
		})
	if err != nil {
		return err
	}
	defer couchbase.Delete(taskKey)
	return f()
}

func garbageCollectBlobs() error {
	return runMarkedTask("garbageCollectBlobs", "ensureMinReplCount",
		garbageCollectBlobsTask)
}

func garbageCollectBlobsTask() error {
	log.Printf("Garbage collecting blobs without any file references")

	viewRes := struct {
		Rows []struct {
			Key []string
		}
		Errors []struct {
			From   string
			Reason string
		}
	}{}

	nm, err := findNodeMap()
	if err != nil {
		return err
	}

	count := 0
	startKey := "g"
	done := false
	for !done {
		log.Printf("  gc loop at %#v", startKey)
		// we hit this view descending because we want file sorted
		// before blob the fact that we walk the list backwards
		// hopefully not too awkward
		err := couchbase.ViewCustom("cbfs", "file_blobs",
			map[string]interface{}{
				"stale":      false,
				"descending": true,
				"limit":      globalConfig.GCLimit + 1,
				"startkey":   []string{startKey},
			}, &viewRes)
		if err != nil {
			return err
		}
		done = len(viewRes.Rows) < globalConfig.GCLimit

		if len(viewRes.Errors) > 0 {
			return fmt.Errorf("View errors: %v", viewRes.Errors)
		}

		lastBlob := ""
		for _, r := range viewRes.Rows {
			blobId := r.Key[0]
			typeFlag := r.Key[1]
			blobNode := r.Key[2]
			startKey = blobId

			switch typeFlag {
			case "file":
				lastBlob = blobId
			case "blob":
				if blobId != lastBlob {
					n, ok := nm[blobNode]
					switch {
					case blobNode == "":
						removeBlobOwnershipRecord(blobId, serverId)
						count++
					case ok:
						queueBlobRemoval(n, blobId)
						count++
					default:
						log.Printf("No nodemap entry for %v",
							blobNode)
					}
				}
			}
		}

		if !relockTask("garbageCollectBlobs") {
			log.Printf("We lost the lock for garbage collecting.")
			return errors.New("Lost lock")
		}
	}

	log.Printf("Scheduled %d blobs for deletion", count)
	return nil
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

	if resp.StatusCode != 200 {
		io.Copy(ioutil.Discard, resp.Body)
		return fmt.Errorf("Unexpected status fetching %v from %v: %v",
			h, u, resp.Status)
	}

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
			"stale":        false,
		},
		&viewRes)

	if err != nil {
		log.Printf("Error finding docs to suck: %v", err)
		return
	}

	log.Printf("Going to fetch %v startup objects", len(viewRes.Rows))

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
		if runNamedGlobalTask(name, job.period(), job.f) {
			log.Printf("Ran job %v", name)
		}
		time.Sleep(job.period() + time.Second)
	}
}

func runPeriodicJobs() {
	for n, j := range periodicJobs {
		go runPeriodicJob(n, j)
	}
}

func updateConfig() error {
	conf := cbfsconfig.CBFSConfig{}
	err := conf.RetrieveConfig(couchbase)
	if err != nil {
		return err
	}
	globalConfig = &conf
	return nil
}

func reloadConfig() {
	for {
		time.Sleep(time.Minute)
		if err := updateConfig(); err != nil {
			log.Printf("Error updating config: %v", err)
		}
	}
}
