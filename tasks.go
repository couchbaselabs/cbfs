package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	cb "github.com/couchbaselabs/go-couchbase"
	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
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

var globalPeriodicJobs = map[string]*PeriodicJob{}
var localPeriodicJobs = map[string]*PeriodicJob{}

func init() {
	globalPeriodicJobs = map[string]*PeriodicJob{
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
				return globalConfig.UpdateNodeSizesFreq
			},
			updateNodeSizes,
		},
		"trimFullNodes": &PeriodicJob{
			func() time.Duration {
				return globalConfig.TrimFullNodesFreq
			},
			trimFullNodes,
		},
	}

	localPeriodicJobs = map[string]*PeriodicJob{
		"validateLocal": &PeriodicJob{
			func() time.Duration {
				return globalConfig.LocalValidationFreq
			},
			validateLocal,
		},
		"reconcile": &PeriodicJob{
			func() time.Duration {
				return globalConfig.ReconcileFreq
			},
			reconcile,
		},
		"cleanTmp": &PeriodicJob{
			func() time.Duration {
				return time.Hour
			},
			cleanTmpFiles,
		},
	}
}

type JobMarker struct {
	Node    string    `json:"node"`
	Started time.Time `json:"started"`
	Type    string    `json:"type"`
}

type TaskList struct {
	Tasks map[string]time.Time `json:"tasks"`
	Node  string               `json:"node"`
	Type  string               `json:"type"`
}

func clearTasks() error {
	return couchbase.Delete("/@" + serverId + "/tasks")
}

func updateTasks(add, remove string) error {
	k := "/@" + serverId + "/tasks"
	ts := time.Now().UTC()
	err := couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		_, err := mc.CAS(vb, k, func(in []byte) ([]byte, memcached.CasOp) {
			ob := TaskList{Tasks: map[string]time.Time{}}
			json.Unmarshal(in, &ob)
			if add != "" {
				ob.Tasks[add] = ts
			}
			delete(ob.Tasks, remove)
			if len(ob.Tasks) == 0 {
				return nil, memcached.CASDelete
			}
			ob.Type = "tasks"
			ob.Node = serverId
			return mustEncode(&ob), memcached.CASStore
		}, 0)
		return err
	})
	if err == memcached.CASQuit {
		err = nil
	}
	return err
}

func startedTask(name string) error {
	return updateTasks(name, "")
}

func endedTask(name string) error {
	return updateTasks("", name)
}

func listRunningTasks() (map[string]TaskList, error) {
	nodes, err := findAllNodes()
	if err != nil {
		return nil, err
	}

	rv := map[string]TaskList{}

	keys := []string{}

	for _, n := range nodes {
		keys = append(keys, "/@"+n.name+"/tasks")
	}

	responses := couchbase.GetBulk(keys)

	for k, res := range responses {
		if res.Status == gomemcached.SUCCESS {
			ob := TaskList{}
			err = json.Unmarshal(res.Body, &ob)
			if err != nil {
				return nil, err
			}
			rv[k] = ob
		}
	}

	return rv, err
}

// Run a named task if we know one hasn't in the last t seconds.
func runNamedGlobalTask(name string, t time.Duration, f func() error) error {
	key := "/@" + name

	if t.Seconds() < 1 {
		time.Sleep(time.Second)
		return fmt.Errorf("Would've run with a 0s ttl")
	}

	jm := JobMarker{
		Node:    serverId,
		Started: time.Now(),
		Type:    "job",
	}

	alreadyRunning := errors.New("running")

	err := couchbase.Do(key, func(mc *memcached.Client, vb uint16) error {
		resp, err := mc.Add(vb, key, 0, int(t.Seconds()),
			mustEncode(&jm))
		if err != nil {
			return err
		}
		if resp.Status == gomemcached.KEY_EEXISTS {
			return alreadyRunning
		}
		if resp.Status != gomemcached.SUCCESS {
			return fmt.Errorf("Wanted success, got %v", resp.Status)
		}
		return nil
	})

	if err == nil {
		err = startedTask(name)
		defer endedTask(name)
		if err != nil {
			return err
		}
		err = f()
	} else if err == alreadyRunning {
		err = nil
	}

	return err
}

func runNamedLocalTask(name string, t time.Duration, f func() error) error {
	return runNamedGlobalTask(serverId+"/"+name, t, f)
}

func logErrors(from string, errs <-chan error) {
	for e := range errs {
		log.Printf("%v - %v", from, e)
	}
}

func validateLocal() error {
	log.Printf("Validating Local Blobs")

	me := StorageNode{name: serverId}

	oids := make(chan string, 1000)
	errs := make(chan error)
	quit := make(chan bool)
	defer close(quit)

	go logErrors("local validation", errs)

	go me.iterateBlobs(oids, nil, quit)

	start := time.Now()
	count := 0
	for hash := range oids {
		if !hasBlob(hash) {
			log.Printf("Mistakenly registered with %v",
				hash)
			removeBlobOwnershipRecord(hash, serverId)
		}
		count++
	}
	log.Printf("Validated %v files in %v", count, time.Since(start))
	return nil
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

	viewRes := struct {
		Rows []struct {
			Id  string
			Doc struct {
				Json struct {
					Nodes map[string]string
				}
			}
		}
		Errors []cb.ViewError
	}{}

	log.Printf("Cleaning up node %v with count %v",
		node, globalConfig.NodeCleanCount)
	err = couchbase.ViewCustom("cbfs", "node_blobs",
		map[string]interface{}{
			"key":          node,
			"limit":        globalConfig.NodeCleanCount,
			"reduce":       false,
			"include_docs": true,
			"stale":        false,
		}, &viewRes)
	if err != nil {
		log.Printf("Error executing node_blobs view: %v", err)
		return
	}
	foundRows := 0
	for _, r := range viewRes.Rows {
		foundRows++

		if len(r.Doc.Json.Nodes) < globalConfig.MinReplicas {
			salvageBlob(r.Id[1:], node, nodes)
		} else {
			// There are enough copies, just remove this one.
			removeBlobOwnershipRecord(r.Id[1:], node)
		}
	}
	log.Printf("Removed %v blobs from %v", foundRows, node)
	if foundRows == 0 && len(viewRes.Errors) == 0 {
		log.Printf("Removing node record: %v", node)
		err = couchbase.Delete("/" + node)
		if err != nil {
			log.Printf("Error deleting %v node record: %v", node, err)
		}
		err = couchbase.Delete("/" + node + "/r")
		if err != nil {
			log.Printf("Error deleting %v node counter: %v", node, err)
		}
		err = couchbase.Delete("/" + node + "/tasks")
		if err != nil {
			log.Printf("Error deleting %v task list: %v", node, err)
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

func anyTaskRunning(taskNames []string) bool {
	for _, task := range taskNames {
		if taskRunning(task) {
			return true
		}
	}
	return false
}

func relockTask(taskName string) bool {
	k := "/@" + taskName

	task := globalPeriodicJobs[taskName]
	if task == nil {
		task = localPeriodicJobs[taskName]
		k = "/@" + serverId + "/" + taskName
	}

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
		exp := task.period().Seconds()
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

func runMarkedTask(name string, excl []string, f func() error) error {
	for anyTaskRunning(excl) {
		log.Printf("Execution of %v is blocked on one of %v",
			name, excl)
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
	return runMarkedTask("garbageCollectBlobs",
		[]string{"ensureMinReplCount", "trimFullNodes"},
		garbageCollectBlobsTask)
}

func moveSomeOffOf(n StorageNode, nl NodeList) {
	log.Printf("Freeing up some space from %v", n)

	viewRes := struct {
		Rows []struct {
			Id  string
			Doc struct {
				Json struct {
					Nodes  map[string]string
					Length int64
				}
			}
		}
		Errors []cb.ViewError
	}{}

	err := couchbase.ViewCustom("cbfs", "node_blobs",
		map[string]interface{}{
			"key":          n.name,
			"limit":        globalConfig.TrimFullNodesCount,
			"reduce":       false,
			"include_docs": true,
			"stale":        false,
		}, &viewRes)
	if err != nil {
		log.Printf("Error executing node_blobs view: %v", err)
		return
	}

	removed := int64(0)
	log.Printf("Moving %v blobs from %v", len(viewRes.Rows), n)
	for _, row := range viewRes.Rows {
		oid := row.Id[1:]
		candidates := NodeList{}

		removed += row.Doc.Json.Length

		if removed > globalConfig.TrimFullNodesSpace {
			log.Printf("Cleaned up enough from %v, cutting out", n)
			return
		}

		if len(row.Doc.Json.Nodes)-1 < globalConfig.MinReplicas {
			for _, n := range nl {
				if _, ok := row.Doc.Json.Nodes[n.name]; !ok {
					candidates = append(candidates, n)
				}
			}

			candidates = candidates.withAtLeast(
				uint64(globalConfig.TrimFullNodesSpace))

			if len(candidates) == 0 {
				log.Printf("No candidates available to move %v",
					oid)
				continue
			}

			newnode := candidates[rand.Intn(len(candidates))]

			log.Printf("Moving replica of %v from %v to %v",
				oid, n, newnode)
			queueBlobAcquire(newnode, oid, n.name)
		} else {
			// There are enough, just trim it.
			log.Printf("Just trimming %v from %v", oid, n)
			queueBlobRemoval(n, oid)
		}
	}

}

func trimFullNodes() error {
	return runMarkedTask("trimFullNodes",
		[]string{"ensureMinReplCount", "garbageCollectBlobs"},
		trimFullNodesTask)
}

func trimFullNodesTask() error {
	nl, err := findAllNodes()
	if err != nil {
		return err
	}

	toRelieve := nl.withNoMoreThan(uint64(globalConfig.TrimFullNodesSpace))
	if len(toRelieve) == 0 {
		return nil
	}

	hasSpace := nl.withAtLeast(uint64(globalConfig.TrimFullNodesSpace))

	if len(hasSpace) == 0 {
		log.Printf("No needs have sufficient free space")
		return nil
	}

	for _, n := range toRelieve {
		moveSomeOffOf(n, hasSpace)
	}

	return nil
}

func okToClean(oid string) bool {
	ob, err := getBlobOwnership(oid)
	if err == nil {
		_, t := ob.mostRecent()
		if time.Since(t) < time.Hour {
			return false
		}
	}
	return true
}

func garbageCollectBlobsTask() error {
	log.Printf("Garbage collecting blobs without any file references")

	viewRes := struct {
		Rows []struct {
			Key []string
		}
		Errors []cb.ViewError
	}{}

	nm, err := findNodeMap()
	if err != nil {
		return err
	}

	count, skipped := 0, 0
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
						if okToClean(blobId) {
							queueBlobRemoval(n, blobId)
							count++
						} else {
							log.Printf("Not cleaning %v, recently used",
								blobId)
							skipped++
						}
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

	log.Printf("Scheduled %d blobs for deletion, skipped %d", count, skipped)
	return nil
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
			"reduce":   false,
			"limit":    *maxStartupObjects,
			"startkey": 1,
			"endkey":   *maxStartupRepls - 1,
			"stale":    false,
		},
		&viewRes)

	if err != nil {
		log.Printf("Error finding docs to suck: %v", err)
		return
	}

	log.Printf("Going to fetch %v startup objects", len(viewRes.Rows))

	for _, r := range viewRes.Rows {
		if !hasBlob(r.Id[1:]) {
			queueBlobFetch(r.Id[1:], "")
		}
	}
}

func runGlobalPeriodicJob(name string, job *PeriodicJob) {
	time.Sleep(time.Second * time.Duration(5+rand.Intn(60)))
	for {
		err := runNamedGlobalTask(name, job.period(), job.f)
		if err != nil {
			log.Printf("Error running global task %v: %v", name, err)
		}
		time.Sleep(job.period() + time.Second)
	}
}

func runLocalPeriodicJob(name string, job *PeriodicJob) {
	time.Sleep(time.Second * time.Duration(5+rand.Intn(60)))
	for {
		err := runNamedLocalTask(name, job.period(), job.f)
		if err != nil {
			log.Printf("Error running local task %v: %v", name, err)
		}
		time.Sleep(job.period() + time.Second)
	}
}

func runPeriodicJobs() {
	for n, j := range globalPeriodicJobs {
		if j.period() == 0 {
			log.Printf("global job %v is misconfigured, ignoring", n)
		} else {
			go runGlobalPeriodicJob(n, j)
		}
	}
	for n, j := range localPeriodicJobs {
		if j.period() == 0 {
			log.Printf("local job %v is misconfigured, ignoring", n)
		} else {
			go runLocalPeriodicJob(n, j)
		}
	}
}

func updateConfig() error {
	conf, err := RetrieveConfig()
	if err != nil {
		return err
	}
	globalConfig = conf
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
