package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"time"

	"encoding/hex"
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
	period       func() time.Duration
	f            func() error
	excl         []string
	ticker       *time.Ticker
	configChange chan interface{}
}

type periodicJobRecipe struct {
	period func() time.Duration
	f      func() error
	excl   []string
}

var globalPeriodicJobRecipes = map[string]*periodicJobRecipe{}
var localPeriodicJobRecipes = map[string]*periodicJobRecipe{}
var taskInducers = map[string]chan time.Time{}

func init() {
	globalPeriodicJobRecipes = map[string]*periodicJobRecipe{
		"checkStaleNodes": {
			func() time.Duration {
				return globalConfig.StaleNodeCheckFreq
			},
			checkStaleNodes,
			nil,
		},
		"garbageCollectBlobs": {
			func() time.Duration {
				return globalConfig.GCFreq
			},
			garbageCollectBlobs,
			[]string{"ensureMinReplCount", "trimFullNodes"},
		},
		"ensureMinReplCount": {
			func() time.Duration {
				return globalConfig.UnderReplicaCheckFreq
			},
			ensureMinimumReplicaCount,
			[]string{"garbageCollectBlobs", "trimFullNodes"},
		},
		"pruneExcessiveReplicas": {
			func() time.Duration {
				return globalConfig.OverReplicaCheckFreq
			},
			pruneExcessiveReplicas,
			nil,
		},
		"updateNodeSizes": {
			func() time.Duration {
				return globalConfig.UpdateNodeSizesFreq
			},
			updateNodeSizes,
			nil,
		},
		"trimFullNodes": {
			func() time.Duration {
				return globalConfig.TrimFullNodesFreq
			},
			trimFullNodes,
			[]string{"ensureMinReplCount", "garbageCollectBlobs"},
		},
	}

	localPeriodicJobRecipes = map[string]*periodicJobRecipe{
		"validateLocal": {
			func() time.Duration {
				return globalConfig.LocalValidationFreq
			},
			validateLocal,
			[]string{"reconcile", "quickReconcile"},
		},
		"reconcile": {
			func() time.Duration {
				return globalConfig.ReconcileFreq
			},
			reconcile,
			[]string{"validateLocal", "quickReconcile"},
		},
		"quickReconcile": {
			func() time.Duration {
				return globalConfig.QuickReconcileFreq
			},
			quickReconcile,
			[]string{"reconcile", "validateLocal"},
		},
		"cleanTmp": {
			func() time.Duration {
				return time.Hour
			},
			cleanTmpFiles,
			nil,
		},
	}

	initTaskMetrics()
}

type JobMarker struct {
	Node    string    `json:"node"`
	Started time.Time `json:"started"`
	Type    string    `json:"type"`
}

type TaskState struct {
	State     string    `json:"state"`
	Timestamp time.Time `json:"ts"`
}

type TaskList struct {
	Tasks map[string]TaskState `json:"tasks"`
	Node  string               `json:"node"`
	Type  string               `json:"type"`
}

func setTaskState(task, state string) error {
	k := "/@" + serverId + "/tasks"
	ts := time.Now().UTC()

	err := couchbase.Update(k, 0, func(in []byte) ([]byte, error) {
		ob := TaskList{Tasks: map[string]TaskState{}}
		json.Unmarshal(in, &ob)
		if state == "" {
			delete(ob.Tasks, task)
			if len(ob.Tasks) == 0 {
				return nil, nil
			}
		} else {
			ob.Tasks[task] = TaskState{state, ts}
		}
		ob.Type = "tasks"
		ob.Node = serverId
		return json.Marshal(ob)
	})

	if err == cb.UpdateCancel {
		err = nil
	}
	return err
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
func runNamedTask(name string, job *PeriodicJob, force bool) error {
	key := "/@" + name

	t := job.period()
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
		var resp *gomemcached.MCResponse
		var err error
		if force {
			resp, err = memcached.UnwrapMemcachedError(mc.Set(vb,
				key, 0, int(t.Seconds()), mustEncode(&jm)))
		} else {
			resp, err = memcached.UnwrapMemcachedError(mc.Add(vb,
				key, 0, int(t.Seconds()), mustEncode(&jm)))
		}
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
		err = setTaskState(name, "preparing")
		defer setTaskState(name, "")
		if err != nil {
			return err
		}
		err = runMarkedTask(name, job)
	} else if err == alreadyRunning {
		err = nil
	}

	return err
}

func runGlobalTask(name string, job *PeriodicJob, force bool) error {
	return runNamedTask(name, job, force)
}

func runLocalTask(name string, job *PeriodicJob, force bool) error {
	return runNamedTask(serverId+"/"+name, job, force)
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

	nl, err := findAllNodes()
	if err != nil {
		log.Printf("Error getting node list for local validation: %v",
			err)
	}

	start := time.Now()
	count := 0
	for hash := range oids {
		if !hasBlob(hash) {
			log.Printf("Mistakenly registered with %v",
				hash)
			owners := removeBlobOwnershipRecord(hash, serverId)

			if owners < globalConfig.MinReplicas {
				log.Printf("Local validation on %v dropped rep to %v",
					hash, owners)
				if !salvageBlob(hash, "",
					globalConfig.MinReplicas-owners, nl) {

					log.Printf("Internode queue full salvaging %v",
						hash)
				}
			}
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
			if !salvageBlob(r.Id[1:], node, 1, nodes) {
				log.Printf("Queue is full during cleanup")
				break
			}
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
		err = removeFromNodeRegistry(node)
		if err != nil {
			log.Printf("Error deleting %v from registry: %v", node, err)
		}
		cleanNodeTaskMarkers(node)
	}
}

func cleanNodeTaskMarkers(node string) {
	err := couchbase.Delete("/@" + node + "/tasks")
	if err != nil {
		log.Printf("Error removing %v's task list: %v", node, err)
	}
	for name := range globalPeriodicJobRecipes {
		k := "/@" + name + "/running"

		err = couchbase.Update(k, 0, func(in []byte) ([]byte, error) {
			if len(in) == 0 {
				return nil, cb.UpdateCancel
			}
			ob := map[string]string{}
			err := json.Unmarshal(in, &ob)
			if err == nil && ob["node"] == node {
				return nil, nil
			}
			return nil, cb.UpdateCancel
		})

		if err != nil && err != cb.UpdateCancel {
			log.Printf("Error removing %v's %v running marker: %v",
				node, name, err)
		}
	}
}

func checkStaleNodes() error {
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
			log.Printf("Node %v missed heartbeat schedule: %v",
				node.name, d)
			go cleanupNode(node.name)
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

	task := globalPeriodicJobRecipes[taskName]
	if task == nil {
		task = localPeriodicJobRecipes[taskName]
		k = "/@" + serverId + "/" + taskName
	}

	err := couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		resp, err := mc.Get(vb, k)
		if err != nil {
			return err
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

		_, err = mc.Send(req)
		return err
	})

	return err == nil
}

func runMarkedTask(name string, job *PeriodicJob) error {
	start := time.Now()
	for anyTaskRunning(job.excl) {
		log.Printf("Execution of %v is blocked on one of %v",
			name, job.excl)
		time.Sleep(5 * time.Second)
		if time.Since(start) > job.period() {
			return fmt.Errorf("Execution blocked for too long")
		}
	}

	if !strings.HasPrefix(name, serverId+"/") && !relockTask(name) {
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
	err = setTaskState(name, "running")
	if err != nil {
		// I'd rather not run a task than erroneously report
		// is as running when we couldn't update the state to
		// running.
		return err
	}

	defer endedTask(name, time.Now())
	return job.f()
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
				globalConfig.TrimFullNodesSpace)

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
	nl, err := findAllNodes()
	if err != nil {
		return err
	}

	toRelieve := nl.withNoMoreThan(globalConfig.TrimFullNodesSpace)
	if len(toRelieve) == 0 {
		return nil
	}

	hasSpace := nl.withAtLeast(globalConfig.TrimFullNodesSpace)

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
	return markGarbage(oid) == nil
}

func garbageCollectBlobs() error {
	if !globalConfig.GCEnabled {
		log.Printf("Garbage collection is disabled -- skipping")
		return nil
	}

	log.Printf("Garbage collecting blobs without any file references")

	backedup, err := loadExistingHashes()
	if err != nil {
		return err
	}

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

	count, skipped, inBackup := 0, 0, 0
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
			if len(r.Key) < 3 {
				log.Printf("Malformed key in gc result: %+v", r)
				continue
			}
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
						if b, err := hex.DecodeString(blobId); err == nil &&
							backedup.Contains(b) {

							inBackup++
						} else if okToClean(blobId) {
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

	log.Printf("Scheduled %d blobs for deletion, skipped %d, in backup %d",
		count, skipped, inBackup)
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
			if !maybeQueueBlobFetch(r.Id[1:], "") {
				log.Printf("Fetch queue is full, giving up.")
				return
			}
		}
	}
}

func periodicTaskGasp(name string) {
	buf := make([]byte, 8192)
	w := runtime.Stack(buf, false)
	log.Fatalf("Fatal error in periodic job %v: %v\n%s",
		name, recover(), buf[:w])
}

func runPeriodicJob(name string, job *PeriodicJob, inducer chan time.Time,
	executor func(name string, job *PeriodicJob, force bool) error) {

	defer periodicTaskGasp(name)

	time.Sleep(time.Second * time.Duration(5+rand.Intn(60)))
	period := job.period()
	if period < time.Second {
		log.Printf("Period for %v is too short (%v), going reasonable",
			name, period)
		period = time.Hour * 24
	}
	job.ticker = time.NewTicker(period)

	for {
		select {
		case <-inducer:
			err := executor(name, job, true)
			if err != nil {
				log.Printf("Error running induced task %v: %v", name, err)
			}

		case <-job.ticker.C:
			err := executor(name, job, false)
			if err != nil {
				log.Printf("Error running task %v: %v", name, err)
			}

		case <-job.configChange:
			if period != job.period() {
				period = job.period()
				if period > 0 {
					log.Printf("Config change for %v to %v",
						name, period)
					job.ticker.Stop()
					job.ticker = time.NewTicker(period)
				} else {
					log.Printf("New period for %v is too short: %v",
						name, period)
				}
			}
		}
	}
}

func launchJobs(m map[string]*periodicJobRecipe,
	executor func(string, *PeriodicJob, bool) error) {

	for n, recipe := range m {
		inducer := make(chan time.Time, 1)
		j := &PeriodicJob{
			period:       recipe.period,
			f:            recipe.f,
			excl:         recipe.excl,
			configChange: make(chan interface{}),
		}
		if _, exists := taskInducers[n]; exists {
			log.Fatalf("Duplicate task launching: %v", n)
		}
		taskInducers[n] = inducer
		confBroadcaster.Register(j.configChange)
		go runPeriodicJob(n, j, inducer, executor)
	}
}

func runPeriodicJobs() {
	launchJobs(globalPeriodicJobRecipes, runGlobalTask)
	launchJobs(localPeriodicJobRecipes, runLocalTask)
}

func startTasks() {
	cleanNodeTaskMarkers(serverId)
	// Forget the last time we did local validation. We're
	// restarting, so things have changed.
	couchbase.Delete("/@" + serverId + "/validateLocal")
	// And quick reconcile...
	couchbase.Delete("/@" + serverId + "/quickReconcile")
	runPeriodicJobs()
	// Immediately induce local reconciliation to get our blobs
	// registered.
	err := induceTask("quickReconcile")
	if err != nil {
		log.Printf("Error inducing initial reconciliation: %v", err)
	}
}

var noSuchTask = errors.New("no such task")
var taskAlreadyQueued = errors.New("task already queued")

func induceTask(name string) error {
	ch := taskInducers[name]
	if ch == nil {
		return noSuchTask
	}
	select {
	case ch <- time.Now():
	default:
		return taskAlreadyQueued
	}
	return nil
}

func updateConfig() error {
	conf, err := RetrieveConfig()
	if err != nil {
		return err
	}
	confBroadcaster.Submit(configChange{globalConfig, conf})
	globalConfig = conf
	return nil
}

func reloadConfig() {
	for _ = range time.Tick(time.Minute) {
		if err := updateConfig(); err != nil && !gomemcached.IsNotFound(err) {
			log.Printf("Error updating config: %v", err)
		}
	}
}
