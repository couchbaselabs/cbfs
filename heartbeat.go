package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
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

var nodeTooOld = errors.New("Node information is too stale")

type StorageNode struct {
	Addr     string    `json:"addr"`
	Type     string    `json:"type"`
	Time     time.Time `json:"time"`
	BindAddr string    `json:"bindaddr"`
	Hash     string    `json:"hash"`
}

func (a StorageNode) Address() string {
	if strings.HasPrefix(a.BindAddr, ":") {
		return a.Addr + a.BindAddr
	}
	return a.BindAddr
}

type NodeList []StorageNode

func (a NodeList) Len() int {
	return len(a)
}

func (a NodeList) Less(i, j int) bool {
	return a[i].Time.Before(a[j].Time)
}

func (a NodeList) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type PeriodicJob struct {
	period time.Duration
	f      func() error
}

var periodicJobs = map[string]*PeriodicJob{
	"checkStaleNodes": &PeriodicJob{
		time.Minute * 5,
		checkStaleNodes,
	},
}

func adjustPeriodicJobs() error {
	periodicJobs["checkStaleNodes"].period = *staleNodeFreq
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
		data, err := json.Marshal(&jm)
		if err != nil {
			log.Fatalf("Can't jsonify a JobMarker: %v", err)
		}
		resp, err := mc.Add(vb, key, 0, int(t.Seconds()), data)
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
		time.Sleep(*reconcileFreq)
	}
}

func removeBlobOwnershipRecord(h, node string) {
	log.Printf("Cleaning up %v from %v", h, node)

	k := "/" + h
	err := couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		_, err := mc.CAS(vb, k, func(in []byte) []byte {
			ownership := BlobOwnership{}
			err := json.Unmarshal(in, &ownership)
			if err == nil {
				delete(ownership.Nodes, node)
			} else {
				return nil
			}

			rv, err := json.Marshal(&ownership)
			if err != nil {
				log.Fatalf("Error marshaling blob ownership: %v", err)
			}
			return rv
		}, 0)
		return err
	})
	if err != nil {
		log.Printf("Error cleaning %v from %v", node, h)
	}
}

func cleanupNode(node string) {
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
		removeBlobOwnershipRecord(r.ID[1:], node)
		foundRows++
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
	vres, err := couchbase.View("cbfs", "nodes", map[string]interface{}{
		"stale": false})
	if err != nil {
		return err
	}
	for _, r := range vres.Rows {
		ks, ok := r.Key.(string)
		if !ok {
			log.Printf("Wrong key type returned from view: %#v", r)
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, ks)
		if err != nil {
			log.Printf("Error parsing time from %v", r)
			continue
		}
		d := time.Since(t)
		node := r.ID[1:]

		if d > *staleNodeLimit {
			if node == serverId {
				log.Printf("Would've cleaned up myself after %v",
					d)
				continue
			}
			log.Printf("  Node %v missed heartbeat schedule: %v", node, d)
			go cleanupNode(node)
		} else {
			log.Printf("%v is ok at %v", node, d)
		}
	}
	return nil
}

func runPeriodicJob(name string, job *PeriodicJob) {
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
