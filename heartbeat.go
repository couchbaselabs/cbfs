package main

import (
	"encoding/json"
	"flag"
	"fmt"
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

type AboutNode struct {
	Addr     string    `json:"addr"`
	Type     string    `json:"type"`
	Time     time.Time `json:"time"`
	BindAddr string    `json:"bindaddr"`
	Hash     string    `json:"hash"`
}

type PeriodicJob struct {
	period time.Duration
	f      func() error
}

var periodicJobs = map[string]PeriodicJob{
	"checkStaleNodes": PeriodicJob{
		time.Minute * 5,
		checkStaleNodes,
	},
}

func getNodeAddress(sid string) (string, error) {
	sidkey := "/" + sid
	aboutSid := AboutNode{}
	err := couchbase.Get(sidkey, &aboutSid)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(aboutSid.BindAddr, ":") {
		return aboutSid.Addr + aboutSid.BindAddr, nil
	}
	return aboutSid.BindAddr, nil
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
	}

	return true
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

		aboutMe := AboutNode{
			Addr:     localAddr,
			Type:     "storage",
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

func reconcile() error {
	explen := getHash().Size() * 2
	return filepath.Walk(*root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && !strings.HasPrefix(info.Name(), "tmp") &&
			len(info.Name()) == explen {
			// I can do way more efficient stuff than this.
			recordBlobOwnership(info.Name(), info.Size())
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

func checkStaleNodes() error {
	// TODO:  Make this not lie.
	log.Printf("Checking stale nodes")
	return nil
}

func runPeriodicJob(name string, job PeriodicJob) {
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
