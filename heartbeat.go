package main

import (
	"flag"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
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
