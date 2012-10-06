package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

var noFSFree = errors.New("no filesystemFree")

var spaceUsed int64

func availableSpace() uint64 {
	freeSpace, err := filesystemFree()
	if err != nil && err != noFSFree {
		log.Printf("Error getting filesystem info: %v", err)
	}

	if maxStorage > 0 {
		avail := int64(maxStorage) - spaceUsed
		if avail < 0 {
			avail = 0
		}
		if int64(freeSpace) > avail {
			freeSpace = uint64(avail)
		}
	}
	return freeSpace
}

func updateSpaceUsed() error {
	viewRes := struct {
		Rows []struct {
			Value float64
		}
	}{}

	err := couchbase.ViewCustom("cbfs", "node_size",
		map[string]interface{}{
			"group_level": 1,
			"key":         serverId,
		}, &viewRes)
	if err != nil {
		return err
	}

	if len(viewRes.Rows) != 1 {
		return fmt.Errorf("Expected 1 result, got %v", viewRes.Rows)
	}

	atomic.StoreInt64(&spaceUsed, int64(viewRes.Rows[0].Value))
	return nil
}

func updateSpaceUsedLoop() {
	for {
		err := updateSpaceUsed()
		if err == nil {
			time.Sleep(time.Minute)
		} else {
			log.Printf("Error updating space used: %v", err)
			time.Sleep(time.Second * 5)
		}
	}
}

func heartbeat() {
	go updateSpaceUsedLoop()
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
			Used:     spaceUsed,
			Free:     availableSpace(),
		}

		err = couchbase.Set("/"+serverId, 0, aboutMe)
		if err != nil {
			log.Printf("Failed to record a heartbeat: %v", err)
		}
		time.Sleep(globalConfig.HeartbeatFreq)
	}
}
