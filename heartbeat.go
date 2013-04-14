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

func availableSpace() int64 {
	freeSpace, err := filesystemFree()
	if err != nil {
		if err != noFSFree {
			log.Printf("Error getting filesystem info: %v", err)
		}
		freeSpace = maxStorage
	}

	if maxStorage > 0 {
		avail := int64(maxStorage) - spaceUsed
		if avail < 0 {
			avail = 0
		}
		if int64(freeSpace) > avail {
			freeSpace = avail
		}
	}
	return freeSpace
}

func increaseSpaceUsed(by int64) {
	atomic.AddInt64(&spaceUsed, by)
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
	// Give it time to get its initial registry in and settle down
	// some.  7s may be too much, or too little.  It doesn't much
	// matter.  We know "right now" is always too soon, so just
	// let the loop push out.  7 is arbitrary.  I could also
	// attach it with a sync.Once to occur after the first
	// heartbeat.  I would do that if accuracy mattered.
	time.Sleep(time.Second * 7)
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

func oneHeartbeat(startTime time.Time) {
	u, err := url.Parse(*couchbaseServer)
	c, err := net.Dial("tcp", u.Host)
	localAddr := ""
	if err == nil {
		localAddr = strings.Split(c.LocalAddr().String(), ":")[0]
		c.Close()
	}

	aboutMe := StorageNode{
		Addr:      localAddr,
		Type:      "node",
		Started:   startTime,
		Time:      time.Now().UTC(),
		BindAddr:  *bindAddr,
		FrameBind: *framesBind,
		Used:      spaceUsed,
		Free:      availableSpace(),
	}

	err = couchbase.Set("/"+serverId, 0, aboutMe)
	if err != nil {
		log.Printf("Failed to record a heartbeat: %v", err)
	}
}

func heartbeat() {
	defer periodicTaskGasp("heartbeat")

	configChange := make(chan interface{})
	confBroadcaster.Register(configChange)

	startTime := time.Now().UTC()
	go updateSpaceUsedLoop()

	period := globalConfig.HeartbeatFreq
	ticker := time.NewTicker(period)

	for {
		select {
		case <-ticker.C:
			oneHeartbeat(startTime)
		case <-configChange:
			if period != globalConfig.HeartbeatFreq {
				period = globalConfig.HeartbeatFreq
				if period > 0 {
					log.Printf("Config change for %v to %v",
						"heartbeat", period)
					ticker.Stop()
					ticker = time.NewTicker(period)
				} else {
					log.Printf("New period for %v is too short: %v",
						"heartbeat", period)
				}
			}
		}
	}
}
