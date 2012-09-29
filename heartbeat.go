package main

import (
	"errors"
	"log"
	"net"
	"net/url"
	"strings"
	"time"
)

var noFSFree = errors.New("no filesystemFree")

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
