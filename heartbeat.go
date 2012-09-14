package main

import (
	"log"
	"net"
	"net/url"
	"strings"
	"time"
)

func heartbeat() {
	for {

		u, err := url.Parse(*couchbaseServer)
		c, err := net.Dial("tcp", u.Host)
		localAddr := ""
		if err == nil {
			localAddr = strings.Split(c.LocalAddr().String(), ":")[0]
			c.Close()
		}

		aboutMe := map[string]interface{}{
			"addr":     localAddr,
			"type":     "storage",
			"time":     time.Now().UTC(),
			"bindaddr": *bindAddr,
		}
		intfs, err := net.InterfaceAddrs()
		if err == nil {
			addrs := []string{}
			for _, intf := range intfs {
				addrs = append(addrs, intf.String())
			}
			aboutMe["interfaces"] = addrs
		}
		err = couchbase.Set("/"+serverIdentifier(), aboutMe)
		if err != nil {
			log.Printf("Failed to record a heartbeat: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
}
