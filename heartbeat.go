package main

import (
	"flag"
	"log"
	"net"
	"net/url"
	"strings"
	"time"
)

var heartFreq = flag.Duration("heartbeat", 10*time.Second,
	"Heartbeat frequency")

type AboutNode struct {
	Addr     string `json:"addr"`
	Type     string `json:"type"`
	Time     string `json:"time"`
	BindAddr string `json:"bindaddr"`
	Hash     string `json:"hash"`
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

		aboutMe := AboutNode{}
		aboutMe.Addr = localAddr
		aboutMe.Type = "storage"
		aboutMe.Time = time.Now().UTC().String()
		aboutMe.BindAddr = *bindAddr
		aboutMe.Hash = *hashType

		err = couchbase.Set("/"+serverIdentifier(), aboutMe)
		if err != nil {
			log.Printf("Failed to record a heartbeat: %v", err)
		}
		time.Sleep(*heartFreq)
	}
}
