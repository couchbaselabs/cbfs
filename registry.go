package main

import (
	"io/ioutil"
	"log"
	"strings"
	"time"
)

var serverId string

func serverIdentifier() string {
	if serverId == "" {
		bytes, err := ioutil.ReadFile(*guidFile)
		if err == nil {
			serverId = strings.TrimSpace(string(bytes))
		} else {
			serverId = time.Now().UTC().Format(time.RFC3339Nano)
			err = ioutil.WriteFile(*guidFile,
				[]byte(serverId), 0666)
			if err != nil {
				log.Fatalf("Can't write guid file; %v", err)
			}
		}
	}

	return serverId
}
