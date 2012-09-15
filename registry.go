package main

import (
	"io/ioutil"
	"strings"
	"time"
)

var serverId string

func initServerId() error {
	bytes, err := ioutil.ReadFile(*guidFile)
	if err == nil {
		serverId = strings.TrimSpace(string(bytes))
	} else {
		serverId = time.Now().UTC().Format(time.RFC3339Nano)
		err = ioutil.WriteFile(*guidFile,
			[]byte(serverId), 0666)
	}
	return err
}
