package main

import (
	"errors"
	"flag"
	"io/ioutil"
	"strings"
	"time"
)

var serverId string

func init() {
	flag.StringVar(&serverId, "nodeID", "",
		"Node ID (defaults to what's stored in guid file or arbitrary)")
}

func validateServerId(s string) error {
	invalid := errors.New("Invalid server id: " + s)
	switch {
	case len(s) == 0:
		return invalid
	case s[0] == '/' || s[0] == '@':
		return invalid
	}
	return nil
}

func initServerId() error {
	var err error
	var bytes []byte
	if serverId == "" {
		bytes, err = ioutil.ReadFile(*guidFile)
	}
	if len(bytes) > 0 && err == nil {
		serverId = strings.TrimSpace(string(bytes))
	} else {
		serverId = time.Now().UTC().Format(time.RFC3339Nano)
		err = ioutil.WriteFile(*guidFile,
			[]byte(serverId), 0666)
	}
	return err
}
