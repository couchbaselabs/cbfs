package main

import (
	"log"
	"os"

	"github.com/couchbaselabs/cbfs/client"
)

func getClient(u string) *cbfsclient.Client {
	c, err := cbfsclient.New(u)
	if err != nil {
		log.Fatalf("Error getting config: %v", err)
	}
	return c
}

func getConfCommand(u string, args []string) {
	conf, err := getClient(u).GetConfig()
	if err != nil {
		log.Fatalf("Error getting config: %v", err)
	}
	conf.Dump(os.Stdout)
}

func setConfCommand(u string, args []string) {
	key, val := args[0], args[1]
	err := getClient(u).SetConfigParam(key, val)
	if err != nil {
		log.Fatalf("Error setting config: %v", err)
	}
}
