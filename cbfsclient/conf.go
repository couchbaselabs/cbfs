package main

import (
	"log"
	"os"

	"github.com/couchbaselabs/cbfs/client"
)

func getConfCommand(u string, args []string) {
	conf, err := cbfsclient.GetConfig(u)
	if err != nil {
		log.Fatalf("Error getting config: %v", err)
	}
	conf.Dump(os.Stdout)
}

func setConfCommand(u string, args []string) {
	key, val := args[0], args[1]
	err := cbfsclient.SetConfigParam(u, key, val)
	if err != nil {
		log.Fatalf("Error setting config: %v", err)
	}
}
