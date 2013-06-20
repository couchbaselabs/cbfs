package main

import (
	"os"

	"github.com/couchbaselabs/cbfs/client"
)

func getClient(u string) *cbfsclient.Client {
	c, err := cbfsclient.New(u)
	maybeFatal(err, "Error getting config: %v", err)
	return c
}

func getConfCommand(u string, args []string) {
	conf, err := getClient(u).GetConfig()
	maybeFatal(err, "Error getting config: %v", err)
	conf.Dump(os.Stdout)
}

func setConfCommand(u string, args []string) {
	key, val := args[0], args[1]
	err := getClient(u).SetConfigParam(key, val)
	maybeFatal(err, "Error setting config: %v", err)
}
