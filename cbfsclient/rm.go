package main

import (
	"flag"
	"sync"

	"github.com/couchbaselabs/cbfs/client"
	"github.com/couchbaselabs/cbfs/tool"
)

var rmFlags = flag.NewFlagSet("rm", flag.ExitOnError)
var rmRecurse = rmFlags.Bool("r", false, "Recursively delete")
var rmVerbose = rmFlags.Bool("v", false, "Verbose")
var rmNoop = rmFlags.Bool("n", false, "Dry run")
var rmWg = sync.WaitGroup{}
var rmCh = make(chan string, 100)

func rmDashR(client *cbfsclient.Client, under string) {
	listing, err := client.ListDepth(under, 8192)
	cbfstool.MaybeFatal(err, "Error listing files at %q: %v", under, err)

	for fn := range listing.Files {
		rmCh <- quotingReplacer.Replace(fn)
	}
}

func rmFile(client *cbfsclient.Client, u string) error {
	if *rmNoop {
		return nil
	}
	err := client.Rm(u)
	if err == cbfsclient.Missing {
		err = nil
	}
	return err
}

func rmWorker(client *cbfsclient.Client) {
	defer rmWg.Done()

	for u := range rmCh {
		cbfstool.Verbose(*rmVerbose, "Deleting %v", u)

		err := rmFile(client, u)
		cbfstool.MaybeFatal(err, "Error removing %v: %v", u, err)
	}
}

func rmCommand(u string, args []string) {
	rmFlags.Parse(args)

	client, err := cbfsclient.New(u)
	cbfstool.MaybeFatal(err, "Error creating cbfs client: %v", err)

	for i := 0; i < 4; i++ {
		rmWg.Add(1)
		go rmWorker(client)
	}

	for _, path := range rmFlags.Args() {
		if *rmRecurse {
			rmDashR(client, path)
		} else {
			rmCh <- path
		}
	}
	close(rmCh)

	rmWg.Wait()
}
