package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/couchbaselabs/cbfs/client"
)

var rmFlags = flag.NewFlagSet("rm", flag.ExitOnError)
var rmRecurse = rmFlags.Bool("r", false, "Recursively delete")
var rmVerbose = rmFlags.Bool("v", false, "Verbose")
var rmNoop = rmFlags.Bool("n", false, "Dry run")
var rmWg = sync.WaitGroup{}
var rmCh = make(chan string, 100)

func rmDashR(baseUrl string) {
	for strings.HasSuffix(baseUrl, "/") {
		baseUrl = baseUrl[:len(baseUrl)-1]
	}

	listing, err := cbfsclient.List(baseUrl)
	if err != nil {
		log.Fatalf("Error listing files: %v", err)
	}
	for fn := range listing.Files {
		rmCh <- baseUrl + "/" + quotingReplacer.Replace(fn)
	}
	for dn := range listing.Dirs {
		if *rmVerbose {
			log.Printf("Recursing into %v/%v", baseUrl, dn)
		}
		rmDashR(baseUrl + "/" + quotingReplacer.Replace(dn))
	}
}

func rmFile(u string) error {
	if *rmNoop {
		return nil
	}
	req, err := http.NewRequest("DELETE", u, nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode != 204 && res.StatusCode != 404 {
		return fmt.Errorf("Unexpected status deleting %v: %v",
			u, res.Status)
	}
	return nil
}

func rmWorker() {
	defer rmWg.Done()

	for u := range rmCh {
		if *rmVerbose {
			log.Printf("Deleting %v", u)
		}

		err := rmFile(u)
		if err != nil {
			log.Fatalf("Error removing %v: %v", u, err)
		}
	}
}

func rmCommand(u string, args []string) {
	rmFlags.Parse(args)

	if rmFlags.NArg() < 1 {
		log.Fatalf("Filename is required")
	}

	for i := 0; i < 4; i++ {
		rmWg.Add(1)
		go rmWorker()
	}

	for _, path := range rmFlags.Args() {
		ru := relativeUrl(u, path)
		if *rmRecurse {
			rmDashR(ru)
		} else {
			rmCh <- ru
		}
	}
	close(rmCh)

	rmWg.Wait()
}
