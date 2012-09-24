package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"sync"
)

var rmFlags = flag.NewFlagSet("rm", flag.ExitOnError)
var rmRecurse = rmFlags.Bool("r", false, "Recursively delete")
var rmVerbose = rmFlags.Bool("v", false, "Verbose")
var rmWg = sync.WaitGroup{}
var rmCh = make(chan string, 100)

func rmDashR(baseUrl string) {
	for strings.HasSuffix(baseUrl, "/") {
		baseUrl = baseUrl[:len(baseUrl)-1]
	}

	listing, err := listStuff(baseUrl)
	if err != nil {
		log.Fatalf("Error listing files: %v", err)
	}
	for fn := range listing.Files {
		rmCh <- baseUrl + "/" + fn
	}
	for dn := range listing.Dirs {
		if *rmVerbose {
			log.Printf("Recursing into %v/%v", baseUrl, dn)
		}
		rmDashR(baseUrl + "/" + dn)
	}
}

func rmWorker() {
	defer rmWg.Done()

	for u := range rmCh {
		if *rmVerbose {
			log.Printf("Deleting %v", u)
		}

		req, err := http.NewRequest("DELETE", u, nil)
		if err != nil {
			log.Fatalf("Error making new request: %v", err)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("Error issuing delete request: %v", err)
		}
		res.Body.Close()
		if res.StatusCode != 204 && res.StatusCode != 404 {
			log.Fatalf("Unexpected status deleting %v: %v",
				u, err)
		}
	}
}

func rmCommand(args []string) {
	rmFlags.Parse(args)

	if rmFlags.NArg() < 1 {
		log.Fatalf("URL is required")
	}

	for i := 0; i < 4; i++ {
		rmWg.Add(1)
		go rmWorker()
	}

	if *rmRecurse {
		rmDashR(rmFlags.Arg(0))
	} else {
		rmCh <- rmFlags.Arg(0)
	}
	close(rmCh)

	rmWg.Wait()
}
