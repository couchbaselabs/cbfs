package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/couchbaselabs/cbfs/tools"
	"github.com/dustin/go-humanize"
)

var fsckFlags = flag.NewFlagSet("fsck", flag.ExitOnError)
var fsckVerbose = fsckFlags.Bool("v", false, "Use more bandwidth, say more stuff")

func fsckCommand(ustr string, args []string) {
	fsckFlags.Parse(args)

	u, err := url.Parse(ustr)
	cbfstool.MaybeFatal(err, "Error parsing URL: %v", err)

	u.Path = "/.cbfs/fsck/"
	if !*fsckVerbose {
		u.RawQuery = "errsonly=true"
	}

	res, err := http.Get(u.String())
	cbfstool.MaybeFatal(err, "Error executing GET of %v - %v", u, err)

	defer res.Body.Close()
	if res.StatusCode != 200 {
		log.Printf("fsck error: %v", res.Status)
		io.Copy(os.Stderr, res.Body)
		os.Exit(1)
	}

	found := 0
	errors := 0

	if *fsckVerbose {
		done := make(chan bool)
		defer close(done)

		go func(f *int) {
			t := time.NewTicker(time.Second * 5)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					log.Printf("Examined %v files",
						humanize.Comma(int64(found)))
				case <-done:
					return
				}
			}
		}(&found)
	}

	d := json.NewDecoder(res.Body)
	for {
		status := struct {
			Path  string `json:"path"`
			OID   string `json:"oid,omitempty"`
			Reps  int    `json:"reps,omitempty"`
			EType string `json:"etype,omitempty"`
			Error string `json:"error,omitempty"`
		}{}

		err = d.Decode(&status)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("Error executing fsck: %v", err)
		}

		found++
		if status.Error != "" {
			log.Printf("Error on %#v - %v - %v: %v",
				status.Path, status.OID,
				status.EType, status.Error)
			errors++
		}
	}

	log.Printf("Found %v files and %v errors", found, errors)
}
