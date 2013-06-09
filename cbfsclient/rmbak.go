package main

import (
	"flag"
	"log"
	"net/http"
	"net/url"
	"sort"
)

var rmbakFlags = flag.NewFlagSet("rmbak", flag.ExitOnError)
var rmbakNoop = rmbakFlags.Bool("n", false,
	"Don't perform any destructive actions.")
var rmbakKeep = rmbakFlags.Int("keep", 14, "Number of old backups to keep")
var rmbakVerbose = rmbakFlags.Bool("v", false, "Verbose logging")

type backups []Backup

func (b backups) Len() int {
	return len(b)
}

func (b backups) Less(i, j int) bool {
	return b[i].When.Before(b[i].When)
}

func (b backups) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func rmBakCommand(ustr string, args []string) {
	rmbakFlags.Parse(args)
	*rmVerbose = *rmbakVerbose
	*rmNoop = *rmbakNoop

	u, err := url.Parse(ustr)
	maybeFatal(err, "Error parsing URL: %v", err)

	u.Path = "/.cbfs/backup/"

	data := struct{ Backups backups }{}

	err = getJsonData(u.String(), &data)
	maybeFatal(err, "Error getting backup data: %v", err)

	sort.Sort(data.Backups)

	if len(data.Backups) < *rmbakKeep {
		if *rmbakVerbose {
			log.Printf("Only %v backups. Not cleaning", len(data.Backups))
		}
		return
	}

	torm := data.Backups[:len(data.Backups)-*rmbakKeep]
	if *rmbakVerbose {
		log.Printf("Removing %v backups, keeping %v",
			len(torm), len(data.Backups)-len(torm))
	}

	for i := 0; i < 4; i++ {
		rmWg.Add(1)
		go rmWorker()
	}

	for _, b := range torm {
		if *rmbakVerbose {
			log.Printf("Removing %v", b.Filename)
		}
		rmCh <- relativeUrl(u.String(), b.Filename)
	}
	close(rmCh)

	rmWg.Wait()

	// Issue a backup cleanup request.

	u.Path = "/.cbfs/backup/mark/"
	u.RawQuery = "all=true"

	res, err := http.Post(u.String(),
		"application/x-www-form-urlencoded", nil)
	maybeFatal(err, "Error executing POST to %v - %v", u, err)

	if res.StatusCode != 204 {
		log.Fatalf("Error marking backups: %v", res.Status)
	}
}
