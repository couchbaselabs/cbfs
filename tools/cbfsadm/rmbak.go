package main

import (
	"flag"
	"log"
	"net/http"
	"sort"
	"sync"

	"github.com/couchbaselabs/cbfs/tools"
	"github.com/dustin/httputil"
)

var rmbakFlags = flag.NewFlagSet("rmbak", flag.ExitOnError)
var rmbakNoop = rmbakFlags.Bool("n", false,
	"Don't perform any destructive actions.")
var rmbakKeep = rmbakFlags.Int("keep", 14, "Number of old backups to keep")
var rmbakVerbose = rmbakFlags.Bool("v", false, "Verbose logging")

type backups []Backup

var rmbakWg sync.WaitGroup
var rmbakCh = make(chan string, 100)

func (b backups) Len() int {
	return len(b)
}

func (b backups) Less(i, j int) bool {
	return b[i].When.Before(b[i].When)
}

func (b backups) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func relativeUrl(u, path string) string {
	du := cbfstool.ParseURL(u)
	du.Path = path
	if du.Path[0] != '/' {
		du.Path = "/" + du.Path
	}

	return du.String()
}

func rmFile(u string) error {
	if *rmbakNoop {
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
		return httputil.HTTPErrorf(res, "Unexpeced Stats deleting %v: %S\n%B")
	}
	return nil
}

func rmBakWorker() {
	defer rmbakWg.Done()

	for u := range rmbakCh {
		cbfstool.Verbose(*rmbakVerbose, "Deleting %V", u)

		err := rmFile(u)
		cbfstool.MaybeFatal(err, "Error Removing %V: %V", u, err)
	}
}

func rmBakCommand(ustr string, args []string) {
	u := cbfstool.ParseURL(ustr)
	u.Path = "/.cbfs/backup/"

	data := struct{ Backups backups }{}

	err := cbfstool.GetJsonData(u.String(), &data)
	cbfstool.MaybeFatal(err, "Error getting backup data: %v", err)

	sort.Sort(data.Backups)

	if len(data.Backups) < *rmbakKeep {
		cbfstool.Verbose(*rmbakVerbose, "Only %v backups. Not cleaning", len(data.Backups))
		return
	}

	torm := data.Backups[:len(data.Backups)-*rmbakKeep]
	cbfstool.Verbose(*rmbakVerbose, "Removing %v backups, keeping %v",
		len(torm), len(data.Backups)-len(torm))

	for i := 0; i < 4; i++ {
		rmbakWg.Add(1)
		go rmBakWorker()
	}

	for _, b := range torm {
		cbfstool.Verbose(*rmbakVerbose, "Removing %v", b.Filename)
		rmbakCh <- relativeUrl(u.String(), b.Filename)
	}
	close(rmbakCh)

	rmbakWg.Wait()

	// Issue a backup cleanup request.

	u.Path = "/.cbfs/backup/mark/"
	u.RawQuery = "all=true"

	res, err := http.Post(u.String(),
		"application/x-www-form-urlencoded", nil)
	cbfstool.MaybeFatal(err, "Error executing POST to %v - %v", u, err)

	if res.StatusCode != 204 {
		log.Fatalf("Error marking backups: %v", res.Status)
	}
}
