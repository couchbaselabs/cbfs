package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/couchbaselabs/cbfs/tools"
)

var restoreFlags = flag.NewFlagSet("restore", flag.ExitOnError)
var restoreForce = restoreFlags.Bool("f", false, "Overwrite existing")
var restoreNoop = restoreFlags.Bool("n", false, "Noop")
var restoreVerbose = restoreFlags.Bool("v", false, "Verbose restore")
var restorePat = restoreFlags.String("match", ".*", "Regex for paths to match")
var restoreWorkers = restoreFlags.Int("workers", 4, "Number of restore workers")

type restoreWorkItem struct {
	Path string
	Meta *json.RawMessage
}

func restoreFile(base, path string, data interface{}) error {
	if *restoreNoop {
		log.Printf("NOOP would restore %v", path)
		return nil
	}

	fileMetaBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	u := cbfstool.ParseURL(base)
	u.Path = fmt.Sprintf("/.cbfs/backup/restore/%v", path)
	res, err := http.Post(u.String(),
		"application/json",
		bytes.NewReader(fileMetaBytes))
	cbfstool.MaybeFatal(err, "Error executing POST to %v - %v", u, err)

	defer res.Body.Close()
	switch {
	case res.StatusCode == 201:
		log.Printf("Restored %v", path)
		// OK
	case res.StatusCode == 409 && !*restoreForce:
		// OK
	default:
		log.Printf("restore error on %v: %v", path, res.Status)
		io.Copy(os.Stderr, res.Body)
		fmt.Fprintln(os.Stderr)
		return fmt.Errorf("HTTP Error restoring %v: %v", path, res.Status)
	}

	return nil
}

func restoreWorker(wg *sync.WaitGroup, base string, ch <-chan restoreWorkItem) {
	defer wg.Done()
	for ob := range ch {
		err := restoreFile(base, ob.Path, ob.Meta)
		if err != nil {
			log.Printf("Error restoring %v: %v",
				ob.Path, err)
		}
	}
}

func restoreCommand(ustr string, args []string) {
	regex, err := regexp.Compile(*restorePat)
	cbfstool.MaybeFatal(err, "Error parsing match pattern: %v", err)

	fn := restoreFlags.Arg(0)

	start := time.Now()

	f, err := os.Open(fn)
	cbfstool.MaybeFatal(err, "Error opening restore file: %v", err)

	defer f.Close()
	gz, err := gzip.NewReader(f)
	cbfstool.MaybeFatal(err, "Error uncompressing restore file: %v", err)

	wg := &sync.WaitGroup{}

	ch := make(chan restoreWorkItem)
	for i := 0; i < *restoreWorkers; i++ {
		wg.Add(1)
		go restoreWorker(wg, ustr, ch)
	}

	d := json.NewDecoder(gz)
	nfiles := 0
	done := false
	for !done {
		ob := restoreWorkItem{}

		err := d.Decode(&ob)
		switch err {
		case nil:
			if regex.MatchString(ob.Path) {
				nfiles++
				ch <- ob
			}
		case io.EOF:
			done = true
			break
		default:
			log.Fatalf("Error reading backup file: %v", err)
		}
	}
	close(ch)
	wg.Wait()

	log.Printf("Restored %v files in %v", nfiles, time.Since(start))
}
