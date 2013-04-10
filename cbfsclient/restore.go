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
	"net/url"
	"os"
	"regexp"
	"time"
)

var restoreFlags = flag.NewFlagSet("restore", flag.ExitOnError)
var restoreForce = restoreFlags.Bool("f", false, "Overwrite existing")
var restoreNoop = restoreFlags.Bool("n", false, "Noop")
var restoreVerbose = restoreFlags.Bool("v", false, "Verbose restore")
var restorePat = restoreFlags.String("match", ".*", "Regex for paths to match")

func restoreFile(base, path string, data interface{}) error {
	log.Printf("Restoring %v", path)

	if *restoreNoop {
		return nil
	}

	u, err := url.Parse(base)
	if err != nil {
		log.Fatalf("Error parsing URL: %v", err)
	}

	fileMetaBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	u.Path = fmt.Sprintf("/.cbfs/backup/restore/%v", path)
	res, err := http.Post(u.String(),
		"application/json",
		bytes.NewReader(fileMetaBytes))
	if err != nil {
		log.Fatalf("Error executing POST to %v - %v", u, err)
	}
	defer res.Body.Close()
	if res.StatusCode != 201 {
		log.Printf("restore error: %v", res.Status)
		io.Copy(os.Stderr, res.Body)
		fmt.Fprintln(os.Stderr)
		return fmt.Errorf("HTTP Error restoring %v: %v", path, res.Status)
	}

	return nil
}

func restoreCommand(ustr string, args []string) {
	restoreFlags.Parse(args)

	regex, err := regexp.Compile(*restorePat)
	if err != nil {
		log.Fatalf("Error parsing match pattern: %v", err)
	}

	if restoreFlags.NArg() < 1 {
		log.Fatalf("Filename is required")
	}
	fn := restoreFlags.Arg(0)

	start := time.Now()

	f, err := os.Open(fn)
	if err != nil {
		log.Fatalf("Error opening restore file: %v", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		log.Fatalf("Error uncompressing restore file: %v", err)
	}

	d := json.NewDecoder(gz)
	nfiles := 0
	done := false
	for !done {
		ob := struct {
			Path string
			Meta *json.RawMessage
		}{}

		err := d.Decode(&ob)
		switch err {
		case nil:
			if !regex.MatchString(ob.Path) {
				// Skipping
				continue
			}
			nfiles++
			err := restoreFile(ustr, ob.Path, ob.Meta)
			if err != nil {
				log.Printf("Error restoring %v: %v",
					ob.Path, err)
			}
		case io.EOF:
			done = true
			break
		default:
			log.Fatalf("Error reading backup file: %v", err)
		}
	}

	log.Printf("Restored %v files in %v", nfiles, time.Since(start))
}
