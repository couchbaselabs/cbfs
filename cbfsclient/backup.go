package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var backupFlags = flag.NewFlagSet("backup", flag.ExitOnError)
var backupWait = backupFlags.Bool("w", false, "Wait for backup to complete")

func backupCommand(ustr string, args []string) {
	backupFlags.Parse(args)

	u, err := url.Parse(ustr)
	maybeFatal(err, "Error parsing URL: %v", err)

	if backupFlags.NArg() < 1 {
		log.Fatalf("Filename is required")
	}
	fn := backupFlags.Arg(0)

	u.Path = "/.cbfs/backup/"

	form := url.Values{
		"fn": []string{fn},
		"bg": []string{strconv.FormatBool(*backupWait == false)},
	}

	start := time.Now()
	res, err := http.Post(u.String(),
		"application/x-www-form-urlencoded",
		strings.NewReader(form.Encode()))
	maybeFatal(err, "Error executing POST to %v - %v", u, err)

	defer res.Body.Close()
	if !(res.StatusCode == 202 || res.StatusCode == 201) {
		log.Printf("backup error: %v", res.Status)
		io.Copy(os.Stderr, res.Body)
		os.Exit(1)
	}

	if *backupWait {
		log.Printf("Completed backup to %v in %v", fn, time.Since(start))
	} else {
		log.Printf("Submitted backup task for %v", fn)
	}
}
