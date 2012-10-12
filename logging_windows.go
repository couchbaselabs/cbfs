// +build windows !darwin !freebsd !linux !openbsd !netbsd

package main

import (
	corelog "log"
	"log/syslog"
	"os"
)

var log *corelog.Logger

func init() {
	log = corelog.New(os.Stderr, "", 0)
	log.SetFlags(corelog.LstdFlags)
}

func initLogger(slog bool) {
	if slog {
		log.Printf("No syslog support on Windows, using regular logging")
	}
}
