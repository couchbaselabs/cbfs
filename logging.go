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
		lw, err := syslog.New(syslog.LOG_INFO, "cbfs")
		if err != nil {
			corelog.Fatalf("Can't initialize logger: %v", err)
		}
		log = corelog.New(lw, "", 0)
	}
}
