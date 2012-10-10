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
		l, err := syslog.NewLogger(syslog.LOG_INFO, 0)
		if err != nil {
			corelog.Fatalf("Can't initialize logger: %v", err)
		}
		log = l
	}
}
