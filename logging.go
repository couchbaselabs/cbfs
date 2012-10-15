// +build !windows

package main

import (
	"log"
	"log/syslog"
)

func initLogger(slog bool) {
	if slog {
		lw, err := syslog.New(syslog.LOG_INFO, "cbfs")
		if err != nil {
			log.Fatalf("Can't initialize syslog: %v", err)
		}
		log.SetOutput(lw)
	}
}
