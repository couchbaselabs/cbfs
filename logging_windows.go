package main

import (
	"log"
)

func initLogger(slog bool) {
	if slog {
		log.Printf("No syslog support on Windows, using regular logging")
	}
}
