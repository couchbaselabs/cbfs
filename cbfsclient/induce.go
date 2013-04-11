package main

import (
	"log"
	"net/http"
	"net/url"
)

func induceCommand(ustr string, args []string) {
	u, err := url.Parse(ustr)
	if err != nil {
		log.Fatalf("Error parsing URL: %v", err)
	}

	u.Path = "/.cbfs/tasks/" + args[0]

	res, err := http.PostForm(u.String(), nil)
	if err != nil {
		log.Fatalf("Error inducing %v: %v", args[0], err)
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		log.Fatalf("Error inducing %v: %v", args[0], res.Status)
	}
}
