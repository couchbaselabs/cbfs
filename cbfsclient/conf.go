package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/couchbaselabs/cbfs/config"
)

func getConfURL(uin string) string {
	u, err := url.Parse(uin)
	if err != nil {
		log.Fatalf("Error parsing URL: %v", err)
	}

	u.Path = "/.cbfs/config/"
	return u.String()
}

func getConfig(u string) cbfsconfig.CBFSConfig {
	conf := cbfsconfig.CBFSConfig{}

	res, err := http.Get(getConfURL(u))
	if err != nil {
		log.Fatalf("Error making HTTP connection: %v", err)
	}
	defer res.Body.Close()
	d := json.NewDecoder(res.Body)
	err = d.Decode(&conf)
	if err != nil {
		log.Fatalf("Error parsing response: %v", err)
	}
	return conf
}

func getConfCommand(u string, args []string) {
	getConfig(u).Dump(os.Stdout)
}

func setConfCommand(u string, args []string) {
	conf := getConfig(u)

	key, val := args[0], args[1]

	err := conf.SetParameter(key, val)
	if err != nil {
		log.Fatalf("Unhandled property: %v (try running getconf)",
			key)
	}

	data, err := json.Marshal(&conf)
	if err != nil {
		log.Fatalf("Can't marshal config: %v", err)
	}

	req, err := http.NewRequest("PUT", getConfURL(u),
		bytes.NewBuffer(data))
	if err != nil {
		log.Fatalf("Can't build request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Can't establish http connection: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != 204 {
		log.Printf("HTTP error:  %v", res.Status)
		io.Copy(os.Stderr, res.Body)
		os.Exit(1)
	}
}
