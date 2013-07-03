package main

import (
	"flag"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/couchbaselabs/cbfs/client"
)

var cbfsUrlFlag = flag.String("cbfs", "http://cbfs:8484/", "URL to cbfs base")
var serieslyUrlFlag = flag.String("seriesly", "http://seriesly:3133/",
	"URL to seriesly base")
var pollFreq = flag.Duration("freq", 5*time.Second, "How often to poll cbfs")

var cbfsUrl, serieslyUrl *url.URL
var client *cbfsclient.Client

var nodeLock sync.Mutex
var nodes map[string]cbfsclient.StorageNode

func updateNodes() {
	nodeLock.Lock()
	defer nodeLock.Unlock()

	n, err := client.Nodes()
	lf := log.Printf
	if nodes == nil {
		lf = log.Fatalf
	}
	if err != nil {
		lf("Couldn't update/init nodes: %v", err)
		return
	}

	nodes = n
}

func updateNodesLoop() {
	updateNodes()
	for _ = range time.Tick(time.Minute) {
		updateNodes()
	}
}

func pollNode(name string, node cbfsclient.StorageNode, t time.Time) {
	log.Printf("Polling %v / %#v", name, node)

	sres, err := http.Get(node.URLFor("/.cbfs/debug/"))
	if err != nil {
		log.Printf("Error getting data from %v: %v", name, err)
		return
	}
	defer sres.Body.Close()
	if sres.StatusCode != 200 {
		log.Printf("HTTP error getting debug data: %v", sres.Status)
		return
	}

	du := *serieslyUrl
	du.RawQuery = "ts=" + strconv.FormatInt(t.UnixNano(), 10)

	du.Path = "/" + name
	dres, err := http.Post(du.String(), sres.Header.Get("Content-Type"), sres.Body)
	if err != nil {
		log.Printf("Error posting stats: %v", err)
	}
	defer dres.Body.Close()

	if dres.StatusCode != 204 {
		log.Printf("HTTP Error posting result to %v: %v", du.String(), dres.StatusCode)
	}
}

func poll(t time.Time) {
	log.Printf("Polling...")
	for k, v := range nodes {
		go pollNode(k, v, t)
	}
}

func mustParseUrl(ustr string) *url.URL {
	u, e := url.Parse(ustr)
	if e != nil {
		log.Fatalf("Error parsing URL %q: %v", ustr, e)
	}
	return u
}

func parseUrls() {
	cbfsUrl = mustParseUrl(*cbfsUrlFlag)
	serieslyUrl = mustParseUrl(*serieslyUrlFlag)
}

func main() {
	flag.Parse()

	parseUrls()

	var err error
	client, err = cbfsclient.New(cbfsUrl.String())
	if err != nil {
		log.Fatalf("Can't instantiate cbfsclient: %v", err)
	}

	go updateNodesLoop()

	for t := range time.Tick(*pollFreq) {
		poll(t)
	}
}
