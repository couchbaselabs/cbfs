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
	"github.com/dustin/httputil"
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

	if len(n) != len(nodes) {
		createDatabases(n)
	}

	nodes = n
}

func createDatabases(m map[string]cbfsclient.StorageNode) {
	for k := range m {
		du := *serieslyUrl
		du.Path = "/" + k

		req, err := http.NewRequest("PUT", du.String(), nil)
		if err != nil {
			log.Fatalf("Error creating DB %q: %v", k, err)
		}

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("Error issuing HTTP request:  %v", err)
			return
		}
		defer res.Body.Close()

		if res.StatusCode != 201 {
			log.Printf("Error creating db: %v", res.Status)
		}
	}
}

func updateNodesLoop() {
	updateNodes()
	for _ = range time.Tick(time.Minute) {
		updateNodes()
	}
}

func httpCopy(dest, src string) error {
	sres, err := http.Get(src)
	if err != nil {
		return err
	}
	defer sres.Body.Close()
	if sres.StatusCode != 200 {
		return httputil.HTTPErrorf(sres, "error copying from %v: S\n%B", src)
	}

	dres, err := http.Post(dest, sres.Header.Get("Content-Type"), sres.Body)
	if err != nil {
		return err
	}
	defer dres.Body.Close()

	if dres.StatusCode != 201 {
		return httputil.HTTPErrorf(dres, "Error posting result to %v: %S\n%B",
			dest)
	}
	return nil
}

func pollNode(name string, node cbfsclient.StorageNode, t time.Time) {
	du := *serieslyUrl
	du.RawQuery = "ts=" + strconv.FormatInt(t.UnixNano(), 10)
	du.Path = "/" + name

	if err := httpCopy(du.String(), node.URLFor("/.cbfs/debug/")); err != nil {
		log.Printf("Error copying data: %v", err)
	}
}

func poll(t time.Time) {
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

func initHttp() {
	http.DefaultClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:     true,
			ResponseHeaderTimeout: time.Millisecond * 100,
		},
	}
}

func main() {
	flag.Parse()

	initHttp()

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
