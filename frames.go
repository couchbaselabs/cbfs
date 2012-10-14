package main

import (
	"flag"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/dustin/frames"
	"github.com/dustin/frames/http"
)

const (
	frameConnectTimeout = time.Second * 5
	frameCheckFreq      = time.Second * 5
	frameMaxIdle        = time.Minute * 5
)

var framesBind = flag.String("frameBind", ":8423",
	"Binding for frames protocol.")

type frameClient struct {
	client   *http.Client
	checker  *time.Timer
	lastUsed time.Time
}

var frameClients = map[string]*frameClient{}
var frameClientsLock sync.Mutex

func findExistingFrameClient(addr string) *frameClient {
	frameClientsLock.Lock()
	defer frameClientsLock.Unlock()
	return frameClients[addr]
}

func destroyFrameClient(addr string) {
	frameClientsLock.Lock()
	defer frameClientsLock.Unlock()
	fc := frameClients[addr]
	if fc == nil {
		return
	}
	fc.checker.Stop()
	framesweb.CloseFramesClient(fc.client)
	delete(frameClients, addr)
}

func checkFrameClient(addr string) {
	fc := findExistingFrameClient(addr)
	if fc == nil {
		return
	}
	if time.Since(fc.lastUsed) > frameMaxIdle {
		log.Printf("Client was last used %v (%v), closing",
			fc.lastUsed, time.Since(fc.lastUsed))
		destroyFrameClient(addr)
		return
	}
	res, err := fc.client.Get("http://" + addr + "/.cbfs/ping/")
	if err != nil || res.StatusCode != 204 {
		status := "<none>"
		if err == nil {
			status = res.Status
		}
		log.Printf("Found error checking frame client, killing: %v/%v",
			err, status)
		destroyFrameClient(addr)
		return
	}
	fc.checker = time.AfterFunc(frameCheckFreq, func() {
		checkFrameClient(addr)
	})
}

func connectNewFramesClient(addr string) *frameClient {
	c, err := net.DialTimeout("tcp", addr, frameConnectTimeout)
	if err != nil {
		log.Printf("Error connecting to %v: %v", addr, err)
		return nil
	}
	frt := &framesweb.FramesRoundTripper{
		Dialer: frames.NewClient(c),
	}
	hc := &http.Client{Transport: frt}
	frameClientsLock.Lock()
	defer frameClientsLock.Unlock()

	fc := &frameClient{
		client: hc,
		checker: time.AfterFunc(frameCheckFreq, func() {
			checkFrameClient(addr)
		}),
	}
	frameClients[addr] = fc
	return fc
}

func getFrameClient(addr string) *http.Client {
	fc := findExistingFrameClient(addr)
	if fc == nil {
		fc = connectNewFramesClient(addr)
	}
	if fc == nil {
		log.Printf("Failed to find or get frames client for %v", addr)
		return http.DefaultClient
	} else {
		fc.lastUsed = time.Now()
	}
	return fc.client
}

func serveFrame() {
	if *framesBind == "" {
		return
	}

	l, err := net.Listen("tcp4", *framesBind)
	if err != nil {
		log.Fatalf("Error setting up frames listener.")
	}

	ll, err := frames.ListenerListener(l)
	if err != nil {
		log.Fatalf("Error listen listening: %v", err)
	}

	s := &http.Server{
		Handler: http.HandlerFunc(httpHandler),
	}

	log.Fatal(s.Serve(ll))
}
