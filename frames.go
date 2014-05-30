package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/dustin/frames"
	"github.com/dustin/frames/http"
	"github.com/dustin/httputil"
)

const (
	frameConnectTimeout = time.Second * 5
	frameCheckFreq      = time.Second * 5
	frameMaxIdle        = time.Minute * 5
	minFrameRead        = 180
	minFrameWritten     = 120
)

var framesBind = flag.String("frameBind", ":8423",
	"Binding for frames protocol.")

type frameClient struct {
	conn         frames.ChannelDialer
	client       *http.Client
	checker      *time.Timer
	prevInfo     frames.Info
	lastActivity time.Time
}

var frameClients = map[string]*frameClient{}
var frameClientsLock sync.Mutex

func getFramesInfos() map[string]frames.Info {
	rv := map[string]frames.Info{}
	frameClientsLock.Lock()
	defer frameClientsLock.Unlock()

	for k, v := range frameClients {
		rv[k] = v.conn.GetInfo()
	}

	return rv
}

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
	err := framesweb.CloseFramesClient(fc.client)
	if err != nil {
		log.Printf("Error closing %v frame client: %v", addr, err)
	}
	delete(frameClients, addr)
}

func checkFrameClient(addr string) {
	fc := findExistingFrameClient(addr)
	if fc == nil {
		return
	}
	info := fc.conn.GetInfo()
	sufficient := false

	if (info.BytesRead-fc.prevInfo.BytesRead > minFrameRead) ||
		(info.BytesWritten-fc.prevInfo.BytesWritten > minFrameWritten) {
		fc.lastActivity = time.Now()
		sufficient = true
	}

	if time.Since(fc.lastActivity) > frameMaxIdle {
		log.Printf("Too long with insufficient activity on %v, shutting down",
			addr)
		destroyFrameClient(addr)
		return
	}

	// If we're not naturally moving enough data, send a ping.
	if !sufficient {
		ch := make(chan error)
		go func() {
			res, err := fc.client.Get("http://" +
				addr + "/.cbfs/ping/")
			if err == nil {
				res.Body.Close()
				if res.StatusCode != 204 {
					err = httputil.HTTPError(res)
				}
			}
			ch <- err
		}()

		var err error
		select {
		case err = <-ch:
		case <-time.After(time.Minute):
			err = errors.New("ping timeout")
		}

		if err != nil {
			log.Printf("Ping error on %v: %v", addr, err)
			destroyFrameClient(addr)
			return
		}
	}

	fc.prevInfo = info
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
	conn := frames.NewClient(c)
	frt := &framesweb.FramesRoundTripper{
		Dialer:  conn,
		Timeout: time.Second * 5,
	}
	hc := &http.Client{Transport: frt}
	frameClientsLock.Lock()
	defer frameClientsLock.Unlock()

	fwc := &frameClient{
		conn:   conn,
		client: hc,
		checker: time.AfterFunc(frameCheckFreq, func() {
			checkFrameClient(addr)
		}),
	}
	frameClients[addr] = fwc
	return fwc
}

func getFrameClient(addr string) *http.Client {
	fc := findExistingFrameClient(addr)
	if fc == nil {
		fc = connectNewFramesClient(addr)
	}
	if fc == nil {
		log.Printf("Failed to find or get frames client for %v", addr)
		return http.DefaultClient
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
