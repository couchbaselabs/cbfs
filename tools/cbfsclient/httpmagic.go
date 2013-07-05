package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const SIGINFO = syscall.Signal(29)

type httpTracker struct {
	http.RoundTripper
	sync.Mutex
	inflight map[string]time.Time
}

func (t *httpTracker) register(u string) {
	t.Lock()
	defer t.Unlock()
	t.inflight[u] = time.Now()
}

func (t *httpTracker) unregister(u string) {
	t.Lock()
	defer t.Unlock()
	delete(t.inflight, u)
}

func (t *httpTracker) reportOnce() {
	t.Lock()
	defer t.Unlock()

	log.Printf("In-flight HTTP requests:")
	for k, t := range t.inflight {
		log.Printf("  servicing %q for %v", k, time.Since(t))
	}
}

func (t *httpTracker) report(ch <-chan os.Signal) {
	for _ = range ch {
		t.reportOnce()
	}
}

type trackFinalizer struct {
	b io.ReadCloser
	t *httpTracker
	u string
}

func (d *trackFinalizer) Close() error {
	d.t.unregister(d.u)
	return d.b.Close()
}

func (d *trackFinalizer) Read(b []byte) (int, error) {
	return d.b.Read(b)
}

func (d *trackFinalizer) WriteTo(w io.Writer) (n int64, err error) {
	return io.Copy(w, d.b)
}

func (t *httpTracker) RoundTrip(req *http.Request) (*http.Response, error) {
	u := req.URL.String()
	t.register(u)
	res, err := t.RoundTrip(req)
	if err == nil {
		res.Body = &trackFinalizer{res.Body, t, u}
	} else {
		t.unregister(u)
	}
	return res, err
}

func initHttpMagic() {
	http.DefaultTransport = &httpTracker{
		RoundTripper: http.DefaultTransport,
		inflight:     map[string]time.Time{},
	}

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, SIGINFO)

	go http.DefaultTransport.(*httpTracker).report(sigch)
}
