package main

import (
	"expvar"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	cb "github.com/couchbaselabs/go-couchbase"
	_ "github.com/couchbase/gomemcached/debug"
	"github.com/samuel/go-metrics/metrics"
)

const minRecordRate = 4096

var (
	taskDurations = map[string]metrics.Histogram{}
	writeBytes    = metrics.NewBiasedHistogram()
	readBytes     = metrics.NewBiasedHistogram()

	cbHistos   = map[string]metrics.Histogram{}
	cbHistosMu = sync.Mutex{}

	expHistos *expvar.Map
)

func init() {
	m := expvar.NewMap("io")

	m.Set("w_B", &metrics.HistogramExport{
		Histogram:       writeBytes,
		Percentiles:     []float64{0.1, 0.2, 0.80, 0.90, 0.99},
		PercentileNames: []string{"p10", "p20", "p80", "p90", "p99"}})
	m.Set("r_B", &metrics.HistogramExport{
		Histogram:       readBytes,
		Percentiles:     []float64{0.1, 0.2, 0.80, 0.90, 0.99},
		PercentileNames: []string{"p10", "p20", "p80", "p90", "p99"}})

	expHistos = expvar.NewMap("cb")

	cb.ConnPoolCallback = recordConnPoolStat
}

func connPoolHisto(name string) metrics.Histogram {
	cbHistosMu.Lock()
	defer cbHistosMu.Unlock()
	rv, ok := cbHistos[name]
	if !ok {
		rv = metrics.NewBiasedHistogram()
		cbHistos[name] = rv

		expHistos.Set(name, &metrics.HistogramExport{
			Histogram:       rv,
			Percentiles:     []float64{0.25, 0.5, 0.75, 0.90, 0.99},
			PercentileNames: []string{"p25", "p50", "p75", "p90", "p99"}})
	}
	return rv
}

func recordConnPoolStat(host string, source string, start time.Time, err error) {
	duration := time.Since(start)
	histo := connPoolHisto(host)
	histo.Update(int64(duration))
}

func initTaskMetrics() {
	m := expvar.NewMap("tasks")

	for k := range globalPeriodicJobRecipes {
		taskDurations[k] = metrics.NewBiasedHistogram()
	}
	for k := range localPeriodicJobRecipes {
		taskDurations[k] = metrics.NewBiasedHistogram()
	}

	for k, v := range taskDurations {
		m.Set(k+"_ms", &metrics.HistogramExport{
			Histogram:       v,
			Percentiles:     []float64{0.5, 0.9, 0.99, 0.999},
			PercentileNames: []string{"p50", "p90", "p99", "p999"}})
	}
}

func doDebug(w http.ResponseWriter, req *http.Request) {
	req.URL.Path = strings.Replace(req.URL.Path, debugPrefix, "/debug/vars", 1)
	http.DefaultServeMux.ServeHTTP(w, req)
}

func shortTaskName(n string) string {
	if strings.HasPrefix(n, serverId+"/") {
		n = n[len(serverId)+1:]
	}
	return n
}

func endedTask(named string, t time.Time) {
	taskDurations[shortTaskName(named)].Update(
		int64(time.Since(t) / time.Millisecond))
}

type rateConn struct {
	c net.Conn
}

func (r *rateConn) WriteTo(w io.Writer) (int64, error) {
	n, err := io.Copy(w, r.c)
	readBytes.Update(n)
	return n, err
}

func (r *rateConn) Write(b []byte) (n int, err error) {
	n, err = r.c.Write(b)
	writeBytes.Update(int64(n))
	return
}

func (r *rateConn) ReadFrom(rr io.Reader) (int64, error) {
	n, err := io.Copy(r.c, rr)
	writeBytes.Update(n)
	return n, err
}

func (r *rateConn) Read(b []byte) (n int, err error) {
	n, err = r.c.Read(b)
	readBytes.Update(int64(n))
	return
}

func (r *rateConn) Close() error {
	return r.c.Close()
}

func (r *rateConn) LocalAddr() net.Addr {
	return r.c.LocalAddr()
}

func (r *rateConn) RemoteAddr() net.Addr {
	return r.c.RemoteAddr()
}

func (r *rateConn) SetDeadline(t time.Time) error {
	return r.c.SetDeadline(t)
}

func (r *rateConn) SetReadDeadline(t time.Time) error {
	return r.c.SetReadDeadline(t)
}

func (r *rateConn) SetWriteDeadline(t time.Time) error {
	return r.c.SetWriteDeadline(t)
}

type rateListener struct {
	l net.Listener
}

func (r *rateListener) Accept() (net.Conn, error) {
	c, err := r.l.Accept()
	return &rateConn{c: c}, err
}

func (r *rateListener) Close() error {
	return r.l.Close()
}

func (r *rateListener) Addr() net.Addr {
	return r.l.Addr()
}

func rateListen(nettype, laddr string) (net.Listener, error) {
	l, err := net.Listen(nettype, laddr)
	return &rateListener{l: l}, err
}
