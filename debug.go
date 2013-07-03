package main

import (
	"expvar"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/samuel/go-metrics/metrics"
)

const minRecordRate = 4096

var (
	taskDurations = map[string]metrics.Histogram{}
	writeRate     = metrics.NewBiasedHistogram()
	readRate      = metrics.NewBiasedHistogram()
)

func init() {
	m := expvar.NewMap("io")

	m.Set("w_Bps", &metrics.HistogramExport{writeRate,
		[]float64{0.5, 0.9, 0.99, 0.999},
		[]string{"p50", "p90", "p99", "p999"}})
	m.Set("r_Bps", &metrics.HistogramExport{readRate,
		[]float64{0.5, 0.9, 0.99, 0.999},
		[]string{"p50", "p90", "p99", "p999"}})
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
		m.Set(k+"_ms", &metrics.HistogramExport{v,
			[]float64{0.5, 0.9, 0.99, 0.999},
			[]string{"p50", "p90", "p99", "p999"}})
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

type rateWriter struct {
	w             http.ResponseWriter
	bytesWritten  int64
	totalDuration time.Duration
}

func (r *rateWriter) Header() http.Header {
	return r.w.Header()
}

func (r *rateWriter) WriteHeader(i int) {
	r.w.WriteHeader(i)
}

func (r *rateWriter) Write(b []byte) (int, error) {
	t := time.Now()
	n, err := r.w.Write(b)
	r.bytesWritten += int64(n)
	r.totalDuration += time.Since(t)
	return n, err
}

func (r *rateWriter) ReadFrom(rr io.Reader) (int64, error) {
	t := time.Now()
	n, err := io.Copy(r.w, rr)
	r.bytesWritten += int64(n)
	r.totalDuration += time.Since(t)
	log.Printf("Completed ReadFrom: %v/%v", n, err)
	return n, err
}

func (r *rateWriter) recordRates() {
	if r.bytesWritten > minRecordRate {
		bps := float64(r.bytesWritten) / r.totalDuration.Seconds()
		writeRate.Update(int64(bps))
	}
}
