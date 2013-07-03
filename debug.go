package main

import (
	"expvar"
	"net/http"
	"strings"
	"time"

	"github.com/samuel/go-metrics/metrics"
)

var taskDurations = map[string]metrics.Histogram{}

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
