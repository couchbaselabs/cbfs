package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/couchbaselabs/cbfs/config"
	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

func doGetConfig(c *Container, w http.ResponseWriter, req *http.Request) {
	err := updateConfig()
	if err != nil && !gomemcached.IsNotFound(err) {
		log.Printf("Error updating config: %v", err)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(200)

	e := json.NewEncoder(w)
	err = e.Encode(&globalConfig)
	if err != nil {
		log.Printf("Error sending config: %v", err)
	}
}

func putConfig(c *Container, w http.ResponseWriter, req *http.Request) {
	d := json.NewDecoder(req.Body)
	conf := cbfsconfig.CBFSConfig{}

	err := d.Decode(&conf)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error reading config: %v", err)
		return
	}

	err = StoreConfig(conf)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error writing config: %v", err)
		return
	}

	err = updateConfig()
	if err != nil {
		log.Printf("Error fetching newly stored config: %v", err)
	}

	w.WriteHeader(204)
}

func doBlobInfo(c *Container, w http.ResponseWriter, req *http.Request) {
	if err := req.ParseForm(); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	blobs, err := getBlobs(req.Form["blob"])
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	res := map[string]interface{}{}
	for k, v := range blobs {
		res[k] = struct {
			Nodes map[string]time.Time `json:"nodes"`
		}{v.Nodes}
	}

	sendJson(w, req, res)
}

func doList(c *Container, w http.ResponseWriter, req *http.Request) {
	if canGzip(req) {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		w = &geezyWriter{w, gz}
	}

	w.WriteHeader(200)
	explen := getHash().Size() * 2
	filepath.Walk(*root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && !strings.HasPrefix(info.Name(), "tmp") &&
			len(info.Name()) == explen {
			_, e := w.Write([]byte(info.Name() + "\n"))
			return e
		}
		return nil
	})
}

func doListTaskInfo(c *Container, w http.ResponseWriter, req *http.Request) {
	res := struct {
		Global map[string][]string `json:"global"`
		Local  map[string][]string `json:"local"`
	}{make(map[string][]string), make(map[string][]string)}

	for k, v := range globalPeriodicJobRecipes {
		res.Global[k] = v.excl
	}
	for k, v := range localPeriodicJobRecipes {
		res.Local[k] = v.excl
	}

	sendJson(w, req, res)
}

func doListTasks(c *Container, w http.ResponseWriter, req *http.Request) {
	tasks, err := listRunningTasks()
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error listing tasks:  %v", err)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(200)

	// Reformat for more APIish output.
	output := map[string]map[string]TaskState{}

	for _, tl := range tasks {
		// Remove node prefix from local task names.
		npre := tl.Node + "/"

		for k, v := range tl.Tasks {
			if strings.HasPrefix(k, npre) {
				delete(tl.Tasks, k)
				tl.Tasks[k[len(npre):]] = v
			}
		}
		output[tl.Node] = tl.Tasks
	}

	e := json.NewEncoder(w)
	err = e.Encode(output)
	if err != nil {
		log.Printf("Error encoding running tasks list: %v", err)
	}
}

func doFileInfo(c *Container, w http.ResponseWriter, req *http.Request, fn string) {
	fm := fileMeta{}
	err := couchbase.Get(c.shortName(fn), &fm)
	switch {
	case err == nil:
	case gomemcached.IsNotFound(err):
		http.Error(w, "not found", 404)
		return
	default:
		http.Error(w, err.Error(), 500)
		return
	}
	sendJson(w, req, map[string]interface{}{
		"path": fn,
		"meta": fm,
	})
}

func doGetMeta(c *Container, w http.ResponseWriter, req *http.Request, path string) {
	got := fileMeta{}
	err := couchbase.Get(c.shortName(path), &got)
	if err != nil {
		log.Printf("Error getting file %#v: %v", path, err)
		http.Error(w, err.Error(), 404)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(200)
	if got.Userdata == nil {
		w.Write([]byte("{}"))
	} else {
		w.Write(*got.Userdata)
	}
}

func putMeta(c *Container, w http.ResponseWriter, req *http.Request, path string) {
	got := fileMeta{}
	casid := uint64(0)
	k := c.shortName(path)
	err := couchbase.Gets(k, &got, &casid)
	if err != nil {
		log.Printf("Error getting file %#v: %v", path, err)
		http.Error(w, err.Error(), 404)
		return
	}

	r := json.RawMessage{}
	err = json.NewDecoder(req.Body).Decode(&r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	got.Userdata = &r
	b := mustEncode(&got)

	err = couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		req := &gomemcached.MCRequest{
			Opcode:  gomemcached.SET,
			VBucket: vb,
			Key:     []byte(path),
			Cas:     casid,
			Opaque:  0,
			Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
			Body:    b}
		_, err := mc.Send(req)
		return err
	})

	if err == nil {
		w.WriteHeader(201)
	} else {
		http.Error(w, err.Error(), 500)
	}
}

func doListNodes(c *Container, w http.ResponseWriter, req *http.Request) {
	nl, err := findAllNodes()
	if err != nil {
		log.Printf("Error executing nodes view: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error generating node list: %v", err)
		return
	}

	respob := map[string]map[string]interface{}{}
	for _, node := range nl {
		age := time.Since(node.Time)
		respob[node.name] = map[string]interface{}{
			"size":       node.storageSize,
			"addr":       node.Address(),
			"starttime":  node.Started,
			"hbtime":     node.Time,
			"hbage_ms":   age.Nanoseconds() / 1e6,
			"hbage_str":  age.String(),
			"used":       node.Used,
			"free":       node.Free,
			"addr_raw":   node.Addr,
			"bindaddr":   node.BindAddr,
			"framesbind": node.FrameBind,
			"version":    node.Version,
		}
		// Grandfathering these in.
		if !node.Started.IsZero() {
			uptime := time.Since(node.Started)
			respob[node.name]["uptime_ms"] = uptime.Nanoseconds() / 1e6
			respob[node.name]["uptime_str"] = uptime.String()
		}

	}

	sendJson(w, req, respob)
}

func doGetFramesData(c *Container, w http.ResponseWriter, req *http.Request) {
	sendJson(w, req, getFramesInfos())
}

func proxyViewRequest(c *Container, w http.ResponseWriter, req *http.Request,
	path string) {

	nodes := couchbase.Nodes()
	node := nodes[rand.Intn(len(nodes))]
	u, err := url.Parse(node.CouchAPIBase)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		return
	}

	u.Path = "/" + path
	u.RawQuery = req.URL.RawQuery

	client := &http.Client{
		Transport: TimeoutTransport(*viewTimeout),
	}

	res, err := client.Get(u.String())
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	defer res.Body.Close()

	for k, vs := range res.Header {
		w.Header()[k] = vs
	}

	output := io.Writer(w)

	if canGzip(req) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Del("Content-Length")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		output = gz
	}
	w.WriteHeader(res.StatusCode)

	io.Copy(output, res.Body)
}

func proxyCRUDGet(c *Container, w http.ResponseWriter, req *http.Request,
	path string) {

	val, err := couchbase.GetRaw(c.shortName(path))
	if err != nil {
		w.WriteHeader(404)
		fmt.Fprintf(w, "Error getting value: %v", err)
		return
	}
	w.WriteHeader(200)
	w.Write(val)
}

func proxyCRUDPut(c *Container, w http.ResponseWriter, req *http.Request,
	path string) {

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error reading data: %v", err)
		return
	}

	err = couchbase.SetRaw(c.shortName(path), 0, data)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error storing value: %v", err)
		return
	}

	w.WriteHeader(204)
}

func proxyCRUDDelete(c *Container, w http.ResponseWriter, req *http.Request,
	path string) {

	err := couchbase.Delete(c.shortName(path))
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error deleting value: %v", err)
		return
	}

	w.WriteHeader(204)
}

func doListDocs(c *Container, w http.ResponseWriter, req *http.Request,
	path string) {

	// trim off trailing slash early so we handle them consistently
	if strings.HasSuffix(path, "/") {
		path = path[0 : len(path)-1]
	}

	includeMeta := req.FormValue("includeMeta")
	depthString := req.FormValue("depth")
	depth := 1
	if depthString != "" {
		i, err := strconv.Atoi(depthString)
		if err != nil {
			w.WriteHeader(400)
			fmt.Fprintf(w, "Error processing depth parameter: %v", err)
			return
		}
		depth = i
	}

	fl, err := c.listFiles(path, includeMeta == "true", depth)
	if err != nil {
		log.Printf("Error executing file browse view: %v", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error generating file list: %v", err)
		return
	}

	if len(fl.Dirs) == 0 && len(fl.Files) == 0 {
		w.WriteHeader(404)
		return
	}

	if canGzip(req) {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		w = &geezyWriter{w, gz}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(200)

	e := json.NewEncoder(w)
	err = e.Encode(fl)
	if err != nil {
		log.Printf("Error writing json stream: %v", err)
	}
}

func doPing(c *Container, w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(204)
}

func doInduceTask(c *Container, w http.ResponseWriter, req *http.Request,
	taskName string) {

	err := induceTask(taskName)
	switch err {
	case noSuchTask:
		http.Error(w, fmt.Sprintf("No such task: %q", taskName), 404)
	case taskAlreadyQueued, nil:
		w.WriteHeader(202)
	default:
		http.Error(w, err.Error(), 500)
	}
}
