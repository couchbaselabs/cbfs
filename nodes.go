package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/couchbase/gomemcached"
	cb "github.com/couchbaselabs/go-couchbase"
)

// Objects larger than this won't use Frames.
const largishObject = 256 * 1024

var VERSION = "0.0.0"

var nodeTooOld = errors.New("Node information is too stale")
var notQueued = errors.New("Could not queue request")

type StorageNode struct {
	Addr      string    `json:"addr"`
	Type      string    `json:"type"`
	Started   time.Time `json:"started"`
	Time      time.Time `json:"time"`
	BindAddr  string    `json:"bindaddr"`
	FrameBind string    `json:"framebind"`
	Used      int64     `json:"used"`
	Free      int64     `json:"free"`
	Version   string    `json:"version"`

	name        string
	storageSize int64
}

func (s StorageNode) String() string {
	return fmt.Sprintf("{StorageNode %v/%v}", s.name, s.Addr)
}

func (a StorageNode) Address() string {
	if strings.HasPrefix(a.BindAddr, ":") {
		return a.Addr + a.BindAddr
	}
	return a.BindAddr
}

func (a StorageNode) FrameAddress() string {
	if strings.HasPrefix(a.FrameBind, ":") {
		return a.Addr + a.FrameBind
	}
	return a.FrameBind
}

func (a StorageNode) Client() *http.Client {
	addr := a.FrameAddress()
	if addr == "" {
		return http.DefaultClient
	}
	return getFrameClient(addr)
}

func (a StorageNode) ClientForTransfer(l int64) *http.Client {
	if l > largishObject {
		return http.DefaultClient
	}
	return a.Client()
}

func (a StorageNode) BlobURL(h string) string {
	return fmt.Sprintf("http://%s/.cbfs/blob/%s",
		a.Address(), h)
}

func (a StorageNode) fetchURL(h string) string {
	return fmt.Sprintf("http://%s/.cbfs/fetch/%s",
		a.Address(), h)
}

func (n StorageNode) IsDead() bool {
	// Get the freshest data.
	nn, err := findNode(n.name)
	if err == nil {
		return time.Now().Sub(nn.Time) > globalConfig.StaleNodeLimit
	}
	return false
}

func (n StorageNode) IsLocal() bool {
	return n.name == serverId
}

type NodeList []StorageNode

func (a NodeList) Len() int {
	return len(a)
}

func (a NodeList) Less(i, j int) bool {
	tdiff := a[i].Time.Sub(a[j].Time)
	if tdiff < 0 {
		tdiff = -tdiff
	}
	// Nodes that have heartbeated within a heartbeat time of each
	// other are sorted randomly.  This generally happens when
	// they're heartbeating regularly and we don't want to prefer
	// one over the other just because it happened to talk most
	// frequently.
	if tdiff < globalConfig.HeartbeatFreq {
		return rand.Intn(1) == 0
	}
	return a[i].Time.After(a[j].Time)
}

func (a NodeList) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Ask a node to acquire a blob.
func (n StorageNode) acquireBlob(oid, prevNode string) error {
	if n.IsLocal() {
		if !maybeQueueBlobFetch(oid, prevNode) {
			return notQueued
		}
	} else {
		req, err := http.NewRequest("GET", n.fetchURL(oid), nil)
		if err != nil {
			return err
		}

		req.Header.Set("X-Prevnode", prevNode)

		resp, err := n.Client().Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 202 {
			return fmt.Errorf("Error executing remote fetch: %v",
				resp.Status)
		}
	}
	return nil
}

// Ask a node to delete a blob.
func (n StorageNode) deleteBlob(oid string) error {
	if n.IsLocal() {
		return removeObject(oid)
	} else {
		req, err := http.NewRequest("DELETE", n.BlobURL(oid), nil)
		if err != nil {
			return err
		}
		resp, err := n.Client().Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		// 204 or 404 is considered successfully having the blob be
		// deleted.
		if resp.StatusCode != 204 && resp.StatusCode != 404 {
			return fmt.Errorf("Unexpected status %v deleting %v from %s",
				resp.Status, oid, n)
		}
	}
	log.Printf("Removed %v from %v", oid, n)
	return nil
}

// Iterate the list of blobs registered to this node and emit them
// into the given channel.
func (n StorageNode) iterateBlobs(ch chan<- string, cherr chan<- error,
	quit <-chan bool) {

	defer close(ch)
	if cherr != nil {
		defer close(cherr)
	}

	viewRes := struct {
		Rows []struct {
			Id string
		}
		Errors []cb.ViewError
	}{}

	startDocId := ""
	done := false
	limit := 1000
	for !done {
		params := map[string]interface{}{
			"key":    n.name,
			"reduce": false,
			"limit":  limit,
		}
		if startDocId != "" {
			params["startkey_docid"] = cb.DocID(startDocId)
		}
		err := couchbase.ViewCustom("cbfs", "node_blobs", params,
			&viewRes)
		if err != nil {
			cherr <- err
			return
		}
		for _, e := range viewRes.Errors {
			cherr <- e
		}

		done = len(viewRes.Rows) < limit

		for _, r := range viewRes.Rows {
			startDocId = r.Id
			select {
			case <-quit:
				return
			case ch <- startDocId[1:]:
				// We sent one.
			}
		}
	}
}

func findAllNodes() (NodeList, error) {
	nodeReg, err := retrieveNodeRegistry()
	if err != nil {
		return NodeList{}, err
	}

	nodeSizes := nodeReg.Nodes
	nodeKeys := []string{}
	for k := range nodeSizes {
		nodeKeys = append(nodeKeys, "/"+k)
	}

	rv := make(NodeList, 0, len(nodeSizes))

	bres, _, err := couchbase.GetBulk(nodeKeys)
	if err != nil {
		return nil, err
	}
	for nid, mcresp := range bres {
		if mcresp.Status != gomemcached.SUCCESS {
			log.Printf("Error fetching %v: %v", nid, mcresp)
			continue
		}

		node := StorageNode{}
		err = json.Unmarshal(mcresp.Body, &node)
		if err != nil {
			log.Printf("Error unmarshalling storage node %v: %v",
				nid, err)
			continue
		}

		node.name = nid[1:]
		node.storageSize = int64(nodeSizes[node.name])

		rv = append(rv, node)
	}

	sort.Sort(rv)

	return rv, nil
}

func findNode(name string) (StorageNode, error) {
	sn := StorageNode{}
	err := couchbase.Get("/"+name, &sn)
	return sn, err
}

func updateNodeSizes() error {
	viewRes := struct {
		Rows []struct {
			Key   string
			Value float64
		}
	}{}

	err := couchbase.ViewCustom("cbfs", "node_size",
		map[string]interface{}{
			"group_level": 1,
		}, &viewRes)
	if err != nil {
		return err
	}

	for _, r := range viewRes.Rows {
		err = setInNodeRegistry(r.Key, int64(r.Value))
		if err != nil {
			return err
		}
	}
	return nil
}

func findNodeMap() (map[string]StorageNode, error) {
	rv := map[string]StorageNode{}
	nl, err := findAllNodes()
	if err != nil {
		return rv, err
	}
	for _, n := range nl {
		rv[n.name] = n
	}
	return rv, nil
}

func (nl NodeList) minusLocal() NodeList {
	rv := make(NodeList, 0, len(nl))
	for _, n := range nl {
		if !n.IsLocal() {
			rv = append(rv, n)
		}
	}
	return rv
}

func (nl NodeList) minus(other NodeList) NodeList {
	rv := make(NodeList, 0, len(nl))
	for _, n := range nl {
		found := false
		for _, o := range other {
			if o.name == n.name {
				found = true
			}
		}
		if !found {
			rv = append(rv, n)
		}
	}
	return rv
}

func (nl NodeList) named(name string) StorageNode {
	for _, sn := range nl {
		if sn.name == name {
			return sn
		}
	}
	return StorageNode{}
}

// Find a node with at least this many bytes free.
func (nl NodeList) withAtLeast(free int64) NodeList {
	rv := NodeList{}
	for _, node := range nl {
		if node.Free > free {
			rv = append(rv, node)
		}
	}
	return rv
}

// Find nodes with no more than this much space free.
func (nl NodeList) withNoMoreThan(free int64) NodeList {
	rv := NodeList{}
	for _, node := range nl {
		if node.Free <= free {
			rv = append(rv, node)
		}
	}
	return rv
}

func (nl NodeList) candidatesFor(oid string, exclude NodeList) NodeList {
	// Find the owners of this blob
	ownership := BlobOwnership{}
	oidkey := "/" + oid
	err := couchbase.Get(oidkey, &ownership)
	if err != nil {
		log.Printf("Missing ownership record for OID: %v", oid)
		return nl
	}

	owners := ownership.ResolveNodes()

	// Find a good destination candidate.
	return nl.minus(owners).withAtLeast(ownership.Length)
}

func (nl NodeList) BlobURLs(h string) []string {
	rv := make([]string, 0, len(h))
	for _, n := range nl {
		rv = append(rv, n.BlobURL(h))
	}
	return rv
}

func findRemoteNodes() (NodeList, error) {
	allNodes, err := findAllNodes()
	if err != nil {
		return allNodes, err
	}
	return allNodes.minusLocal(), nil
}
