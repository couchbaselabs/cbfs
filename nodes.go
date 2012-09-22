package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/dustin/gomemcached"
)

var nodeTooOld = errors.New("Node information is too stale")

type StorageNode struct {
	Addr     string    `json:"addr"`
	Type     string    `json:"type"`
	Time     time.Time `json:"time"`
	BindAddr string    `json:"bindaddr"`
	Hash     string    `json:"hash"`
	Used     int64     `json:"used"`
	Free     uint64    `json:"free"`

	name        string
	storageSize int64
}

func (a StorageNode) Address() string {
	if strings.HasPrefix(a.BindAddr, ":") {
		return a.Addr + a.BindAddr
	}
	return a.BindAddr
}

func (a StorageNode) BlobURL(h string) string {
	return fmt.Sprintf("http://%s/.cbfs/blob/%s",
		a.Address(), h)
}

func (a StorageNode) fetchURL(h string) string {
	return fmt.Sprintf("http://%s/.cbfs/fetch/%s",
		a.Address(), h)
}

func (n StorageNode) IsLocal() bool {
	return n.name == serverId
}

type NodeList []StorageNode

func (a NodeList) Len() int {
	return len(a)
}

func (a NodeList) Less(i, j int) bool {
	return a[i].Time.After(a[j].Time)
}

func (a NodeList) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Ask a node to acquire a blob.
func (n StorageNode) acquireBlob(oid string) error {
	resp, err := http.Get(n.fetchURL(oid))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		return fmt.Errorf("Error executing remote fetch: %v",
			resp.Status)
	}
	return nil
}

// Ask a node to delete a blob.
func (n StorageNode) deleteBlob(oid string) error {
	req, err := http.NewRequest("DELETE", n.BlobURL(oid), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)

	// 204 or 404 is considered successfully having the blob be
	// deleted.
	if resp.StatusCode != 204 && resp.StatusCode != 404 {
		return fmt.Errorf("Unexpected status %v deleting %v from %s",
			resp.Status, oid, n)
	}
	return nil
}

func findAllNodes() (NodeList, error) {
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
		return NodeList{}, err
	}

	nodeSizes := map[string]float64{}
	nodeKeys := []string{}
	for _, r := range viewRes.Rows {
		nodeSizes[r.Key] = r.Value
		nodeKeys = append(nodeKeys, "/"+r.Key)
	}

	rv := make(NodeList, 0, len(viewRes.Rows))

	for nid, mcresp := range couchbase.GetBulk(nodeKeys) {
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

func findRemoteNodes() (NodeList, error) {
	allNodes, err := findAllNodes()
	if err != nil {
		return allNodes, err
	}
	return allNodes.minusLocal(), nil
}
