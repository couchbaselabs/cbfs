package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

type NodeList []StorageNode

func (a NodeList) Len() int {
	return len(a)
}

func (a NodeList) Less(i, j int) bool {
	return a[i].Time.Before(a[j].Time)
}

func (a NodeList) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
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

func findRemoteNodes() (NodeList, error) {
	allNodes, err := findAllNodes()
	if err != nil {
		return allNodes, err
	}
	remoteNodes := make(NodeList, 0, len(allNodes))
	for _, n := range allNodes {
		if n.name != serverId {
			remoteNodes = append(remoteNodes, n)
		}
	}
	return remoteNodes, nil
}
