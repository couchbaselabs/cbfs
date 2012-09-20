package main

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

var nodeTooOld = errors.New("Node information is too stale")

type StorageNode struct {
	Addr     string    `json:"addr"`
	Type     string    `json:"type"`
	Time     time.Time `json:"time"`
	BindAddr string    `json:"bindaddr"`
	Hash     string    `json:"hash"`

	name string
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

func findAllNodes() NodeList {
	viewRes := struct {
		Rows []struct {
			ID  string
			Doc struct {
				Json StorageNode
			}
		}
	}{}

	rv := make(NodeList, 0, 16)
	err := couchbase.ViewCustom("cbfs", "nodes",
		map[string]interface{}{
			"include_docs": true,
			"descending":   true,
		}, &viewRes)
	if err != nil {
		log.Printf("Error executing nodes view: %v", err)
		return NodeList{}
	}
	for _, r := range viewRes.Rows {
		r.Doc.Json.name = r.ID[1:]
		rv = append(rv, r.Doc.Json)
	}

	return rv
}

func findRemoteNodes() NodeList {
	allNodes := findAllNodes()
	remoteNodes := make(NodeList, 0, len(allNodes))
	for _, n := range allNodes {
		if n.name != serverId {
			remoteNodes = append(remoteNodes, n)
		}
	}
	return remoteNodes
}
