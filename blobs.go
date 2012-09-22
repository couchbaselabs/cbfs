package main

import (
	"encoding/json"
	"log"
	"sort"
	"time"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

type BlobOwnership struct {
	OID    string               `json:"oid"`
	Length int64                `json:"length"`
	Nodes  map[string]time.Time `json:"nodes"`
	Type   string               `json:"type"`
}

func (b BlobOwnership) ResolveNodes() NodeList {
	keys := make([]string, 0, len(b.Nodes))
	for k := range b.Nodes {
		keys = append(keys, "/"+k)
	}
	resps := couchbase.GetBulk(keys)

	rv := make(NodeList, 0, len(resps))

	for k, v := range resps {
		if v.Status == gomemcached.SUCCESS {
			a := StorageNode{}
			err := json.Unmarshal(v.Body, &a)
			if err == nil {
				a.name = k[1:]
				rv = append(rv, a)
			}
		}
	}

	sort.Sort(rv)

	return rv
}

func (b BlobOwnership) ResolveRemoteNodes() NodeList {
	return b.ResolveNodes().minusLocal()
}

func recordBlobOwnership(h string, l int64) error {
	k := "/" + h
	return couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		_, err := mc.CAS(vb, k, func(in []byte) ([]byte, memcached.CasOp) {
			ownership := BlobOwnership{}
			err := json.Unmarshal(in, &ownership)
			if err == nil {
				ownership.Nodes[serverId] = time.Now().UTC()
			} else {
				ownership.Nodes = map[string]time.Time{
					serverId: time.Now().UTC(),
				}
			}
			ownership.OID = h
			ownership.Length = l
			ownership.Type = "blob"
			return mustEncode(&ownership), memcached.CASStore
		}, 0)
		return err
	})
}

func recordBlobAccess(h string) {
	_, err := couchbase.Incr("/"+h+"/r", 1, 1, 0)
	if err != nil {
		log.Printf("Error incrementing counter for %v: %v", h, err)
	}

	_, err = couchbase.Incr("/"+serverId+"/r", 1, 1, 0)
	if err != nil {
		log.Printf("Error incrementing node identifier: %v", h, err)
	}
}

// Returns the number of known owners (-1 if it can't be determined)
func removeBlobOwnershipRecord(h, node string) int {
	log.Printf("Cleaning up %v from %v", h, node)
	numOwners := -1

	k := "/" + h
	err := couchbase.Do(k, func(mc *memcached.Client, vb uint16) error {
		_, err := mc.CAS(vb, k, func(in []byte) ([]byte, memcached.CasOp) {
			ownership := BlobOwnership{}

			if len(in) == 0 {
				return nil, memcached.CASQuit
			}

			err := json.Unmarshal(in, &ownership)
			if err == nil {
				if _, ok := ownership.Nodes[node]; !ok {
					// Skip it fast if we don't have it.
					return nil, memcached.CASQuit
				}
				delete(ownership.Nodes, node)
			} else {
				log.Printf("Error unmarhaling blob removal from %s: %v",
					in, err)
				return nil, memcached.CASQuit
			}

			var rv []byte
			op := memcached.CASStore

			numOwners = len(ownership.Nodes)

			if len(ownership.Nodes) == 0 {
				op = memcached.CASDelete
			} else {
				rv = mustEncode(&ownership)
			}

			return rv, op
		}, 0)
		return err
	})
	if err != nil && err != memcached.CASQuit {
		log.Printf("Error cleaning %v from %v: %v", node, h, err)
		numOwners = -1
	}

	return numOwners
}

func ensureMinimumReplicaCount() error {
	nl, err := findAllNodes()
	if err != nil {
		return err
	}

	viewRes := struct {
		Rows []struct {
			Id string
		}
	}{}

	// Don't bother trying to replicate to more nodes than exist.
	endKey := globalConfig.MinReplicas - 1
	if globalConfig.MinReplicas > len(nl) {
		endKey = len(nl) - 1
	}

	// Find some less replicated docs to suck in.
	err = couchbase.ViewCustom("cbfs", "repcounts",
		map[string]interface{}{
			"reduce":   false,
			"limit":    1000,
			"startkey": 1,
			"endkey":   endKey,
			"stale":    false,
		},
		&viewRes)

	if err != nil {
		return err
	}

	log.Printf("Increasing replica count of %v items",
		len(viewRes.Rows))

	for _, r := range viewRes.Rows {
		salvageBlob(r.Id[1:], "", nl)
	}
	return nil
}

func pruneBlob(oid string, nodemap map[string]string, nl NodeList,
	ch chan<- cleanupObject) {

	if len(nodemap) <= globalConfig.MaxReplicas {
		log.Printf("Asked to prune a blob that has too few replicas: %v",
			oid)
	}

	log.Printf("Pruning blob %v down from %v repls to %v",
		oid, len(nodemap), globalConfig.MaxReplicas)

	nm := map[string]StorageNode{}
	for _, n := range nl {
		nm[n.name] = n
	}

	remaining := len(nodemap)
	for n := range nodemap {
		if remaining <= globalConfig.MaxReplicas {
			break
		}
		remaining--
		if sn, ok := nm[n]; ok {
			ch <- cleanupObject{oid, sn}
		}
	}

}

func pruneExcessiveReplicas() error {
	nl, err := findAllNodes()
	if err != nil {
		return err
	}

	viewRes := struct {
		Rows []struct {
			Id  string
			Doc struct {
				Json struct {
					Nodes map[string]string
				}
			}
		}
	}{}

	// Find some less replicated docs to suck in.
	err = couchbase.ViewCustom("cbfs", "repcounts",
		map[string]interface{}{
			"descending":   true,
			"reduce":       false,
			"include_docs": true,
			"limit":        1000,
			"endkey":       globalConfig.MaxReplicas + 1,
			"stale":        false,
		},
		&viewRes)

	if err != nil {
		return err
	}

	log.Printf("Decreasing replica count of %v items",
		len(viewRes.Rows))

	// Short-circuit when there's nothing to clean
	if len(viewRes.Rows) == 0 {
		return nil
	}

	ch := make(chan cleanupObject, 1000)
	defer close(ch)

	for i := 0; i < *cleanupWorkers; i++ {
		go cleanupWorker(ch)
	}

	for _, r := range viewRes.Rows {
		pruneBlob(r.Id[1:], r.Doc.Json.Nodes, nl, ch)
	}
	return nil
}
