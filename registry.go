package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"strings"
	"time"

	cb "github.com/couchbaselabs/go-couchbase"
)

var serverId string

func init() {
	flag.StringVar(&serverId, "nodeID", "",
		"Node ID (defaults to what's stored in guid file or arbitrary)")
}

const nodeListKey = "/@nodes"

// List of names of nodes
type NodeRegistry struct {
	Nodes       map[string]int64 `json:"nodes"`
	LastModTime time.Time        `json:"lastModTime"`
	LastModBy   string           `json:"lastModBy"`
}

func validateServerId(s string) error {
	invalid := errors.New("Invalid server id: " + s)
	switch {
	case len(s) == 0:
		return invalid
	case s[0] == '/' || s[0] == '@':
		return invalid
	}
	return nil
}

func setInNodeRegistry(nodeID string, size int64) error {
	return couchbase.Update(nodeListKey, 0, func(in []byte) ([]byte, error) {
		reg := NodeRegistry{}
		err := json.Unmarshal(in, &reg)
		if err == nil {
			reg.Nodes[nodeID] = size
		} else {
			reg.Nodes = map[string]int64{
				nodeID: size,
			}
		}
		reg.LastModTime = time.Now().UTC()
		reg.LastModBy = serverId
		return json.Marshal(reg)
	})
}

func removeFromNodeRegistry(nodeID string) error {
	return couchbase.Update(nodeListKey, 0, func(in []byte) ([]byte, error) {
		reg := NodeRegistry{}
		err := json.Unmarshal(in, &reg)
		if err == nil {
			delete(reg.Nodes, nodeID)
		} else {
			return nil, cb.UpdateCancel
		}
		reg.LastModTime = time.Now().UTC()
		reg.LastModBy = serverId
		return json.Marshal(reg)
	})
}

func retrieveNodeRegistry() (NodeRegistry, error) {
	reg := NodeRegistry{}
	err := couchbase.Get(nodeListKey, &reg)
	return reg, err
}

func initServerId() error {
	var err error
	var bytes []byte
	if len(bytes) > 0 && err == nil {
		serverId = strings.TrimSpace(string(bytes))
	} else {
		if serverId == "" {
			h := getHash()
			t := time.Now().UTC().Format(time.RFC3339Nano)
			h.Write([]byte(t))
			serverId = hex.EncodeToString(h.Sum(nil))[:8]
		}
	}
	return err
}
