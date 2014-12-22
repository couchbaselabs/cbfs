package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	cb "github.com/couchbaselabs/go-couchbase"
)

var serverId string

var nodeKeys = flag.Int("nodeKeys", 0, "number of additional node keys")

func init() {
	flag.StringVar(&serverId, "nodeID", "",
		"Node ID (defaults to what's stored in guid file or arbitrary)")
}

const nodeListPrefix = "/@nodes"

var nodeListKeys = []string{nodeListPrefix}

func initNodeListKeys() {
	for i := 0; i < *nodeKeys; i++ {
		nodeListKeys = append(nodeListKeys,
			fmt.Sprintf("%v.%v", nodeListPrefix, i))
	}
}

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

type errslice []error

func (e errslice) Error() string {
	es := []string{}
	for _, err := range e {
		es = append(es, err.Error())
	}
	return "{Errors: " + strings.Join(es, ", ") + "}"
}

func setInNodeRegistry(nodeID string, size int64) error {
	rv := errslice{}
	for _, k := range nodeListKeys {
		err := couchbase.Update(k, 0, func(in []byte) ([]byte, error) {
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
		if err != nil {
			rv = append(rv, err)
		}
	}
	if len(rv) == 0 {
		return nil
	}
	return rv
}

func removeFromNodeRegistry(nodeID string) error {
	rv := errslice{}
	for _, k := range nodeListKeys {
		err := couchbase.Update(k, 0, func(in []byte) ([]byte, error) {
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
		if err != nil {
			rv = append(rv, err)
		}
	}
	if len(rv) == 0 {
		return nil
	}
	return rv
}

func retrieveNodeRegistry() (NodeRegistry, error) {
	reg := NodeRegistry{}
	var err error
	for _, k := range nodeListKeys {
		err = couchbase.Get(k, &reg)
		if err == nil {
			return reg, nil
		}
	}
	return reg, err
}

func initServerId() error {
	var err error
	var bytes []byte
	if len(bytes) > 0 && err == nil {
		serverId = strings.TrimSpace(string(bytes))
	} else {
		if serverId == "" {
			log.Printf("NodeID was not given, generating one")
			h := getHash()
			t := time.Now().UTC().Format(time.RFC3339Nano)
			h.Write([]byte(t))
			serverId = hex.EncodeToString(h.Sum(nil))[:8]
		}
	}
	log.Printf("serverID: %v", serverId)
	return err
}
