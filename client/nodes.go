package cbfsclient

import (
	"fmt"
	"math/rand"
	"time"
)

// Representation of a storage node.
type StorageNode struct {
	Addr      string
	AddrRaw   string    `json:"addr_raw"`
	Started   time.Time `json:"starttime"`
	HBTime    time.Time `json:"hbtime"`
	BindAddr  string
	FrameBind string
	HBAgeStr  string `json:"hbage_str"`
	Used      int64
	Free      int64
	Size      int64
	UptimeStr string `json:"uptime_str"`
	Version   string
}

func (a StorageNode) BlobURL(h string) string {
	return a.URLFor("/.cbfs/blob/" + h)
}

func (a StorageNode) URLFor(h string) string {
	if h[0] != '/' {
		h = "/" + h
	}
	return fmt.Sprintf("http://%s%s", a.Addr, h)
}

// Get the information about the nodes in a cluster.
func (c *Client) Nodes() (map[string]StorageNode, error) {
	var err error
	if c.nodes == nil {
		c.nodes = map[string]StorageNode{}
		err = getJsonData(c.URLFor("/.cbfs/nodes/"), &c.nodes)
	}
	return c.nodes, err
}

const staleDuration = time.Minute

func stale(s string) bool {
	d, err := time.ParseDuration(s)
	if err != nil {
		return true
	}
	return d > staleDuration
}

func (c *Client) RandomNode() (string, StorageNode, error) {
	nodeMap, err := c.Nodes()
	if err != nil {
		return "", StorageNode{}, err
	}

	nodes := make([]string, 0, len(nodeMap))
	for k, node := range nodeMap {
		if !stale(node.HBAgeStr) {
			nodes = append(nodes, k)
		}
	}

	if len(nodes) == 0 {
		return "", StorageNode{}, fmt.Errorf("No nodes available")
	}

	name := nodes[rand.Intn(len(nodes))]

	return name, nodeMap[name], nil
}
