package cbfsclient

import (
	"fmt"
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
	return fmt.Sprintf("http://%s/.cbfs/blob/%s", a.Addr, h)
}

// Get the information about the nodes in a cluster.
func (c Client) Nodes() (map[string]StorageNode, error) {
	rv := map[string]StorageNode{}
	err := getJsonData(c.URLFor("/.cbfs/nodes/"), &rv)
	return rv, err
}
