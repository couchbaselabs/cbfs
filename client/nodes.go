package cbfsclient

import (
	"net/url"
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

// Get the information about the nodes in a cluster.
func (c Client) Nodes() (map[string]StorageNode, error) {
	inputUrl, err := url.Parse(string(c))
	if err != nil {
		return nil, err
	}

	inputUrl.Path = "/.cbfs/nodes/"
	rv := map[string]StorageNode{}
	err = getJsonData(inputUrl.String(), &rv)
	return rv, err
}
