package cbfsclient

import (
	"fmt"
	"net/url"
	"strings"
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
