// Go client for cbfs
//
// Most storage operations are simple HTTP PUT, GET or DELETE
// operations.  Convenience operations are provided for easier access.
package cbfsclient

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// A cbfs client.
type Client struct {
	u     string
	nodes map[string]StorageNode
}

// Construct a new cbfs client.
func New(u string) (*Client, error) {
	uc, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	uc.Path = "/"
	return &Client{u: uc.String()}, nil
}

// Get the full URL for the given filename.
func (c Client) URLFor(fn string) string {
	for strings.HasPrefix(fn, "/") {
		fn = fn[1:]
	}
	return string(c.u) + fn
}

func getJsonData(u string, into interface{}) error {
	res, err := http.Get(u)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("HTTP Error: %v", res.Status)
	}

	d := json.NewDecoder(res.Body)
	return d.Decode(into)
}
