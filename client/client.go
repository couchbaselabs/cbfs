// Go client for cbfs
//
// Most storage operations are simple HTTP PUT, GET or DELETE
// operations.  Convenience operations are provided for easier access.
package cbfsclient

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/dustin/httputil"
)

// A cbfs client.
type Client struct {
	u     string
	pu    *url.URL
	nodes map[string]StorageNode
}

// Construct a new cbfs client.
func New(u string) (*Client, error) {
	uc, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	uc.Path = "/"
	return &Client{u: uc.String(), pu: uc}, nil
}

// Get the full URL for the given filename.
func (c Client) URLFor(fn string) string {
	for strings.HasPrefix(fn, "/") {
		fn = fn[1:]
	}
	return c.u + fn
}

func getJsonData(u string, into interface{}) error {
	res, err := http.Get(u)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return httputil.HTTPError(res)
	}

	d := json.NewDecoder(res.Body)
	return d.Decode(into)
}
