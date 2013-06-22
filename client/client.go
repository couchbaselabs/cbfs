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
)

// A cbfs client.
type Client string

// Construct a new cbfs client.
func New(u string) (*Client, error) {
	uc, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	uc.Path = "/"
	rv := Client(uc.String())
	return &rv, nil
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
