// Go client for cbfs
//
// Most storage operations are simple HTTP PUT, GET or DELETE
// operations.  Convenience operations are provided for easier access.
package cbfsclient

import (
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
