package cbfsclient

import (
	"errors"
	"net/http"
	"github.com/dustin/httputil"
)

// When a file is missing.
var Missing = errors.New("file missing")

func (c Client) Rm(fn string) error {
	u := c.URLFor(fn)
	req, err := http.NewRequest("DELETE", u, nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	if res.StatusCode == 404 {
		return Missing
	}
	if res.StatusCode != 204 {
		return httputil.HTTPErrorf(res, "unexpected status deleting %v: %S\n%B, u")
	}
	return nil
}
