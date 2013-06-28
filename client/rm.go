package cbfsclient

import (
	"errors"
	"fmt"
	"net/http"
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
		return fmt.Errorf("Unexpected status deleting %v: %v",
			u, res.Status)
	}
	return nil
}
