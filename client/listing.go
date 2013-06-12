// Go client for cbfs
//
// Most storage operations are simple HTTP PUT, GET or DELETE
// operations.  Convenience operations are provided for easier access.
package cbfsclient

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Represents a directory as returned from a List operation.
type Dir struct {
	Descendants int   // Number of descendants.
	Largest     int64 // Size of largest file.
	Size        int64 // Total size of all files.
	Smallest    int64 // Size of smallest file.
}

// Meta information from a previous revision of a file.
type PrevMeta struct {
	Headers  http.Header `json:"headers"`  // Headers
	OID      string      `json:"oid"`      // Hash
	Length   float64     `json:"length"`   // Length
	Modified time.Time   `json:"modified"` // Modified date
	Revno    int         `json:"revno"`    // Revision number
}

// Current file meta.
type FileMeta struct {
	Headers http.Header `json:"headers"` // Headers
	OID     string      `json:"oid"`     // Hash
	Length  float64     `json:"length"`  // Length
	// User-supplied JSON
	Userdata *json.RawMessage `json:"userdata,omitempty"`
	// Last modified time
	Modified time.Time `json:"modified"`
	// Recorded previous versions
	Previous []PrevMeta `json:"older"`
	// Current revision number
	Revno int `json:"revno"`
}

// Results from a list operation.
type ListResult struct {
	Dirs  map[string]Dir      // Immediate directories
	Files map[string]FileMeta // Immediate files
}

var fourOhFour = errors.New("not found")

// Same as List, but return an empty result on 404.
func ListOrEmpty(ustr string) (ListResult, error) {
	listing, err := List(ustr)
	if err == fourOhFour {
		err = nil
	}

	return listing, err
}

// List the contents below the given location.
func List(ustr string) (ListResult, error) {
	result := ListResult{}

	inputUrl, err := url.Parse(ustr)
	if err != nil {
		return result, err
	}

	inputUrl.Path = "/.cbfs/list" + inputUrl.Path
	for strings.HasSuffix(inputUrl.Path, "/") {
		inputUrl.Path = inputUrl.Path[:len(inputUrl.Path)-1]
	}
	if inputUrl.Path == "/.cbfs/list" {
		inputUrl.Path = "/.cbfs/list/"
	}
	inputUrl.RawQuery = "includeMeta=true"

	req, err := http.NewRequest("GET", inputUrl.String(), nil)
	if err != nil {
		return result, err
	}

	req.Header.Set("Accept-Encoding", "gzip")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return result, err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case 404:
		return result, fourOhFour
	case 200:
		// ok
	default:
		return result, fmt.Errorf("Error in request to %v: %v",
			inputUrl, res.Status)
	}

	r := io.Reader(res.Body)

	if res.Header.Get("Content-Encoding") == "gzip" {
		gzr, err := gzip.NewReader(res.Body)
		if err != nil {
			return result, err
		}
		r = gzr
	}

	d := json.NewDecoder(r)
	err = d.Decode(&result)
	if err != nil {
		return result, err
	}

	return result, nil
}
