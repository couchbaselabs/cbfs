package cbfsclient

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/dustin/go-saturate"
)

type FetchCallback func(oid string, r io.Reader) error

type blobInfo struct {
	Nodes map[string]time.Time
}

func (c Client) GetBlobInfos(oids ...string) (map[string]blobInfo, error) {
	u := c.URLFor("/.cbfs/blob/info/")
	form := url.Values{"blob": oids}
	res, err := http.PostForm(u, form)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP error fetching blob info: %v",
			res.Status)
	}

	d := json.NewDecoder(res.Body)
	rv := map[string]blobInfo{}
	err = d.Decode(&rv)
	return rv, err
}

type fetchWork struct {
	oid string
	bi  blobInfo
}

type brokenReader struct{ err error }

func (b brokenReader) Read([]byte) (int, error) {
	return 0, b.err
}

type fetchWorker struct {
	n  StorageNode
	cb FetchCallback
}

func (fw fetchWorker) Work(i interface{}) error {
	oid := i.(string)
	res, err := http.Get(fw.n.BlobURL(oid))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("HTTP error: %v", res.Status)
	}
	return fw.cb(oid, res.Body)
}

// Fetch many blobs in bulk.
func (c *Client) Blobs(totalConcurrency, destinationConcurrency int,
	cb FetchCallback, oids ...string) error {

	nodeMap, err := c.Nodes()
	if err != nil {
		return err
	}

	dests := make([]string, 0, len(nodeMap))
	for n := range nodeMap {
		dests = append(dests, n)
	}

	infos, err := c.GetBlobInfos(oids...)
	if err != nil {
		return err
	}

	workch := make(chan saturator.WorkInput)
	go func() {
		// Feed the blob (fanout) workers.
		for oid, info := range infos {
			nodes := []string{}
			for n := range info.Nodes {
				nodes = append(nodes, n)
			}
			workch <- saturator.WorkInput{Input: oid, Dests: nodes}
		}

		// Let everything know we're done.
		close(workch)
	}()

	s := saturator.New(dests, func(n string) saturator.Worker {
		return &fetchWorker{nodeMap[n], cb}
	},
		&saturator.Config{
			DestConcurrency:  destinationConcurrency,
			TotalConcurrency: totalConcurrency,
			Retries:          3,
		})

	return s.Saturate(workch)
}

// Grab a file.
//
// This ensures the request is coming directly from a node that
// already has the blob vs. proxying.
func (c Client) Get(path string) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", c.URLFor(path), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-CBFS-LocalOnly", "true")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	switch res.StatusCode {
	case 200:
		return res.Body, nil
	case 300:
		defer res.Body.Close()
		res, err = http.Get(res.Header.Get("Location"))
		if err != nil {
			return nil, err
		}
		return res.Body, nil
	default:
		defer res.Body.Close()
		return nil, fmt.Errorf("HTTP Error: %v", res.Status)
	}
}

// File info
type FileHandle struct {
	c      Client
	oid    string
	length int64
	header http.Header
	nodes  map[string]time.Time
}

// The nodes containing the files and the last time it was scrubed.
func (f *FileHandle) Nodes() map[string]time.Time {
	return f.nodes
}

// The headers from the file request.
func (f *FileHandle) Header() http.Header {
	return f.header
}

// Length of this file
func (f *FileHandle) Length() int64 {
	return f.length
}

// Implement io.ReaderAt
func (f *FileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	end := int64(len(p)) + off
	if end > f.length {
		return 0, fmt.Errorf("Would seek past EOF")
	}

	nodes, err := f.c.Nodes()
	if err != nil {
		return 0, err
	}

	nodelist := []StorageNode{}
	for k := range nodes {
		if n, ok := nodes[k]; ok {
			nodelist = append(nodelist, n)
		}
	}

	u := nodelist[rand.Intn(len(nodelist))].BlobURL(f.oid)

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", off, end))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != 206 {
		return 0, fmt.Errorf("Unexpected http response: %v", res.Status)
	}

	return io.ReadFull(res.Body, p)
}

// Get a reference to the file at the given path.
func (c Client) OpenFile(path string) (*FileHandle, error) {
	res, err := http.Head(c.URLFor(path))
	if err != nil {
		return nil, err
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP error: %v", res.Status)
	}

	h := res.Header.Get("etag")
	if h == "" {
		return nil, fmt.Errorf("No etag in response headers")
	}

	h = h[1 : len(h)-1]

	infos, err := c.GetBlobInfos(h)
	if err != nil {
		return nil, err
	}

	return &FileHandle{c, h, res.ContentLength, res.Header, infos[h].Nodes}, nil
}
