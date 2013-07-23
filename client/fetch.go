package cbfsclient

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-saturate"
)

type FetchCallback func(oid string, r io.Reader) error

// Blob info as returned from GetBlobInfos
type BlobInfo struct {
	// Map of node name -> last time the object was validated
	Nodes map[string]time.Time
}

// Find out what nodes contain the given blobs.
func (c Client) GetBlobInfos(oids ...string) (map[string]BlobInfo, error) {
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
	rv := map[string]BlobInfo{}
	err = d.Decode(&rv)
	return rv, err
}

type fetchWork struct {
	oid string
	bi  BlobInfo
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
	c       Client
	oid     string
	length  int64
	meta    FileMeta
	nodes   map[string]time.Time
	rdrImpl io.ReadCloser
}

// The nodes containing the files and the last time it was scrubed.
func (f *FileHandle) Nodes() map[string]time.Time {
	return f.nodes
}

// The headers from the file request.
func (f *FileHandle) Meta() FileMeta {
	return f.meta
}

func (f *FileHandle) randomUrl() (string, error) {
	nodes, err := f.c.Nodes()
	if err != nil {
		return "", err
	}

	nodelist := []StorageNode{}
	for k := range nodes {
		if n, ok := nodes[k]; ok {
			nodelist = append(nodelist, n)
		}
	}

	return nodelist[rand.Intn(len(nodelist))].BlobURL(f.oid), nil
}

func (f *FileHandle) Read(b []byte) (int, error) {
	if f.rdrImpl == nil {
		u, err := f.randomUrl()
		if err != nil {
			return 0, err
		}
		res, err := http.Get(u)
		if err != nil {
			return 0, err
		}
		if res.StatusCode != 200 {
			return 0, fmt.Errorf("Unexpected http response: %v",
				res.Status)
		}
		f.rdrImpl = res.Body
	}
	return f.rdrImpl.Read(b)
}

func (f *FileHandle) Close() error {
	if f.rdrImpl == nil {
		return fmt.Errorf("Not open")
	}
	r := f.rdrImpl
	f.rdrImpl = nil
	return r.Close()
}

// Implement io.WriterTo
func (f *FileHandle) WriteTo(w io.Writer) (int64, error) {
	u, err := f.randomUrl()
	if err != nil {
		return 0, err
	}
	res, err := http.Get(u)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return 0, fmt.Errorf("Unexpected http response: %v", res.Status)
	}
	return io.Copy(w, res.Body)
}

// Implement io.ReaderAt
func (f *FileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	end := int64(len(p)) + off
	if end > f.length {
		return 0, fmt.Errorf("Would seek past EOF")
	}

	u, err := f.randomUrl()
	if err != nil {
		return 0, err
	}

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

func noSlash(s string) string {
	for strings.HasPrefix(s, "/") {
		s = s[1:]
	}
	return s
}

func (f *FileHandle) Name() string {
	return "" // TODO:  something smarter
}

// Length of this file
func (f *FileHandle) Size() int64 {
	return f.length
}

// file mode (0444)
func (*FileHandle) Mode() os.FileMode {
	return 0444
}

// Last modification time of this file
func (f *FileHandle) ModTime() time.Time {
	return f.meta.Modified
}

// nil
func (*FileHandle) Sys() interface{} {
	return nil
}

// false
func (*FileHandle) IsDir() bool { return false }

// Some assertions around filehandle's applicability
var (
	_ = os.FileInfo(&FileHandle{})
	_ = io.Closer(&FileHandle{})
	_ = io.Reader(&FileHandle{})
	_ = io.ReaderAt(&FileHandle{})
	_ = io.WriterTo(&FileHandle{})
)

// Get a reference to the file at the given path.
func (c Client) OpenFile(path string) (*FileHandle, error) {
	res, err := http.Get(c.URLFor("/.cbfs/info/file/" + noSlash(path)))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP error: %v", res.Status)
	}
	j := struct {
		Meta FileMeta
		Path string
	}{}
	d := json.NewDecoder(res.Body)
	err = d.Decode(&j)
	if err != nil {
		return nil, err
	}

	h := j.Meta.OID

	infos, err := c.GetBlobInfos(h)
	if err != nil {
		return nil, err
	}

	return &FileHandle{c, h, res.ContentLength, j.Meta,
		infos[h].Nodes, nil}, nil
}
