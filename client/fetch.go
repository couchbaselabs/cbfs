package cbfsclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-saturate"
	"github.com/dustin/httputil"
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
		return nil, httputil.HTTPErrorf(res, "error fetching blob info: %S\n%B")
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
		return httputil.HTTPError(res)
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

	workch := make(chan saturate.WorkInput)
	go func() {
		// Feed the blob (fanout) workers.
		for oid, info := range infos {
			nodes := []string{}
			for n := range info.Nodes {
				nodes = append(nodes, n)
			}
			workch <- saturate.WorkInput{Input: oid, Dests: nodes}
		}

		// Let everything know we're done.
		close(workch)
	}()

	s := saturate.New(dests, func(n string) saturate.Worker {
		return &fetchWorker{nodeMap[n], cb}
	},
		&saturate.Config{
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
		redirectTarget := res.Header.Get("Location")
		log.Printf("Redirecting to %v", redirectTarget)
		resRedirect, err := http.Get(redirectTarget)
		if err != nil {
			return nil, err
		}
		// if we follow the redirect, make sure response code == 200
		switch resRedirect.StatusCode {
		case 200:
			return resRedirect.Body, nil
		default:
			return nil, fmt.Errorf(
				"Got %v response following redirect to %v",
				resRedirect.StatusCode,
				redirectTarget,
			)
		}

	default:
		defer res.Body.Close()
		return nil, httputil.HTTPError(res)
	}
}

// File info
type FileHandle struct {
	c      Client
	oid    string
	off    int64
	length int64
	meta   FileMeta
	nodes  map[string]time.Time
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
	allnodes, err := f.c.Nodes()
	if err != nil {
		return "", err
	}

	nodelist := []StorageNode{}
	for k := range f.nodes {
		if n, ok := allnodes[k]; ok {
			nodelist = append(nodelist, n)
		}
	}

	return nodelist[rand.Intn(len(nodelist))].BlobURL(f.oid), nil
}

func (f *FileHandle) Read(b []byte) (int, error) {
	if f.off >= f.length {
		return 0, io.EOF
	}
	n, err := f.ReadAt(b, f.off)
	f.off += int64(n)
	return n, err
}

func (f *FileHandle) Close() error {
	return nil
}

// Implement io.WriterTo
func (f *FileHandle) WriteTo(w io.Writer) (int64, error) {
	u, err := f.randomUrl()
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return 0, err
	}
	if f.off > 0 {
		req.Header.Set("Range",
			fmt.Sprintf("bytes=%v-%v", f.off, f.length-1))
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}

	defer res.Body.Close()
	if res.StatusCode != 200 {
		return 0, httputil.HTTPErrorf(res, "Unexpected http response: %S\n%B")
	}

	n, err := io.Copy(w, res.Body)
	f.off += n
	return n, err
}

// Implement io.ReaderAt
func (f *FileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	end := int64(len(p)) + off
	if end >= f.length {
		end = f.length
	}

	u, err := f.randomUrl()
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", off, end-1))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	exp := 206
	if off == 0 && end == f.length {
		exp = 200
	}
	if res.StatusCode != exp {
		return 0, httputil.HTTPErrorf(res, "Unexpected http response: %S\n%B")
	}

	n, err = io.ReadFull(res.Body, p)
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	return n, err
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

func (f *FileHandle) Seek(offset int64, whence int) (ret int64, err error) {
	abs := int64(0)
	switch whence {
	case 0:
		abs = offset
	case 1:
		abs = f.off + offset
	case 2:
		abs = f.length + offset
	default:
		return 0, errors.New("bytes: invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("bytes: negative position")
	}
	if abs >= f.length {
		return 0, errors.New("bytes: position out of range")
	}

	f.off = abs
	return f.off, nil
}

// Get a reference to the file at the given path.
func (c Client) OpenFile(path string) (*FileHandle, error) {
	res, err := http.Get(c.URLFor("/.cbfs/info/file/" + noSlash(path)))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, httputil.HTTPError(res)
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

	return &FileHandle{c, h, 0, j.Meta.Length, j.Meta,
		infos[h].Nodes}, nil
}
