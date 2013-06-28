package cbfsclient

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type FetchCallback func(oid string, r io.Reader) error

type blobInfo struct {
	Nodes map[string]time.Time
}

func (c Client) getBlobInfos(oids ...string) (map[string]blobInfo, error) {
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

func fetchOne(oid string, si StorageNode, cb FetchCallback) error {
	res, err := http.Get(si.BlobURL(oid))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("HTTP error: %v", res.Status)
	}
	return cb(oid, res.Body)
}

func fetchWorker(cb FetchCallback, nodes map[string]StorageNode,
	ch chan fetchWork, errch chan<- error, wg *sync.WaitGroup) {

	defer wg.Done()
	for w := range ch {
		var err error
		for n := range w.bi.Nodes {
			err = fetchOne(w.oid, nodes[n], cb)
			if err == nil {
				break
			}
		}
		if err != nil {
			select {
			case errch <- err:
			default:
			}
			cb(w.oid,
				brokenReader{fmt.Errorf("couldn't find %v", w.oid)})
		}
	}
}

// Fetch many blobs in bulk.
func (c Client) Blobs(concurrency int,
	cb FetchCallback, oids ...string) error {

	nodes, err := c.Nodes()
	if err != nil {
		return err
	}

	infos, err := c.getBlobInfos(oids...)
	if err != nil {
		return err
	}

	workch := make(chan fetchWork)
	errch := make(chan error, 1)

	wg := &sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go fetchWorker(cb, nodes, workch, errch, wg)
	}

	for oid, info := range infos {
		workch <- fetchWork{oid, info}
	}
	close(workch)

	go func() {
		wg.Wait()
		close(errch)
	}()

	return <-errch
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

	if res.StatusCode == 300 {
		defer res.Body.Close()
		nu := res.Header.Get("Location")
		res, err = http.Get(nu)
		if err != nil {
			return nil, err
		}
	}

	return res.Body, nil
}
