package cbfsclient

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
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

func nodeFetchWorker(nodeName string, node StorageNode, cb FetchCallback,
	ch chan nodeFetchWork, wg *sync.WaitGroup) {
	defer wg.Done()

	for wi := range ch {
		wi.res <- fetchOne(wi.oid, node, cb)
	}
}

func fillSelector(wi nodeFetchWork, workchans map[string]chan nodeFetchWork,
	nodes map[string]time.Time) []reflect.SelectCase {

	cases := []reflect.SelectCase{}
	for n := range nodes {
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectSend,
			Chan: reflect.ValueOf(workchans[n]),
			Send: reflect.ValueOf(wi),
		})
	}

	return cases
}

func fetchWorker(cb FetchCallback, nodes map[string]StorageNode,
	ch chan fetchWork, workchans map[string]chan nodeFetchWork,
	errch chan<- error, wg *sync.WaitGroup) {

	defer wg.Done()
	for w := range ch {

		wi := nodeFetchWork{
			oid: w.oid,
			res: make(chan error, 1),
		}

		var err error
		var cases []reflect.SelectCase
		availCases := 0
		for i := 0; i < 3; i++ {
			if availCases == 0 {
				cases = fillSelector(wi, workchans, w.bi.Nodes)
			}
			selected, _, _ := reflect.Select(cases)
			err = <-wi.res
			if err == nil {
				break
			}
			// Now we have to retry as something went
			// wrong.  We null out this node's channel
			// since it gave an error, allowing us to
			// retry on any other available node.
			availCases--
			cases[selected].Chan = reflect.ValueOf(nil)
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

type nodeFetchWork struct {
	oid string
	res chan error
}

// Fetch many blobs in bulk.
func (c *Client) Blobs(totalConcurrency, destinationConcurrency int,
	cb FetchCallback, oids ...string) error {

	nodes, err := c.Nodes()
	if err != nil {
		return err
	}
	wgt := &sync.WaitGroup{}
	wgn := &sync.WaitGroup{}

	infos, err := c.getBlobInfos(oids...)
	if err != nil {
		return err
	}

	// Error result goes here.
	errch := make(chan error, 1)
	// Each blob we need to fetch goes here.
	workch := make(chan fetchWork)
	// Each node worker will receive its blob to do here.
	workchans := map[string]chan nodeFetchWork{}

	// Spin up destination workers.
	for name, node := range nodes {
		ch := make(chan nodeFetchWork)
		workchans[name] = ch
		for i := 0; i < destinationConcurrency; i++ {
			wgn.Add(1)
			go nodeFetchWorker(name, node, cb, ch, wgn)
		}
	}

	// Spin up blob (fanout) workers.
	for i := 0; i < totalConcurrency; i++ {
		wgt.Add(1)
		go fetchWorker(cb, nodes, workch, workchans, errch, wgt)
	}

	// Feed the blob (fanout) workers.
	for oid, info := range infos {
		workch <- fetchWork{oid, info}
	}

	// Let everything know we're done.
	close(workch)
	wgt.Wait()
	for _, c := range workchans {
		close(c)
	}

	go func() {
		wgn.Wait()
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
