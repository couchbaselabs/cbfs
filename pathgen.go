package main

import (
	"log"
	"strings"
	"sync"

	cb "github.com/couchbaselabs/go-couchbase"
)

type namedFile struct {
	name string
	meta fileMeta
	err  error
}

func pathDataFetcher(c *Container, wg *sync.WaitGroup, quit <-chan bool,
	in <-chan string, out chan<- *namedFile) {
	defer wg.Done()

	for {
		select {
		case s, ok := <-in:
			if !ok {
				return
			}
			ob := namedFile{name: s}
			ob.err = couchbase.Get(c.shortName(s), &ob.meta)
			out <- &ob
		case <-quit:
			return
		}
	}
}

func (c Container) pathGenerator(from string, ch chan *namedFile,
	errs chan error, quit chan bool) {

	parts := strings.Split(from, "/")

	viewRes := struct {
		Rows []struct {
			Key []string
			Id  string
		}
		Errors []cb.ViewError
	}{}

	limit := 1000
	fetchch := make(chan string, limit)
	startKey := parts
	done := false

	wg := &sync.WaitGroup{}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go pathDataFetcher(&c, wg, quit, fetchch, ch)
	}
	defer func() {
		close(fetchch)
		wg.Wait()
		close(ch)
		close(errs)
	}()

	for !done {
		err := couchbase.ViewCustom("cbfs", "file_browse",
			map[string]interface{}{
				"stale":    false,
				"reduce":   false,
				"limit":    limit,
				"startkey": startKey,
			}, &viewRes)
		if err != nil {
			log.Printf("View error: %v", err)
			select {
			case errs <- err:
			case <-quit:
			}
			return
		}
		for _, e := range viewRes.Errors {
			select {
			case errs <- e:
			case <-quit:
				return
			}
		}

		done = len(viewRes.Rows) < limit

		for _, r := range viewRes.Rows {
			k := strings.Join(r.Key, "/")
			if !strings.HasPrefix(k, from) {
				return
			}
			startKey = r.Key

			fetchch <- k
		}
	}
}
