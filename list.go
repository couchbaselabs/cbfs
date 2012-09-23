package main

import (
	"encoding/json"
	"log"
	"strings"
)

func listFiles(path string, includeMeta bool) (map[string]interface{}, error) {
	viewRes := struct {
		Rows []struct {
			Key   []string
			Value float64
		}
	}{}

	// use the requested path to build our view query parameters
	startKey := strings.Split(path, "/")
	endKey := make([]string, len(startKey)+1, len(startKey)+1)
	copy(endKey, startKey)
	endKey[len(startKey)] = "ZZZZZZ" // FIXME use {} instead
	groupLevel := len(startKey) + 1

	// query the view
	err := couchbase.ViewCustom("cbfs", "file_browse",
		map[string]interface{}{"group_level": groupLevel, "start_key": startKey, "end_key": endKey}, &viewRes)
	if err != nil {
		return nil, err
	}

	// use the view result to build a list of keys
	keys := make([]string, len(viewRes.Rows), len(viewRes.Rows))
	for i, r := range viewRes.Rows {
		keys[i] = strings.Join(r.Key, "/")
	}

	// do a multi-get on the all the keys returned
	bulkResult := couchbase.GetBulk(keys)

	// divide items up into files and directories
	files := map[string]interface{}{}
	dirs := map[string]interface{}{}
	for _, r := range viewRes.Rows {
		key := strings.Join(r.Key, "/")
		res, ok := bulkResult[key]
		if ok == true {
			// this means we have a file
			rv := map[string]interface{}{}
			err := json.Unmarshal(res.Body, &rv)
			if err != nil {
				log.Printf("Error deserializing json, ignoring: %v", err)
			} else {
				if includeMeta {
					files[r.Key[len(r.Key)-1]] = rv
				} else {
					files[r.Key[len(r.Key)-1]] = map[string]interface{}{}
				}
			}
		} else {
			// no record in the multi-get metans this is a directory
			dirs[r.Key[len(r.Key)-1]] = map[string]interface{}{}
		}
	}

	// assemble the final return value
	rv := map[string]interface{}{"dirs": dirs, "files": files}

	return rv, nil
}
