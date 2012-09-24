package main

import (
	"encoding/json"
	"log"
	"strings"
)

func listFiles(path string, includeMeta bool,
	depth int) (map[string]interface{}, error) {

	viewRes := struct {
		Rows []struct {
			Key   []string
			Value map[string]interface{}
		}
	}{}

	// use the requested path to build our view query parameters
	startKey := []string{}
	if path != "" {
		startKey = strings.Split(path, "/")
	}
	endKey := make([]string, len(startKey)+1, len(startKey)+1)
	copy(endKey, startKey)
	endKey[len(startKey)] = "ZZZZZZ" // FIXME use {} instead
	groupLevel := len(startKey) + depth

	// query the view
	err := couchbase.ViewCustom("cbfs", "file_browse",
		map[string]interface{}{
			"group_level": groupLevel,
			"start_key":   startKey,
			"end_key":     endKey,
		}, &viewRes)
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
		name := strings.Join(r.Key[len(r.Key)-depth:], "/")
		res, ok := bulkResult[key]
		if ok == true {
			// this means we have a file
			rv := map[string]interface{}{}
			err := json.Unmarshal(res.Body, &rv)
			if err != nil {
				log.Printf("Error deserializing json, ignoring: %v", err)
			} else {
				if includeMeta {
					files[name] = rv
				} else {
					files[name] = map[string]interface{}{}
				}
			}
		} else {
			// no record in the multi-get metans this is a directory
			dirs[name] = map[string]interface{}{
				"children": r.Value["count"],
				"size":     r.Value["sum"],
				"smallest": r.Value["min"],
				"largest":  r.Value["max"],
			}
		}
	}

	// assemble the final return value
	rv := map[string]interface{}{"path": "/" + path, "dirs": dirs, "files": files}

	return rv, nil
}
