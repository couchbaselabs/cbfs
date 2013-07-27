package main

import (
	"encoding/json"
	"strings"
)

type fileListing struct {
	Files map[string]interface{} `json:"files"`
	Dirs  map[string]interface{} `json:"dirs"`
	Path  string                 `json:"path"`
}

func toStringJoin(in []interface{}, sep string) string {
	s := []string{}
	for _, a := range in {
		s = append(s, a.(string))
	}
	return strings.Join(s, sep)
}

func (c Container) listFiles(path string, includeMeta bool,
	depth int) (fileListing, error) {

	emptyObject := &(json.RawMessage{'{', '}'})
	viewRes := struct {
		Rows []struct {
			Key   []interface{}
			Value struct {
				Count, Sum, Min, Max int64
			}
		}
	}{}

	// use the requested path to build our view query parameters
	endKey := []interface{}{}
	if path != "" {
		for _, k := range strings.Split(path, "/") {
			endKey = append(endKey, k)
		}
	}
	endKey = append(endKey, emptyObject)
	startKey := endKey[:len(endKey)-1]
	groupLevel := len(startKey) + depth

	// query the view
	err := couchbase.ViewCustom("cbfs", "file_browse",
		map[string]interface{}{
			"group_level": groupLevel,
			"start_key":   startKey,
			"end_key":     endKey,
		}, &viewRes)
	if err != nil {
		return fileListing{}, err
	}

	// use the view result to build a list of keys
	keys := make([]string, len(viewRes.Rows), len(viewRes.Rows))
	for i, r := range viewRes.Rows {
		keys[i] = c.shortName(toStringJoin(r.Key, "/"))
	}

	// do a multi-get on the all the keys returned
	bulkResult, err := couchbase.GetBulk(keys)
	if err != nil {
		return fileListing{}, err
	}

	// divide items up into files and directories
	files := map[string]interface{}{}
	dirs := map[string]interface{}{}
	for _, r := range viewRes.Rows {
		key := c.shortName(toStringJoin(r.Key, "/"))
		subkey := r.Key
		if len(r.Key) > depth {
			subkey = r.Key[len(r.Key)-depth:]
		}
		name := toStringJoin(subkey, "/")
		res, ok := bulkResult[key]
		if ok == true {
			// this means we have a file
			if includeMeta {
				rm := json.RawMessage(res.Body)
				files[name] = &rm
			} else {
				files[name] = emptyObject
			}
		} else {
			// no record in the multi-get means this is a directory
			dirs[name] = struct {
				Count int64 `json:"descendants"`
				Sum   int64 `json:"size"`
				Min   int64 `json:"smallest"`
				Max   int64 `json:"largest"`
			}{r.Value.Count, r.Value.Sum, r.Value.Min, r.Value.Max}
		}
	}

	rv := fileListing{
		Path:  "/" + path,
		Dirs:  dirs,
		Files: files,
	}

	return rv, nil
}
