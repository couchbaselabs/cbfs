package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dustin/go-humanize"
)

type cbfsDir struct {
	Descendants int
	Largest     int64
	Size        int64
	Smallest    int64
}

// XXX: I probably should make this usable from the cbfs project
// instead of copying it around.
type prevMeta struct {
	Headers  http.Header `json:"headers"`
	OID      string      `json:"oid"`
	Length   float64     `json:"length"`
	Modified time.Time   `json:"modified"`
	Revno    int         `json:"revno"`
}

type fileMeta struct {
	Headers  http.Header      `json:"headers"`
	OID      string           `json:"oid"`
	Length   float64          `json:"length"`
	Userdata *json.RawMessage `json:"userdata,omitempty"`
	Modified time.Time        `json:"modified"`
	Previous []prevMeta       `json:"older"`
	Revno    int              `json:"revno"`
}

type listResult struct {
	Dirs  map[string]cbfsDir
	Files map[string]fileMeta
}

func listStuff(ustr string) (listResult, error) {
	result := listResult{}

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

	res, err := http.Get(inputUrl.String())
	if err != nil {
		return result, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return result, fmt.Errorf("Error in request to %v: %v",
			inputUrl.String(), res.Status)
	}

	d := json.NewDecoder(res.Body)
	err = d.Decode(&result)
	if err != nil {
		return result, err
	}

	return result, nil
}

func lsCommand(u string, args []string) {
	if len(args) > 0 {
		u = relativeUrl(u, args[0])
	}

	result, err := listStuff(u)
	if err != nil {
		log.Fatalf("Error listing directory: %v", err)
	}

	dirnames := sort.StringSlice{}
	filenames := sort.StringSlice{}
	for k := range result.Dirs {
		dirnames = append(dirnames, k)
	}
	for k := range result.Files {
		filenames = append(filenames, k)
	}
	dirnames.Sort()
	filenames.Sort()

	tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	for i := range dirnames {
		dn := dirnames[i]
		di := result.Dirs[dn]
		fmt.Fprintf(tw, "d %8s\t%s\t(%s descendants)\n",
			humanize.Bytes(uint64(di.Size)), dn,
			humanize.Comma(int64(di.Descendants)))
	}
	for i := range filenames {
		fn := filenames[i]
		fi := result.Files[fn]
		fmt.Fprintf(tw, "f %8s\t%s\t%s\n",
			humanize.Bytes(uint64(fi.Length)), fn,
			fi.Headers.Get("Content-Type"))
	}
	tw.Flush()
}
