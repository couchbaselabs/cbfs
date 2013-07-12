package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/couchbaselabs/cbfs/client"
	"github.com/couchbaselabs/cbfs/tools"
)

var fileInfoFlags = flag.NewFlagSet("fileInfo", flag.ExitOnError)

func fileInfoCommand(base string, args []string) {
	fileInfoFlags.Parse(args)

	u := cbfstool.ParseURL(base)
	u.Path = args[0]
	if u.Path[0] != '/' {
		u.Path = "/" + u.Path
	}

	res, err := http.Head(u.String())
	if err != nil {
		log.Fatalf("Error getting %v: %v", u, err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		log.Fatalf("HTTP Error on %v: %v", u, res.Status)
	}

	h := res.Header.Get("etag")
	if h == "" {
		log.Fatalf("No etag found.")
	}
	h = h[1 : len(h)-1]

	client, err := cbfsclient.New(u.String())
	infos, err := client.GetBlobInfos(h)
	if err != nil {
		log.Fatalf("Coudln't get blob info for %q: %v", h, err)
	}

	fmt.Printf("File: %q\n\n", u.Path[1:])

	fmt.Printf("Headers\n")

	for k, vs := range res.Header {
		fmt.Printf("\t%v = %v\n", k, strings.Join(vs, ", "))
	}

	fmt.Printf("\nNodes:\n")
	for n, t := range infos[h].Nodes {
		fmt.Printf("\t%v (%v)\n", n, t)
	}
}
