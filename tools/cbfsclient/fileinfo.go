package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/couchbaselabs/cbfs/client"
	"github.com/couchbaselabs/cbfs/tools"
)

var fileInfoFlags = flag.NewFlagSet("fileInfo", flag.ExitOnError)
var fileInfoTemplate = fileInfoFlags.String("t", "", "Display template")
var fileInfoTemplateFile = fileInfoFlags.String("T", "", "Display template filename")

const defaultFileInfoTemplate = `File: {{.Filename}}

Headers:
{{range $k, $v := .Header}}    {{$k}} = {{$v | join ","}}
{{end}}
Nodes:
{{range $n, $t := .Nodes}}    {{$n}} ({{$t}})
{{end}}`

func fileInfoCommand(base string, args []string) {
	fileInfoFlags.Parse(args)

	tmpl := cbfstool.GetTemplate(*fileInfoTemplate, *fileInfoTemplateFile,
		defaultFileInfoTemplate)

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

	err = tmpl.Execute(os.Stdout, map[string]interface{}{
		"Filename": u.Path[1:],
		"Header":   res.Header,
		"Nodes":    infos[h].Nodes,
	})
	cbfstool.MaybeFatal(err, "Error executing template: %v", err)
}
