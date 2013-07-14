package main

import (
	"flag"
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

	client, err := cbfsclient.New(base)
	cbfstool.MaybeFatal(err, "Error getting client: %v", err)

	fh, err := client.OpenFile(args[0])
	cbfstool.MaybeFatal(err, "Error getting file info: %v", err)

	err = tmpl.Execute(os.Stdout, map[string]interface{}{
		"Filename": u.Path[1:],
		"Header":   fh.Header(),
		"Nodes":    fh.Nodes(),
	})
	cbfstool.MaybeFatal(err, "Error executing template: %v", err)
}
