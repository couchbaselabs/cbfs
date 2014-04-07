package main

import (
	"flag"
	"log"
	"os"

	"github.com/couchbaselabs/cbfs/client"
	"github.com/couchbaselabs/cbfs/tools"
	"github.com/dustin/httputil"
)

var findFlags = flag.NewFlagSet("find", flag.ExitOnError)
var findTemplate = findFlags.String("t", "", "Display template")
var findTemplateFile = findFlags.String("T", "", "Display template filename")

const defaultFindTemplate = `{{.Name}}
`

func findCommand(u string, args []string) {
	src := findFlags.Arg(0)
	for src[len(src)-1] == '/' {
		src = src[:len(src)-1]
	}

	tmpl := cbfstool.GetTemplate(*findTemplate, *findTemplateFile,
		defaultFindTemplate)

	httputil.InitHTTPTracker(false)

	client, err := cbfsclient.New(u)
	cbfstool.MaybeFatal(err, "Can't build a client: %v", err)

	things, err := client.ListDepth(src, 4096)
	cbfstool.MaybeFatal(err, "Can't list things: %v", err)

	for fn, inf := range things.Files {
		fn = fn[len(src)+1:]
		if err := tmpl.Execute(os.Stdout, struct {
			Name string
			Meta cbfsclient.FileMeta
		}{fn, inf}); err != nil {
			log.Fatalf("Error executing template: %v", err)
		}
	}
}
