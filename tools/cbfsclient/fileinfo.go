package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"text/template"

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

	tmplstr := *fileInfoTemplate
	if tmplstr == "" {
		switch *fileInfoTemplateFile {
		case "":
			tmplstr = defaultFileInfoTemplate
		case "-":
			td, err := ioutil.ReadAll(os.Stdin)
			cbfstool.MaybeFatal(err, "Error reading template from stdin: %v", err)
			tmplstr = string(td)
		default:
			td, err := ioutil.ReadFile(*fileInfoTemplateFile)
			cbfstool.MaybeFatal(err, "Error reading template file: %v", err)
			tmplstr = string(td)
		}
	}

	tmpl, err := template.New("").Funcs(template.FuncMap{
		"join": func(o string, s []string) string {
			return strings.Join(s, o)
		},
	}).Parse(tmplstr)
	cbfstool.MaybeFatal(err, "Error parsing template: %v", err)

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
