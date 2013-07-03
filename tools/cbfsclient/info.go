package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/url"
	"os"
	"sync"
	"text/template"
	"time"

	"github.com/couchbaselabs/cbfs/client"
	"github.com/couchbaselabs/cbfs/config"
	"github.com/couchbaselabs/cbfs/tools"
)

type Tasks map[string]map[string]struct {
	State string
	TS    time.Time
}

type Backup struct {
	Filename string
	OID      string
	When     time.Time
	Conf     cbfsconfig.CBFSConfig
}

type Nodes map[string]cbfsclient.StorageNode

var infoFlags = flag.NewFlagSet("info", flag.ExitOnError)
var infoTemplate = infoFlags.String("t", "", "Display template")
var infoTemplateFile = infoFlags.String("T", "", "Display template filename")
var infoJSON = infoFlags.Bool("json", false, "Dump as json")

const defaultInfoTemplate = `nodes:
{{ range $name, $info := .Nodes }}  {{$name}} {{$info.Version}} up {{$info.UptimeStr}} (age: {{$info.HBAgeStr}})
{{ end }}
{{if .Tasks}}tasks:{{end}}{{ range $node, $tasks := .Tasks }}
  {{$node}}
  {{ range $task, $info := $tasks }}    {{$task}} - {{$info.State}} - {{$info.TS}}
  {{end}}{{end}}
backups:
  Found {{len .Backups.Previous }} backups.
  Current:  {{.Backups.Latest.Filename}} ({{.Backups.Latest.OID}})
            as of {{.Backups.Latest.When}}

config:
{{ range $k, $v := .Conf.ToMap}}  {{$k}}: {{$v}}
{{end}}
`

func infoCommand(base string, args []string) {
	infoFlags.Parse(args)

	tmplstr := *infoTemplate
	if tmplstr == "" {
		switch *infoTemplateFile {
		case "":
			tmplstr = defaultInfoTemplate
		case "-":
			td, err := ioutil.ReadAll(os.Stdin)
			cbfstool.MaybeFatal(err, "Error reading template from stdin: %v", err)
			tmplstr = string(td)
		default:
			td, err := ioutil.ReadFile(*infoTemplateFile)
			cbfstool.MaybeFatal(err, "Error reading template file: %v", err)
			tmplstr = string(td)
		}
	}

	tmpl, err := template.New("").Parse(tmplstr)
	cbfstool.MaybeFatal(err, "Error parsing template: %v", err)

	u, err := url.Parse(base)
	cbfstool.MaybeFatal(err, "Error parsing URL: %v", err)

	result := struct {
		Nodes   Nodes
		Tasks   Tasks
		Backups struct {
			Previous []Backup `json:"backups"`
			Latest   Backup
		}
		Conf cbfsconfig.CBFSConfig
	}{}

	todo := map[string]interface{}{
		"/.cbfs/nodes/":  &result.Nodes,
		"/.cbfs/tasks/":  &result.Tasks,
		"/.cbfs/backup/": &result.Backups,
		"/.cbfs/config/": &result.Conf,
	}

	wg := sync.WaitGroup{}

	for k, v := range todo {
		u.Path = k
		wg.Add(1)
		go func(s string, to interface{}) {
			defer wg.Done()
			err = cbfstool.GetJsonData(s, to)
			cbfstool.MaybeFatal(err, "Error getting node info: %v", err)
		}(u.String(), v)
	}

	wg.Wait()

	if *infoJSON {
		data, err := json.MarshalIndent(result, "", "  ")
		cbfstool.MaybeFatal(err, "Error marshaling rseult: %v", err)
		os.Stdout.Write(data)
	} else {
		err = tmpl.Execute(os.Stdout, result)
		cbfstool.MaybeFatal(err, "Error executing template: %v", err)
	}
}
