package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"text/template"
	"time"

	"github.com/couchbaselabs/cbfs/config"
)

type StorageNode struct {
	Addr      string
	AddrRaw   string    `json:"addr_raw"`
	Started   time.Time `json:"starttime"`
	HBTime    time.Time `json:"hbtime"`
	BindAddr  string
	FrameBind string
	HBAgeStr  string `json:"hbage_str"`
	Used      int64
	Free      int64
	Size      int64
	UptimeStr string `json:"uptime_str"`
}

type Tasks map[string]interface{}

type Nodes map[string]StorageNode

var infoFlags = flag.NewFlagSet("info", flag.ExitOnError)
var infoTemplate = infoFlags.String("t", "", "Display template")

const defaultInfoTemplate = `nodes:
{{ range $name, $nodeinfo := .Nodes }}  {{$name}} up {{$nodeinfo.HBAgeStr}}
{{ end }}
{{if .Tasks}}tasks:{{end}}{{ range $node, $tasks := .Tasks }}
  {{$node}}
  {{ range $task, $info := $tasks }}    {{$task}} - {{$info.state}} - {{$info.ts}}
  {{end}}{{end}}
config:
{{ range $k, $v := .Conf.ToMap}}  {{$k}}: {{$v}}
{{end}}
`

func getJsonData(u string, into interface{}) error {
	res, err := http.Get(u)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("HTTP Error: %v", res.Status)
	}

	d := json.NewDecoder(res.Body)
	return d.Decode(into)
}

func infoCommand(base string, args []string) {
	infoFlags.Parse(args)

	tmplstr := *infoTemplate
	if tmplstr == "" {
		tmplstr = defaultInfoTemplate
	}

	tmpl, err := template.New("").Parse(tmplstr)
	if err != nil {
		log.Fatalf("Error parsing template: %v", err)
	}

	u, err := url.Parse(base)
	if err != nil {
		log.Fatalf("Error parsing URL: %v", err)
	}

	result := struct {
		Nodes Nodes
		Tasks Tasks
		Conf  cbfsconfig.CBFSConfig
	}{}

	todo := map[string]interface{}{
		"/.cbfs/nodes/":  &result.Nodes,
		"/.cbfs/tasks/":  &result.Tasks,
		"/.cbfs/config/": &result.Conf,
	}

	for k, v := range todo {
		u.Path = k
		err = getJsonData(u.String(), v)
		if err != nil {
			log.Fatalf("Error getting node info: %v", err)
		}
	}

	err = tmpl.Execute(os.Stdout, result)
	if err != nil {
		log.Fatalf("Error executing template: %v", err)
	}
}
