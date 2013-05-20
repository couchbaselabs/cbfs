package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"text/template"
	"time"

	"github.com/couchbaselabs/cbfs/config"
	"sync"
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
	Version   string
}

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

type Nodes map[string]StorageNode

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
		if *infoTemplateFile == "" {
			tmplstr = defaultInfoTemplate
		} else {
			td, err := ioutil.ReadFile(*infoTemplateFile)
			if err != nil {
				log.Fatalf("Error reading template file: %v", err)
			}
			tmplstr = string(td)
		}
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
			err = getJsonData(s, to)
			if err != nil {
				log.Fatalf("Error getting node info: %v", err)
			}
		}(u.String(), v)
	}

	wg.Wait()

	if *infoJSON {
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			log.Fatalf("Error marshaling rseult: %v", err)
		}
		os.Stdout.Write(data)
	} else {
		err = tmpl.Execute(os.Stdout, result)
		if err != nil {
			log.Fatalf("Error executing template: %v", err)
		}
	}
}
