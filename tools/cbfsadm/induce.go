package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"text/template"

	"github.com/couchbaselabs/cbfs/client"
	"github.com/couchbaselabs/cbfs/tools"
)

const tasksTmplText = `Which task would you like to induce?

Global Tasks:
{{range $k, $v := .Global}}   - {{$k}}
{{end}}
Local Tasks:
{{range $k, $v := .Local}}   - {{$k}}
{{end}}
`

var tasksTmpl = template.Must(template.New("").Parse(tasksTmplText))

var induceFlags = flag.NewFlagSet("induce", flag.ExitOnError)
var induceAll = induceFlags.Bool("all", false, "induce on all nodes")

func induceTask(ustr, taskname string) error {
	u := cbfstool.ParseURL(ustr)
	u.Path = "/.cbfs/tasks/" + taskname

	res, err := http.PostForm(u.String(), nil)
	if err != nil {
		return err
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("HTTP error: %v", res.Status)
	}
	return nil
}

func induceTaskAll(base, taskname string) {
	c, err := cbfsclient.New(base)
	cbfstool.MaybeFatal(err, "Error getting client: %v", err)

	errs := 0
	nodes, err := c.Nodes()
	cbfstool.MaybeFatal(err, "Error getting nodes: %v", err)

	for name, n := range nodes {
		err := induceTask(n.URLFor("/"), taskname)
		if err != nil {
			log.Printf("Error on node %v: %v", name, err)
			errs++
		}
	}
	if errs != 0 {
		log.Fatalf("There were errors.")
	}
}

func listTasks(ustr string) {
	u := cbfstool.ParseURL(ustr)
	u.Path = "/.cbfs/tasks/info/"

	d := struct {
		Global map[string][]string `json:"global"`
		Local  map[string][]string `json:"local"`
	}{make(map[string][]string), make(map[string][]string)}

	err := cbfstool.GetJsonData(u.String(), &d)
	cbfstool.MaybeFatal(err, "Error getting task info: %v", err)

	tasksTmpl.Execute(os.Stdout, d)
}

func induceCommand(ustr string, args []string) {
	induceFlags.Parse(args)

	if induceFlags.NArg() < 1 {
		listTasks(ustr)
	} else {
		taskname := induceFlags.Arg(0)
		if *induceAll {
			induceTaskAll(ustr, taskname)
		} else {
			err := induceTask(ustr, taskname)
			cbfstool.MaybeFatal(err, "Error inducing %v", taskname)
		}
	}
}
