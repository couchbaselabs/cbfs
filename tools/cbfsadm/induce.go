package main

import (
	"log"
	"net/http"
	"net/url"
	"os"
	"text/template"

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

func induceTask(ustr, taskname string) {
	u, err := url.Parse(ustr)
	cbfstool.MaybeFatal(err, "Error parsing URL: %v", err)

	u.Path = "/.cbfs/tasks/" + taskname

	res, err := http.PostForm(u.String(), nil)
	cbfstool.MaybeFatal(err, "Error inducing %v: %v", taskname, err)

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		log.Fatalf("Error inducing %v: %v", taskname, res.Status)
	}
}

func listTasks(ustr string) {
	u, err := url.Parse(ustr)
	cbfstool.MaybeFatal(err, "Error parsing URL: %v", err)

	u.Path = "/.cbfs/tasks/info/"

	d := struct {
		Global map[string][]string `json:"global"`
		Local  map[string][]string `json:"local"`
	}{make(map[string][]string), make(map[string][]string)}

	err = cbfstool.GetJsonData(u.String(), &d)
	cbfstool.MaybeFatal(err, "Error getting task info: %v", err)

	tasksTmpl.Execute(os.Stdout, d)
}

func induceCommand(ustr string, args []string) {
	if len(args) < 1 {
		listTasks(ustr)
	} else {
		induceTask(ustr, args[0])
	}
}
