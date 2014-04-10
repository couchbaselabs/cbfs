package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/couchbaselabs/cbfs/client"
	"github.com/couchbaselabs/cbfs/tools"
	"github.com/dustin/httputil"
)

var findFlags = flag.NewFlagSet("find", flag.ExitOnError)
var findTemplate = findFlags.String("t", "", "Display template")
var findTemplateFile = findFlags.String("T", "", "Display template filename")
var findDashName = findFlags.String("name", "*", "Glob name to match")

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

	matchedDirs := map[string]struct{}{}
	for fn, inf := range things.Files {
		fn = fn[len(src)+1:]
		dir := filepath.Dir(fn)
		matched := false
		if _, seen := matchedDirs[dir]; !seen {
			matched, err = filepath.Match(*findDashName,
				filepath.Base(dir))
			if err != nil {
				log.Fatalf("Error globbing: %v", err)
			}
			matchedDirs[dir] = struct{}{}
			if matched {
				if err := tmpl.Execute(os.Stdout, struct {
					Name  string
					IsDir bool
					Meta  cbfsclient.FileMeta
				}{Name: dir, IsDir: true}); err != nil {
					log.Fatalf("Error executing template: %v", err)
				}
				continue
			}
		}
		if !matched {
			matched, err = filepath.Match(*findDashName,
				filepath.Base(fn))
			if err != nil {
				log.Fatalf("Error globbing: %v", err)
			}
		}
		if matched {
			if err := tmpl.Execute(os.Stdout, struct {
				Name  string
				IsDir bool
				Meta  cbfsclient.FileMeta
			}{fn, false, inf}); err != nil {
				log.Fatalf("Error executing template: %v", err)
			}
		}
	}
}
