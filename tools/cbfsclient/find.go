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
var findDashName = findFlags.String("name", "", "Glob name to match")

const defaultFindTemplate = `{{.Name}}
`

type dirAndFileMatcher struct {
	m map[string]struct{}
}

func newDirAndFileMatcher() dirAndFileMatcher {
	return dirAndFileMatcher{map[string]struct{}{}}
}

type findMatch struct {
	path  string
	isDir bool
}

func (d dirAndFileMatcher) match(name string) bool {
	matched, err := filepath.Match(*findDashName, name)
	if err != nil {
		log.Fatalf("Error globbing: %v", err)
	}
	return matched
}

func (d dirAndFileMatcher) matches(name string) []findMatch {
	if *findDashName == "" {
		return []findMatch{{name, false}}
	}
	var matches []findMatch

	dir := filepath.Dir(name)
	for dir != "." {
		if _, seen := d.m[dir]; !seen {
			matched := d.match(filepath.Base(dir))
			d.m[dir] = struct{}{}
			if matched {
				matches = append(matches, findMatch{dir, true})
			}
		}
		dir = filepath.Dir(dir)
	}
	// Reverse these so the traversal order makes sense
	for i := 0; i < len(matches)/2; i++ {
		j := len(matches) - i - 1
		matches[i], matches[j] = matches[j], matches[i]
	}

	if d.match(filepath.Base(name)) {
		matches = append(matches, findMatch{name, false})
	}
	return matches
}

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

	matcher := newDirAndFileMatcher()
	for fn, inf := range things.Files {
		fn = fn[len(src)+1:]
		for _, match := range matcher.matches(fn) {
			if err := tmpl.Execute(os.Stdout, struct {
				Name  string
				IsDir bool
				Meta  cbfsclient.FileMeta
			}{match.path, match.isDir, inf}); err != nil {
				log.Fatalf("Error executing template: %v", err)
			}
		}
	}
}
