package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

var commands = map[string]struct {
	nargs  int
	f      func(url string, args []string)
	argstr string
}{
	"getconf": {0, getConfCommand, ""},
	"setconf": {2, setConfCommand, "prop value"},
	"fsck":    {0, fsckCommand, ""},
	"backup":  {-1, backupCommand, "filename"},
	"rmbak":   {0, rmBakCommand, ""},
	"restore": {-1, restoreCommand, "filename"},
	"induce":  {0, induceCommand, "taskname"},
}

func init() {
	log.SetFlags(log.Lmicroseconds)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			"Usage:\n  %s http://cbfs:8484/ cmd [-opts] cmdargs\n",
			os.Args[0])

		fmt.Fprintf(os.Stderr, "\nCommands:\n")

		for k, v := range commands {
			fmt.Fprintf(os.Stderr, "  %s %s\n", k, v.argstr)
		}

		fmt.Fprintf(os.Stderr, "\n---- Subcommand Options ----\n")

		fmt.Fprintf(os.Stderr, "\nfsck:\n")
		fsckFlags.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nbackup <filename>:\n")
		backupFlags.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nrestore <filename>:\n")
		restoreFlags.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nrmbak:\n")
		rmbakFlags.PrintDefaults()
		os.Exit(1)
	}

}

func maybeFatal(err error, msg string, args ...interface{}) {
	if err != nil {
		log.Fatalf(msg, args...)
	}
}

func relativeUrl(u, path string) string {
	du, err := url.Parse(u)
	maybeFatal(err, "Error parsing url: %v", err)

	du.Path = path
	if du.Path[0] != '/' {
		du.Path = "/" + du.Path
	}

	return du.String()
}

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

func verbose(v bool, f string, a ...interface{}) {
	if v {
		log.Printf(f, a...)
	}
}

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
	}

	off := 0
	u := "http://cbfs:8484/"

	if strings.HasPrefix(flag.Arg(0), "http://") {
		u = flag.Arg(0)
		off++
	}

	cmdName := flag.Arg(off)
	cmd, ok := commands[cmdName]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown command: %v\n", cmdName)
		flag.Usage()
	}
	if cmd.nargs == 0 {
	} else if cmd.nargs < 0 {
		reqargs := -cmd.nargs
		if flag.NArg()-1-off < reqargs {
			fmt.Fprintf(os.Stderr, "Incorrect arguments for %v\n", cmdName)
			flag.Usage()
		}
	} else {
		if flag.NArg()-1-off != cmd.nargs {
			fmt.Fprintf(os.Stderr, "Incorrect arguments for %v\n", cmdName)
			flag.Usage()
		}
	}

	cmd.f(u, flag.Args()[off+1:])
}
