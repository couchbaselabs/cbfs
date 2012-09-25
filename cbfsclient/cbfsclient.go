package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"
)

var commands = map[string]struct {
	nargs  int
	f      func(url string, args []string)
	argstr string
}{
	"upload":  {-1, uploadCommand, "/src/dir /dest/dir"},
	"ls":      {0, lsCommand, "[path]"},
	"rm":      {0, rmCommand, "path"},
	"getconf": {0, getConfCommand, ""},
	"setconf": {2, setConfCommand, "prop value"},
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

		fmt.Fprintf(os.Stderr, "\nls:\n")
		lsFlags.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nrm:\n")
		rmFlags.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nupload:\n")
		uploadFlags.PrintDefaults()
		os.Exit(1)
	}

}

func relativeUrl(u, path string) string {
	du, err := url.Parse(u)
	if err != nil {
		log.Fatalf("Error parsing url: %v", err)
	}

	du.Path = path
	if du.Path[0] != '/' {
		du.Path = "/" + du.Path
	}

	return du.String()
}

func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		log.Fatalf("Unable to parse duration: %v", err)
	}
	return d
}

func parseInt(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("Error parsing int: %v", err)
	}
	return i
}

func main() {
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
	}

	u := flag.Arg(0)

	cmdName := flag.Arg(1)
	cmd, ok := commands[cmdName]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown command: %v\n", cmdName)
		flag.Usage()
	}
	if cmd.nargs == 0 {
	} else if cmd.nargs < 0 {
		reqargs := -cmd.nargs
		if flag.NArg()-2 < reqargs {
			fmt.Fprintf(os.Stderr, "Incorrect arguments for %v\n", cmdName)
			flag.Usage()
		}
	} else {
		if flag.NArg()-2 != cmd.nargs {
			fmt.Fprintf(os.Stderr, "Incorrect arguments for %v\n", cmdName)
			flag.Usage()
		}
	}

	cmd.f(u, flag.Args()[2:])
}
