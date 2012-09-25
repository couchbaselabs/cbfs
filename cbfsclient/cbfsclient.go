package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

var workers = flag.Int("workers", 4, "Number of upload workers")
var revs = flag.Int("revs", 0, "Number of old revisions to keep (-1 == all)")

var commands = map[string]struct {
	nargs  int
	f      func(args []string)
	argstr string
}{
	"upload":  {-2, uploadCommand, "[opts] /src/dir http://cbfs:8484/path/"},
	"ls":      {1, lsCommand, "http://cbfs:8484/some/path"},
	"rm":      {-1, rmCommand, "[-r] [-v] http://cbfs:8484/some/path"},
	"getconf": {1, getConfCommand, "http://cbfs:8484/"},
	"setconf": {3, setConfCommand, "http://cbfs:8484/ prop value"},
}

func init() {
	log.SetFlags(log.Lmicroseconds)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			"Usage of %s [-flags] cmd cmdargs\n",
			os.Args[0])

		fmt.Fprintf(os.Stderr, "\nCommands:\n")

		for k, v := range commands {
			fmt.Fprintf(os.Stderr, "  %s %s\n", k, v.argstr)
		}

		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

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

	if flag.NArg() < 1 {
		flag.Usage()
	}

	cmdName := flag.Arg(0)
	cmd, ok := commands[cmdName]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown command: %v\n", cmdName)
		flag.Usage()
	}
	if cmd.nargs < 0 {
		reqargs := -cmd.nargs
		if flag.NArg()-1 < reqargs {
			fmt.Fprintf(os.Stderr, "Incorrect arguments for %v\n", cmdName)
			flag.Usage()
		}
	} else {
		if flag.NArg()-1 != cmd.nargs {
			fmt.Fprintf(os.Stderr, "Incorrect arguments for %v\n", cmdName)
			flag.Usage()
		}
	}

	cmd.f(flag.Args()[1:])
}
