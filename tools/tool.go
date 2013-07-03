// Support for CLI tools.
package cbfstool

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
)

type Command struct {
	Nargs  int
	F      func(url string, args []string)
	Argstr string
	Flags  *flag.FlagSet
}

func (c Command) Usage(name string) {
	fmt.Fprintf(os.Stderr, "Usage:  %s %s\n", name, c.Argstr)
	if c.Flags != nil {
		os.Stderr.Write([]byte{'\n'})
		c.Flags.PrintDefaults()
	}
	os.Exit(64)
}

func setUsage(commands map[string]Command) {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr,
			"Usage:\n  %s [http://cbfs:8484/] cmd [-opts] cmdargs\n",
			os.Args[0])

		fmt.Fprintf(os.Stderr, "\nCommands:\n")

		ss := sort.StringSlice{}
		for k := range commands {
			ss = append(ss, k)
		}
		ss.Sort()

		for _, k := range ss {
			fmt.Fprintf(os.Stderr, "  %s %s\n", k, commands[k].Argstr)
		}

		fmt.Fprintf(os.Stderr, "\n---- Subcommand Options ----\n")

		for _, k := range ss {
			if commands[k].Flags != nil {
				fmt.Fprintf(os.Stderr, "\n%s:\n", k)
				commands[k].Flags.PrintDefaults()
			}
		}

		os.Exit(1)
	}
}

func GetJsonData(u string, into interface{}) error {
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

func MaybeFatal(err error, msg string, args ...interface{}) {
	if err != nil {
		log.Fatalf(msg, args...)
	}
}

func Verbose(v bool, f string, a ...interface{}) {
	if v {
		log.Printf(f, a...)
	}
}

func ToolMain(commands map[string]Command) {
	log.SetFlags(log.Lmicroseconds)

	setUsage(commands)

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
	if cmd.Nargs == 0 {
	} else if cmd.Nargs < 0 {
		reqargs := -cmd.Nargs
		if flag.NArg()-1-off < reqargs {
			cmd.Usage(cmdName)
		}
	} else {
		if flag.NArg()-1-off != cmd.Nargs {
			cmd.Usage(cmdName)
		}
	}

	cmd.F(u, flag.Args()[off+1:])
}
