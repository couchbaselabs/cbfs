package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/couchbaselabs/cbfs/client"
	"github.com/couchbaselabs/cbfs/tool"
	"github.com/dustin/go-humanize"
)

var lsFlags = flag.NewFlagSet("ls", flag.ExitOnError)
var lsDashL = lsFlags.Bool("l", false, "Display detailed listing")

func lsCommand(u string, args []string) {
	lsFlags.Parse(args)

	client, err := cbfsclient.New(u)
	cbfstool.MaybeFatal(err, "Error creating client: %v", err)

	result, err := client.List(lsFlags.Arg(0))
	cbfstool.MaybeFatal(err, "Error listing directory: %v", err)

	dirnames := sort.StringSlice{}
	filenames := sort.StringSlice{}
	for k := range result.Dirs {
		dirnames = append(dirnames, k)
	}
	for k := range result.Files {
		filenames = append(filenames, k)
	}
	dirnames.Sort()
	filenames.Sort()

	if *lsDashL {
		totalFiles := 0
		totalSize := uint64(0)
		tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		for i := range dirnames {
			dn := dirnames[i]
			di := result.Dirs[dn]
			fmt.Fprintf(tw, "d %8s\t%s\t(%s descendants)\n",
				humanize.Bytes(uint64(di.Size)), dn,
				humanize.Comma(int64(di.Descendants)))
			totalSize += uint64(di.Size)
			totalFiles += di.Descendants
		}
		for i := range filenames {
			fn := filenames[i]
			fi := result.Files[fn]
			fmt.Fprintf(tw, "f %8s\t%s\t%s\n",
				humanize.Bytes(uint64(fi.Length)), fn,
				fi.Headers.Get("Content-Type"))

			totalSize += uint64(fi.Length)
			totalFiles++
		}
		fmt.Fprintf(tw, "----------------------------------------\n")
		fmt.Fprintf(tw, "Tot: %s\t\t%s files\n",
			humanize.Bytes(totalSize),
			humanize.Comma(int64(totalFiles)))
		tw.Flush()
	} else {
		allnames := sort.StringSlice{}
		for i := range dirnames {
			allnames = append(allnames, dirnames[i])
		}
		for i := range filenames {
			allnames = append(allnames, filenames[i])
		}
		allnames.Sort()
		for _, a := range allnames {
			fmt.Println(a)
		}
	}
}
