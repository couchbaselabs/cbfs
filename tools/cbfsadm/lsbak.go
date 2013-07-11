package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/couchbaselabs/cbfs/tools"
)

func lsBakCommand(ustr string, args []string) {
	u := cbfstool.ParseURL(ustr)
	u.Path = "/.cbfs/backup/"

	backups := struct {
		Previous []Backup `json:"backups"`
	}{}
	err := cbfstool.GetJsonData(u.String(), &backups)
	cbfstool.MaybeFatal(err, "Error getting backup info: %v", err)

	tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	for _, b := range backups.Previous {
		fmt.Fprintf(tw, "%s\t%v\n", b.Filename, b.When)
	}
	tw.Flush()
}
