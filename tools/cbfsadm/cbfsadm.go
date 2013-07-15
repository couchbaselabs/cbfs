package main

import (
	"github.com/couchbaselabs/cbfs/tools"
)

func main() {
	cbfstool.ToolMain(
		map[string]cbfstool.Command{
			"getconf": {0, getConfCommand, "", nil},
			"setconf": {2, setConfCommand, "prop value", nil},
			"fsck":    {0, fsckCommand, "", fsckFlags},
			"backup":  {1, backupCommand, "filename", backupFlags},
			"rmbak":   {0, rmBakCommand, "", rmbakFlags},
			"restore": {1, restoreCommand, "filename", restoreFlags},
			"induce":  {0, induceCommand, "taskname", induceFlags},
			"lsbak":   {0, lsBakCommand, "", nil},
		})
}
