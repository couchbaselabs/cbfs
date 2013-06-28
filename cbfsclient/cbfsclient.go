package main

import (
	"github.com/couchbaselabs/cbfs/tool"
)

func main() {
	cbfstool.ToolMain(
		map[string]cbfstool.Command{
			"upload":   {-1, uploadCommand, "/src/dir /dest/dir", uploadFlags},
			"download": {-1, downloadCommand, "/src/dir /dest/dir", rmFlags},
			"ls":       {0, lsCommand, "[path]", lsFlags},
			"rm":       {0, rmCommand, "path", rmFlags},
			"info":     {0, infoCommand, "", infoFlags},
		})
}
