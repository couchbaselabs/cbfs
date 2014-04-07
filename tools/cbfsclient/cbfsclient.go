package main

import (
	"github.com/couchbaselabs/cbfs/tools"
)

func main() {
	cbfstool.ToolMain(
		map[string]cbfstool.Command{
			"upload":   {2, uploadCommand, "/src/dir /dest/dir", uploadFlags},
			"download": {-1, downloadCommand, "/src/dir /dest/dir", dlFlags},
			"find":     {1, findCommand, "/src/dir", findFlags},
			"ls":       {0, lsCommand, "[path]", lsFlags},
			"rm":       {-1, rmCommand, "path", rmFlags},
			"info":     {0, infoCommand, "", infoFlags},
			"fileinfo": {1, fileInfoCommand, "path", fileInfoFlags},
		})
}
