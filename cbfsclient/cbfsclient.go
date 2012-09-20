package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbaselabs/go-couchbase"

	"github.com/couchbaselabs/cbfs/config"
)

var workers = flag.Int("workers", 4, "Number of upload workers")
var couchbaseServer = flag.String("couchbase", "", "Couchbase URL")
var couchbaseBucket = flag.String("bucket", "default", "Couchbase bucket")

var cb *couchbase.Bucket

var commands = map[string]struct {
	nargs  int
	f      func(args []string)
	argstr string
}{
	"upload":  {2, uploadCommand, "/src/dir http://cbfs:8484/path/"},
	"getconf": {0, getConfCommand, ""},
	"setconf": {2, setConfCommand, "prop value"},
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

var wg = sync.WaitGroup{}

type uploadReq struct {
	src  string
	dest string
}

func recognizeTypeByName(n, def string) string {
	byname := mime.TypeByExtension(n)
	switch {
	case byname != "":
		return byname
	case strings.HasSuffix(n, ".js"):
		return "application/javascript"
	}
	return def
}

func uploadFile(src, dest string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	someBytes := make([]byte, 512)
	n, err := f.Read(someBytes)
	if err != nil && err != io.EOF {
		return err
	}
	someBytes = someBytes[:n]
	_, err = f.Seek(0, 0)
	if err != nil {
		return err
	}

	preq, err := http.NewRequest("PUT", dest, f)
	if err != nil {
		return err
	}

	ctype := http.DetectContentType(someBytes)
	if strings.HasPrefix(ctype, "text/plain") ||
		strings.HasPrefix(ctype, "application/octet-stream") {
		ctype = recognizeTypeByName(src, ctype)
	}

	preq.Header.Set("Content-Type", ctype)

	resp, err := http.DefaultClient.Do(preq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		return fmt.Errorf("HTTP Error:  %v", resp.Status)
	}
	return nil
}

func uploader(ch chan uploadReq) {
	defer wg.Done()
	for req := range ch {
		log.Printf("%v -> %v", req.src, req.dest)
		retries := 0
		done := false
		for !done {
			err := uploadFile(req.src, req.dest)
			if err != nil {
				if retries < 3 {
					retries++
					log.Printf("Error uploading file: %v... retrying",
						err)
					time.Sleep(time.Duration(retries) * time.Second)
				} else {
					log.Fatalf("Error uploading file: %v", err)
				}
			} else {
				done = true
			}
		}
	}
}

func syncUp(src, u string, ch chan<- uploadReq) {
	err := filepath.Walk(src,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			switch info.Mode() & os.ModeType {
			case os.ModeDir:
				// ignoring quietly
			case os.ModeCharDevice, os.ModeDevice,
				os.ModeNamedPipe, os.ModeSocket, os.ModeSymlink:

				log.Printf("Ignoring special file: %v", path)
			default:
				shortPath := path[len(src):]
				ch <- uploadReq{path, u + shortPath}
			}
			return err
		})
	if err != nil {
		log.Fatalf("Traversal error: %v", err)
	}
}

func uploadCommand(args []string) {
	ch := make(chan uploadReq)

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go uploader(ch)
	}

	syncUp(args[0], args[1], ch)

	close(ch)
	wg.Wait()
}

func getConfCommand(args []string) {
	if cb == nil {
		log.Fatalf("No couchbase bucket specified")
	}
	conf := cbfsconfig.DefaultConfig()
	err := conf.RetrieveConfig(cb)
	if err != nil {
		log.Printf("Error getting config: %v", err)
		log.Printf("Using default, as shown below:")
	}

	conf.Dump(os.Stdout)
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

func setConfCommand(args []string) {
	if cb == nil {
		log.Fatalf("No couchbase bucket specified")
	}
	conf := cbfsconfig.DefaultConfig()
	err := conf.RetrieveConfig(cb)
	if err != nil {
		log.Printf("Error getting config: %v, using default", err)
	}

	switch args[0] {
	default:
		log.Fatalf("Unhandled property: %v (try running getconf)",
			args[0])
	case "gcfreq":
		conf.GCFreq = parseDuration(args[1])
	case "hash":
		conf.Hash = args[1]
	case "hbfreq":
		conf.HeartbeatFreq = parseDuration(args[1])
	case "minrepl":
		conf.MinReplicas = parseInt(args[1])
	case "cleanCount":
		conf.NodeCleanCount = parseInt(args[1])
	case "reconcileFreq":
		conf.ReconcileFreq = parseDuration(args[1])
	case "nodeCheckFreq":
		conf.StaleNodeCheckFreq = parseDuration(args[1])
	case "staleLimit":
		conf.StaleNodeLimit = parseDuration(args[1])
	}

	err = conf.StoreConfig(cb)
	if err != nil {
		log.Fatalf("Error updating config: %v", err)
	}
}

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
	}

	if *couchbaseServer != "" {
		var err error
		cb, err = couchbase.GetBucket(*couchbaseServer,
			"default", *couchbaseBucket)
		if err != nil {
			log.Fatalf("Error connecting to couchbase: %v", err)
		}
	}

	cmdName := flag.Arg(0)
	cmd, ok := commands[cmdName]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown command: %v\n", cmdName)
		flag.Usage()
	}
	if flag.NArg()-1 != cmd.nargs {
		fmt.Fprintf(os.Stderr, "Incorrect arguments for %v\n", cmdName)
		flag.Usage()
	}

	cmd.f(flag.Args()[1:])
}
