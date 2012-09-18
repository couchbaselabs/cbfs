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
	"strings"
	"sync"
)

var workers = flag.Int("workers", 4, "Number of upload workers")

var commands = map[string]struct {
	nargs  int
	f      func(args []string)
	argstr string
}{
	"upload": {2, uploadCommand, "/src/dir http://cbfs:8484/path/"},
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
		byname := mime.TypeByExtension(src)
		if byname != "" {
			ctype = byname
		}
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

		dest := req.dest
		if strings.HasSuffix(dest, "/index.html") {
			dest = dest[:len(dest)-len("index.html")]
		}

		log.Printf("%v -> %v", req.src, dest)
		err := uploadFile(req.src, dest)
		if err != nil {
			log.Fatalf("Error uploading file: %v", err)
		}
	}
}

func syncUp(src, u string, ch chan<- uploadReq) {
	err := filepath.Walk(src,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
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
	if flag.NArg()-1 != cmd.nargs {
		fmt.Fprintf(os.Stderr, "Incorrect arguments for %v\n", cmdName)
		flag.Usage()
	}

	cmd.f(flag.Args()[1:])
}
