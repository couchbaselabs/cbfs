package cbfsclient

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// Options for storing data.
type PutOptions struct {
	// If true, do a fast, unsafe store
	Unsafe bool
	// Expiration time
	Expiration int
	// Hash to verify ("" for no verification)
	Hash string
	// Content type (detected if not specified)
	ContentType string
	// Optional reader transform (e.g. for encryption)
	ContentTransform func(r io.Reader) io.Reader

	keeprevs   int
	keeprevset bool
}

// Specify the number of revs to keep of this object.
func (p *PutOptions) SetKeepRevs(to int) {
	p.keeprevs = to
	p.keeprevset = true
}

func recognizeTypeByName(n, def string) string {
	byname := mime.TypeByExtension(n)
	switch {
	case byname != "":
		return byname
	case strings.HasSuffix(n, ".js"):
		return "application/javascript"
	case strings.HasSuffix(n, ".json"):
		return "application/json"
	case strings.HasSuffix(n, ".css"):
		return "text/css"
	}
	return def
}

// Put some content in CBFS.
//
// The sourcename is optional and is used for content type detection
// (if not specified in options).
//
// The dest filename is the name that the file will have in cbfs.
//
// The io.Reader is the source of content.
//
// Options are optional.
func (c Client) Put(srcname, dest string, r io.Reader, opts PutOptions) error {
	someBytes := make([]byte, 512)
	n, err := r.Read(someBytes)
	if err != nil && err != io.EOF {
		return err
	}
	someBytes = someBytes[:n]

	length := int64(-1)
	if s, ok := r.(io.Seeker); r != os.Stdin && ok {
		length, err = s.Seek(0, 2)
		if err != nil {
			return err
		}

		_, err = s.Seek(0, 0)
		if err != nil {
			return err
		}
	} else {
		r = io.MultiReader(bytes.NewReader(someBytes), r)
	}

	if opts.ContentTransform != nil {
		oldr := r
		r = opts.ContentTransform(r)
		// On a content transformation, we don't know the
		// length.
		if oldr != r {
			length = -1
		}
	}

	_, rn, err := c.RandomNode()
	if err != nil {
		return err
	}

	du := rn.URLFor(dest)
	preq, err := http.NewRequest("PUT", du, r)
	if err != nil {
		return err
	}
	if opts.keeprevset {
		preq.Header.Set("X-CBFS-KeepRevs",
			strconv.Itoa(opts.keeprevs))
	}
	if opts.Unsafe {
		preq.Header.Set("X-CBFS-Unsafe", "true")
	}
	if opts.Expiration > 0 {
		preq.Header.Set("X-CBFS-Expiration",
			strconv.Itoa(opts.Expiration))
	}

	ctype := opts.ContentType
	if ctype == "" {
		ctype := http.DetectContentType(someBytes)
		if strings.HasPrefix(ctype, "text/plain") ||
			strings.HasPrefix(ctype, "application/octet-stream") {
			ctype = recognizeTypeByName(srcname, ctype)
		}
	}

	if length >= 0 {
		preq.Header.Set("Content-Length", strconv.FormatInt(length, 10))
	}
	preq.Header.Set("Content-Type", ctype)
	if opts.Hash != "" {
		preq.Header.Set("X-CBFS-Hash", opts.Hash)
	}

	resp, err := http.DefaultClient.Do(preq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		r, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("HTTP Error:  %v: %s", resp.Status, r)
	}

	return nil
}
