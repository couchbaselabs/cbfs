package main

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var ignorePatterns = []string{}

func loadIgnorePatternsFromFile(fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer f.Close()

	return loadIgnorePatterns(f)
}

func loadIgnorePatterns(r io.Reader) error {
	b := bufio.NewReader(r)

	for {
		line, err := b.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}

		line = strings.TrimSpace(line)
		switch {
		case len(line) == 0, line[0] == '#':
			// ignore
		default:
			_, err = filepath.Match(line, "")
			if err != nil {
				return err
			}
			ignorePatterns = append(ignorePatterns, line)
		}
	}
}

func isIgnored(input string) bool {
	if input[0] == '/' {
		input = input[1:]
	}
	b := filepath.Base(input)
	for _, pat := range ignorePatterns {
		in := b
		if pat[0] == '/' {
			in = input
			pat = pat[1:]
		}
		matched, err := filepath.Match(pat, in)
		// The pattern was checked at load time.
		maybeFatal(err, "Error processing match %v: %v", pat, err)
		if matched {
			return true
		}
	}
	return false
}
