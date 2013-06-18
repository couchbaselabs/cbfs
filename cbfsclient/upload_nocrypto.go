// +build nocrypto

package main

import (
	"io"
)

func maybeCrypt(r io.ReadCloser) io.ReadCloser {
	return r
}

func initCrypto() {
}
