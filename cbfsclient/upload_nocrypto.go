// +build nocrypto

package main

import (
	"io"
)

func maybeCrypt(r io.Reader) io.Reader {
	return r
}

func initCrypto() {
}
