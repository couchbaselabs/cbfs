package main

import (
	"math/rand"
	"testing"
)

type randomDataMaker struct {
	src rand.Source
}

func (r *randomDataMaker) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte(r.src.Int63() & 0xff)
	}
	return len(p), nil
}

var randomData = make([]byte, 1024*1024)

func init() {
	randomSrc := randomDataMaker{rand.NewSource(1028890720402726901)}
	n, err := randomSrc.Read(randomData)
	if err != nil {
		panic(err)
	}
	if n != len(randomData) {
		panic("Didn't initialize random data properly")
	}
}

func benchHash(h string, b *testing.B) {
	*hashType = h
	b.SetBytes(int64(len(randomData)))
	for i := 0; i < b.N; i++ {
		sh := getHash()
		written, err := sh.Write(randomData)
		if err != nil {
			b.Fatalf("Error writing data: %v", err)
		}
		if written != len(randomData) {
			b.Fatalf("Didn't write the correct amount of data: %v != %v",
				written, len(randomData))
		}
	}
}

func BenchmarkHashSHA1(b *testing.B) {
	benchHash("sha1", b)
}

func BenchmarkHashSHA256(b *testing.B) {
	benchHash("sha256", b)
}

func BenchmarkHashSHA512(b *testing.B) {
	benchHash("sha512", b)
}

func BenchmarkHashMD5(b *testing.B) {
	benchHash("md5", b)
}
