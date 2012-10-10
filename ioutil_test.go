package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"reflect"
	"testing"
	"testing/iotest"
	"time"
)

const expSize = 64 * 1024

func TestMultiReader(t *testing.T) {
	t.Parallel()

	randomSrc := randomDataMaker{rand.NewSource(1028890720402726901)}
	lr := io.LimitReader(&randomSrc, expSize)

	r1, r2 := newMultiReader(lr)

	b1 := &bytes.Buffer{}
	b2 := &bytes.Buffer{}

	rs := make(chan copyRes, 2)

	go bgCopy(b1, r1, rs)
	go bgCopy(b2, r2, rs)

	res1 := <-rs
	res2 := <-rs

	if res1.e != nil || res2.e != nil {
		t.Logf("Error copying data: %v/%v", res1.e, res2.e)
	}

	if res1.s != res2.s || res1.s != expSize {
		t.Fatalf("Read %v/%v bytes, expected %v",
			res1.s, res2.s, expSize)
	}

	if !reflect.DeepEqual(b1, b2) {
		t.Fatalf("Didn't read the same data from the two things")
	}
}

func TestMultiReaderSourceError(t *testing.T) {
	t.Parallel()

	// This test fails if it doesn't complete quickly.
	timer := time.AfterFunc(2*time.Second, func() {
		t.Fatalf("Test seems to have hung.")
	})
	defer timer.Stop()

	randomSrc := randomDataMaker{rand.NewSource(1028890720402726901)}
	tordr := iotest.TimeoutReader(&randomSrc)

	lr := io.LimitReader(tordr, expSize)

	r1, _ := newMultiReaderTimeout(lr, 10*time.Millisecond)

	b1 := &bytes.Buffer{}

	rs := make(chan copyRes, 2)

	go bgCopy(b1, r1, rs)

	res1 := <-rs

	if res1.e != Timeout {
		t.Errorf("Expected a timeout, got %v", res1.e)
		t.Fail()
	}
}

func BenchmarkRandomDataMaker(b *testing.B) {
	randomSrc := randomDataMaker{rand.NewSource(1028890720402726901)}
	for i := 0; i < b.N; i++ {
		b.SetBytes(int64(i))
		copied, err := io.CopyN(ioutil.Discard, &randomSrc, int64(i))
		if err != nil {
			b.Fatalf("Error copying at %v: %v", i, err)
		}
		if copied != int64(i) {
			b.Fatalf("Didn't copy enough stuff: %v", copied)
		}
	}
}
