package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strings"
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

var hashOfRandomData string

func init() {
	randomSrc := randomDataMaker{rand.NewSource(1028890720402726901)}
	n, err := randomSrc.Read(randomData)
	if err != nil {
		panic(err)
	}
	if n != len(randomData) {
		panic("Didn't initialize random data properly")
	}

	sh := getHash()
	written, err := sh.Write(randomData)
	if err != nil {
		panic(err)
	}
	if written != len(randomData) {
		panic("short write")
	}
	hashOfRandomData = hex.EncodeToString(sh.Sum([]byte{}))
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

func testWithTempDir(t *testing.T, f func(string)) {
	t.Parallel()
	tmpdir, err := ioutil.TempDir("", "hashtest")
	if err != nil {
		t.Fatalf("Error getting temp dir: %v", err)
	}
	defer os.RemoveAll(tmpdir)

	f(tmpdir)
}

func validateHashFile(fn string) error {
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(randomData, b) {
		return errors.New("Didn't read the same data")
	}
	return nil
}

func TestHashWriterClose(t *testing.T) {
	testWithTempDir(t, func(tmpdir string) {
		hr, err := NewHashRecord(tmpdir, "")
		if err != nil {
			t.Fatalf("Error establishing hash record: %v", err)
		}
		hr.Close()
	})
}

func TestHashWriterDoubleClose(t *testing.T) {
	testWithTempDir(t, func(tmpdir string) {
		hr, err := NewHashRecord(tmpdir, "")
		if err != nil {
			t.Fatalf("Error establishing hash record: %v", err)
		}
		hr.Close()
		hr.Close()
	})
}

func TestHashWriterUninitClose(t *testing.T) {
	testWithTempDir(t, func(tmpdir string) {
		hr := hashRecord{}
		hr.Close()
	})
}

func TestHashWriterNilClose(t *testing.T) {
	testWithTempDir(t, func(tmpdir string) {
		hr := (*hashRecord)(nil)
		hr.Close()
	})
}

func TestHashWriterNoHash(t *testing.T) {
	testWithTempDir(t, func(tmpdir string) {
		hr, err := NewHashRecord(tmpdir, "")
		if err != nil {
			t.Fatalf("Error establishing hash record: %v", err)
		}
		defer hr.Close()
		hr.base = tmpdir
		h, l, err := hr.Process(bytes.NewReader(randomData))
		if err != nil {
			t.Fatalf("Error processing: %v", err)
		}
		if int(l) != len(randomData) {
			t.Fatalf("Processing was short: %v != %v",
				l, len(randomData))
		}
		if h != hashOfRandomData {
			t.Fatalf("Expected hash %v, got %v",
				hashOfRandomData, h)
		}
		err = validateHashFile(hashFilename(tmpdir, hashOfRandomData))
		if err != nil {
			t.Fatalf("Didn't find valid hash: %v", err)
		}
	})
}

func TestHashWriterGoodHash(t *testing.T) {
	testWithTempDir(t, func(tmpdir string) {
		hr, err := NewHashRecord(tmpdir, hashOfRandomData)
		if err != nil {
			t.Fatalf("Error establishing hash record: %v", err)
		}
		defer hr.Close()
		hr.base = tmpdir
		h, l, err := hr.Process(bytes.NewReader(randomData))
		if err != nil {
			t.Fatalf("Error processing: %v", err)
		}
		if int(l) != len(randomData) {
			t.Fatalf("Processing was short: %v != %v",
				l, len(randomData))
		}
		if h != hashOfRandomData {
			t.Fatalf("Expected hash %v, got %v",
				hashOfRandomData, h)
		}
		err = validateHashFile(hashFilename(tmpdir, hashOfRandomData))
		if err != nil {
			t.Fatalf("Didn't find valid hash: %v", err)
		}
	})
}

func TestHashWriterWithBadHash(t *testing.T) {
	testWithTempDir(t, func(tmpdir string) {
		hr, err := NewHashRecord(tmpdir, "fde65ea0f4a6d1b0eb20c3b6b7e054512d2c45dc")
		if err != nil {
			t.Fatalf("Error establishing hash record: %v", err)
		}
		defer hr.Close()
		hr.base = tmpdir
		_, l, err := hr.Process(bytes.NewReader(randomData))
		if err == nil {
			t.Fatalf("Expected error processing")
		} else {
			if !strings.Contains(err.Error(), "Invalid hash") {
				t.Fatalf("Expected invalid hash error, got %v", err)
			}
		}
		if int(l) != len(randomData) {
			t.Fatalf("Processing was short: %v != %v",
				l, len(randomData))
		}
		err = validateHashFile(hashFilename(tmpdir, hashOfRandomData))
		if err == nil {
			t.Fatalf("Unexpectedly found valid hash.")
		}
	})
}
