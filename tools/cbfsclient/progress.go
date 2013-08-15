package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dustin/go-humanize"
)

type progressReader struct {
	orig     io.Reader
	total    int64
	width    int
	clearBuf []byte
	read     chan int
}

func newProgressReader(r io.Reader, n int64) io.ReadCloser {
	width := 80
	rv := &progressReader{
		orig:     r,
		total:    n,
		width:    width,
		clearBuf: make([]byte, width),
		read:     make(chan int),
	}

	for i := range rv.clearBuf {
		rv.clearBuf[i] = ' '
	}
	rv.clearBuf[0] = '\r'
	rv.clearBuf[width-1] = '\r'

	go rv.update()
	return rv
}

func (p *progressReader) showPercent(read int64) {
	p.clear()
	perc := float64(read*100) / float64(p.total)
	fmt.Fprintf(os.Stdout, "%v/%v (%.1f%%)",
		humanize.Bytes(uint64(read)),
		humanize.Bytes(uint64(p.total)), perc)
	os.Stdout.Sync()
}

func (p *progressReader) Close() error {
	p.clear()
	close(p.read)
	return nil
}

func (p *progressReader) clear() {
	os.Stdout.Write(p.clearBuf)
}

func (p *progressReader) update() {
	start := time.After(time.Second * 5)
	var tick <-chan time.Time
	var read int64
	for {
		select {
		case <-start:
			t := time.NewTicker(time.Second)
			defer t.Stop()
			tick = t.C
			start = nil
		case <-tick:
			p.showPercent(read)
		case n, ok := <-p.read:
			if !ok {
				return
			}
			read += int64(n)
		}
	}
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.orig.Read(b)
	p.read <- n
	return n, err
}
