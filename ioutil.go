package main

import (
	"errors"
	"io"
	"math/rand"
	"time"
)

var Timeout = errors.New("Timeout")

type randomDataMaker struct {
	src rand.Source
}

func (r *randomDataMaker) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte(r.src.Int63() & 0xff)
	}
	return len(p), nil
}

type copyRes struct {
	s int64
	e error
}

type ErrorCloser interface {
	io.ReadCloser
	CloseWithError(error) error
}

func bgCopy(w io.Writer, r io.Reader, ch chan<- copyRes) {
	s, e := io.Copy(w, r)
	ch <- copyRes{s, e}
}

type closingPipe struct {
	r       io.Reader
	pr      *io.PipeReader
	pw      *io.PipeWriter
	err     error
	timeout time.Duration
}

func (cp *closingPipe) Read(p []byte) (n int, err error) {
	n, err = cp.r.Read(p)
	if n > 0 {
		// Pipe writes block completely if the consumer stops
		// reading.  This lets us tear them down meaningfully.
		timer := time.AfterFunc(cp.timeout, func() {
			cp.CloseWithError(Timeout)
		})
		defer timer.Stop()

		if n, err := cp.pw.Write(p[:n]); err != nil {
			return n, err
		}
	}
	if err != nil {
		cp.err = err
		cp.pr.CloseWithError(err)
		cp.pw.CloseWithError(err)
	}
	return
}

func (cp *closingPipe) CloseWithError(err error) error {
	cp.err = err
	cp.pr.CloseWithError(cp.err)
	return cp.pw.CloseWithError(cp.err)
}

func (cp *closingPipe) Close() error {
	cp.err = io.EOF
	cp.pr.CloseWithError(cp.err)
	return cp.pw.CloseWithError(cp.err)
}

type pipeErrAdaptor struct {
	p *io.PipeReader
}

func (p *pipeErrAdaptor) Read(b []byte) (int, error) {
	n, err := p.p.Read(b)
	if err == io.ErrClosedPipe {
		err = io.EOF
	}
	return n, err
}

func newMultiReaderTimeout(r io.Reader, to time.Duration) (ErrorCloser, io.Reader) {
	pr, pw := io.Pipe()

	return &closingPipe{r, pr, pw, nil, to},
		&pipeErrAdaptor{pr}
}

func newMultiReader(r io.Reader) (ErrorCloser, io.Reader) {
	return newMultiReaderTimeout(r, 15*time.Second)
}
