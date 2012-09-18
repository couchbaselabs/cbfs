package main

import (
	"io"
	"math/rand"
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

type copyRes struct {
	s int64
	e error
}

func bgCopy(w io.Writer, r io.Reader, ch chan<- copyRes) {
	s, e := io.Copy(w, r)
	ch <- copyRes{s, e}
}

type closingPipe struct {
	r  io.Reader
	pr *io.PipeReader
	pw *io.PipeWriter
}

func (cp *closingPipe) Read(p []byte) (n int, err error) {
	n, err = cp.r.Read(p)
	if n > 0 {
		if n, err := cp.pw.Write(p[:n]); err != nil {
			return n, err
		}
	}
	if err != nil {
		cp.pr.CloseWithError(err)
		cp.pw.CloseWithError(err)
	}
	return
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

func newMultiReader(r io.Reader) (io.Reader, io.Reader) {
	pr, pw := io.Pipe()

	return &closingPipe{r, pr, pw}, &pipeErrAdaptor{pr}
}
