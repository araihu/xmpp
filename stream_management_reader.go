// Copyright 2026 The Mellium Contributors.
// Use of this source code is governed by the BSD 2-clause
// license that can be found in the LICENSE file.

package xmpp

import (
	"context"
	"encoding/xml"
	"errors"
	"io"

	"golang.org/x/sync/semaphore"
)

// smReadAhead lets the peer replay its own unacknowledged stanzas while we
// write ours. It buffers tokens without accepting responsibility for them.
// Once Connect returns the existing Serve loop drains the same token stream.
type smReadAhead struct {
	ctx     context.Context
	stop    context.CancelFunc
	budget  *semaphore.Weighted
	results chan smReadResult
}

type smReadResult struct {
	token xml.Token
	err   error
	size  int64
}

func newSMReadAhead(r xml.TokenReader) *smReadAhead {
	ctx, stop := context.WithCancel(context.Background())
	a := &smReadAhead{
		ctx:     ctx,
		stop:    stop,
		budget:  semaphore.NewWeighted(smMaxBytes),
		results: make(chan smReadResult, smMaxStanzas),
	}
	go a.read(r)
	return a
}

func (a *smReadAhead) read(r xml.TokenReader) {
	defer close(a.results)
	for a.ctx.Err() == nil {
		token, err := r.Token()
		size := smTokenSize(token)
		if size > smMaxBytes {
			token, err, size = nil, errors.New("xmpp: stream management input buffer limit exceeded"), 0
		}
		if err := a.budget.Acquire(a.ctx, size); err != nil {
			return
		}
		select {
		case a.results <- smReadResult{token: xml.CopyToken(token), err: err, size: size}:
		case <-a.ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func (a *smReadAhead) Token() (xml.Token, error) {
	select {
	case result, ok := <-a.results:
		if !ok {
			return nil, io.EOF
		}
		a.budget.Release(result.size)
		return result.token, result.err
	case <-a.ctx.Done():
		return nil, a.ctx.Err()
	}
}

func smTokenSize(token xml.Token) int64 {
	switch tok := token.(type) {
	case xml.CharData:
		return int64(len(tok))
	case xml.StartElement:
		size := len(tok.Name.Local) + len(tok.Name.Space) + 64*len(tok.Attr)
		for _, attr := range tok.Attr {
			size += len(attr.Name.Local) + len(attr.Name.Space) + len(attr.Value)
		}
		return int64(size)
	}
	return 0
}
