// Copyright 2026 The Mellium Contributors.
// Use of this source code is governed by the BSD 2-clause
// license that can be found in the LICENSE file.

package xmpp

import (
	"bytes"
	"context"
	"errors"
	"time"
)

// smReplay sends the retained suffix while Serve can process the peer's replay.
// Application writers wait for done. Replies produced by Serve are staged
// behind the suffix under the session output lock, so they don't block reads.
type smReplay struct {
	session *Session
	done    chan struct{}
	staged  bytes.Buffer
	err     error
}

func (r *smReplay) Write(p []byte) (int, error) {
	select {
	case <-r.done:
		if r.err != nil {
			return 0, r.err
		}
		return r.session.Conn().Write(p)
	default:
		if r.staged.Len()+len(p) > smMaxBytes {
			return 0, errors.New("xmpp: post-replay output buffer limit exceeded")
		}
		return r.staged.Write(p)
	}
}

func (r *smReplay) run(pending [][]byte) {
	s := r.session
	timer := time.AfterFunc(smHandshakeTimeout, func() { _ = s.Conn().Close() })
	defer timer.Stop()
	for _, wire := range pending {
		if err := smWrite(s.Conn(), wire); err != nil {
			r.fail(err)
			return
		}
	}
	// A full replay queue needs a fresh acknowledgment even if the
	// application's next write has to wait for room in it.
	if err := smWrite(s.Conn(), []byte(smRequestXML)); err != nil {
		r.fail(err)
		return
	}
	for {
		s.out.Lock()
		if r.staged.Len() == 0 {
			close(r.done)
			s.out.Unlock()
			return
		}
		wire := r.staged.Bytes()
		r.staged = bytes.Buffer{}
		s.out.Unlock()
		if err := smWrite(s.Conn(), wire); err != nil {
			r.fail(err)
			return
		}
	}
}

func (r *smReplay) fail(err error) {
	_ = r.session.Conn().Close()
	r.session.out.Lock()
	defer r.session.out.Unlock()
	r.err = err
	r.staged = bytes.Buffer{}
	close(r.done)
}

func (s *Session) waitForReplay(ctx context.Context) error {
	if s.smReplay == nil {
		return nil
	}
	select {
	case <-s.smReplay.done:
		return s.smReplay.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
