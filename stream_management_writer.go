// Copyright 2026 The Mellium Contributors.
// Use of this source code is governed by the BSD 2-clause
// license that can be found in the LICENSE file.

package xmpp

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"

	"mellium.im/xmlstream"
	"mellium.im/xmpp/stanza"
)

type smOutputLock chan struct{}

func newSMOutputLock() smOutputLock {
	return make(smOutputLock, 1)
}

func (l smOutputLock) Lock()   { l <- struct{}{} }
func (l smOutputLock) Unlock() { <-l }

func (s *Session) lockOutput(ctx context.Context) error {
	if l, ok := s.out.Locker.(smOutputLock); ok {
		select {
		case l <- struct{}{}:
			if err := ctx.Err(); err != nil {
				l.Unlock()
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}
	s.out.Lock()
	return nil
}

// RequestStreamManagementAck asks the peer to report its handled count. Serve
// processes the response. The call has no effect when management is inactive.
func (s *Session) RequestStreamManagementAck(ctx context.Context) error {
	if s.sm == nil {
		return nil
	}
	return smSend(ctx, s, "r")
}

var errIncompleteManagedElement = errors.New("xmpp: incomplete element on a managed stream")

// smWriter is used with the session output lock held. Its encoder sees the
// final stanza after ID generation and namespace normalization.
type smWriter struct {
	xmlstream.TokenWriteFlusher
	s            *Session
	destination  io.Writer
	depth        int
	stanza       *xml.Encoder
	wire         smBuffer
	sinceRequest int
	err          error
}

func (w *smWriter) EncodeToken(t xml.Token) error {
	if w.err != nil {
		return w.err
	}
	switch tok := t.(type) {
	case xml.StartElement:
		if w.depth == 0 && stanza.Is(tok.Name, w.s.out.XMLNS) {
			w.stanza = xml.NewEncoder(&w.wire)
		}
		w.depth++
	case xml.EndElement:
		w.depth--
	}
	if w.stanza == nil {
		return w.TokenWriteFlusher.EncodeToken(t)
	}
	if err := w.stanza.EncodeToken(t); err != nil {
		return w.fail(err)
	}
	if w.depth != 0 {
		return nil
	}
	if err := w.stanza.Close(); err != nil {
		return w.fail(err)
	}
	wire := w.wire.Bytes()
	w.wire = smBuffer{}
	w.stanza = nil
	if err := w.send(wire); err != nil {
		if errors.Is(err, ErrStreamManagementQueueFull) {
			return err
		}
		return w.fail(err)
	}
	return nil
}

func (w *smWriter) send(wire []byte) error {
	if err := w.TokenWriteFlusher.Flush(); err != nil {
		return err
	}

	state := w.s.sm
	state.mu.Lock()
	if len(state.pending) == smMaxStanzas || state.bytes+len(wire) > smMaxBytes {
		state.mu.Unlock()
		return ErrStreamManagementQueueFull
	}
	state.pending = append(state.pending, wire)
	state.bytes += len(wire)
	request := state.bytes >= smMaxBytes/2
	state.mu.Unlock()

	if err := smWrite(w.destination, wire); err != nil {
		return errors.Join(ErrStreamManagementOutcomeUnknown, err)
	}
	w.sinceRequest++
	if w.sinceRequest < smAckInterval && !request {
		return nil
	}
	w.sinceRequest = 0
	if err := smWrite(w.destination, []byte(smRequestXML)); err != nil {
		return errors.Join(ErrStreamManagementOutcomeUnknown, err)
	}
	return nil
}

func (w *smWriter) Flush() error {
	if w.err != nil {
		return w.err
	}
	if w.stanza != nil {
		// An incomplete stanza is buffered until its matching closing tag.
		return nil
	}
	if err := w.TokenWriteFlusher.Flush(); err != nil {
		return w.fail(err)
	}
	return nil
}

func (w *smWriter) fail(err error) error {
	w.err = err
	w.stanza = nil
	w.wire = smBuffer{}
	_ = w.s.Conn().Close()
	return err
}

// endManagedWrite is called with the output lock held when a writer is done.
// Incomplete elements poison a stream even when no bytes were put on the wire,
// since the namespace and depth state can no longer be used by the next writer.
func (s *Session) endManagedWrite(err *error) {
	if s.smWriter == nil || s.smWriter.depth == 0 {
		return
	}
	if *err == nil {
		*err = errIncompleteManagedElement
	}
	s.smWriter.fail(*err)
}

type smBuffer struct{ bytes.Buffer }

func (b *smBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > smMaxBytes {
		return 0, ErrStreamManagementQueueFull
	}
	return b.Buffer.Write(p)
}

func smWrite(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err == nil && n != len(p) {
		return io.ErrShortWrite
	}
	return err
}
