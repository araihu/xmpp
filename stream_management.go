// Copyright 2026 The Mellium Contributors.
// Use of this source code is governed by the BSD 2-clause
// license that can be found in the LICENSE file.

package xmpp

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"mellium.im/xmlstream"
	"mellium.im/xmpp/internal/decl"
	"mellium.im/xmpp/internal/ns"
	intstream "mellium.im/xmpp/internal/stream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
	"mellium.im/xmpp/stream"
)

const (
	smNamespace        = "urn:xmpp:sm:3"
	smRequestXML       = `<r xmlns="urn:xmpp:sm:3"/>`
	smMaxStanzas       = 256
	smMaxBytes         = 4 << 20
	smAckInterval      = 8
	smHandshakeTimeout = 10 * time.Second
)

// ErrStreamManagementQueueFull indicates that the stanza could not be sent
// because the unacknowledged stanza buffer is full. Previously written stanzas
// remain eligible for resumption.
var ErrStreamManagementQueueFull = errors.New("xmpp: stream management queue full")

// ErrStreamManagementOutcomeUnknown indicates a failed write after a stanza
// was queued for replay, or that the logical stream ended while awaiting its
// response. The peer might already have handled the stanza.
var ErrStreamManagementOutcomeUnknown = errors.New("xmpp: stream management stanza outcome is unknown")

var errStreamManagementProtocol = errors.New("xmpp: invalid stream management data")

// StreamManagement retains XEP-0198 state across connections for one client
// account. The zero value is ready for use. Do not copy it after first use.
// Resumption state is in memory and survives transport failures while the
// process is running.
type StreamManagement struct {
	mu       sync.Mutex
	account  jid.JID
	previous *Session
	state    *smState
}

// ResumeLocation returns the server's preferred address for reconnecting, if
// the previous stream is resumable and the server offered an address. It is
// a dialing hint. Keep verifying TLS and authenticating for the original XMPP
// domain, and fall back to normal discovery if dialing the hint fails.
func (m *StreamManagement) ResumeLocation() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return ""
	}
	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	if m.state.id == "" {
		return ""
	}
	return m.state.location
}

// StreamManagementStatus reports the negotiated state of a client session.
// Counters are cumulative within the logical stream and wrap at 2^32.
type StreamManagementStatus struct {
	Enabled        bool
	Resumed        bool
	Resumable      bool
	Handled        uint32
	Acknowledged   uint32
	Unacknowledged int
}

// StreamManagement returns the session's stream management status.
func (s *Session) StreamManagement() StreamManagementStatus {
	if s.sm == nil {
		return StreamManagementStatus{}
	}
	s.sm.mu.Lock()
	defer s.sm.mu.Unlock()
	return StreamManagementStatus{
		Enabled:        true,
		Resumed:        s.smResumed,
		Resumable:      s.sm.id != "",
		Handled:        s.sm.handled,
		Acknowledged:   s.sm.acked,
		Unacknowledged: len(s.sm.pending),
	}
}

// Connect authenticates a TCP client session on conn. It tries to resume the
// previous authenticated stream before resource binding when possible. On
// refusal or lack of support it negotiates a fresh stream. Stanzas from the
// previous stream are replayed only after successful resumption. Replay may
// continue after Connect returns. Start Serve before sending on the resumed
// session so it can receive the peer's own replay and acknowledgments.
//
// Run one Session.Serve loop to process a managed session. A stanza is handled
// once its handler returns nil and the element has been consumed. Returning an error
// aborts the connection and leaves that stanza eligible for peer replay.
// Internally routed IQ replies are handled after their reader is closed and
// the element has been consumed. Use Session.Close to end a logical stream, or
// close the underlying connection and wait for Serve to return before resuming.
// Connect closes conn if negotiation fails and returns a nil session.
func (m *StreamManagement) Connect(ctx context.Context, origin jid.JID, conn net.Conn, features ...StreamFeature) (_ *Session, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()

	if m.previous != nil {
		select {
		case <-m.previous.smDone:
		default:
			return nil, errors.New("xmpp: previous managed session is still active")
		}
	}
	if !m.account.Equal(jid.JID{}) && !m.account.Equal(origin.Bare()) {
		return nil, errors.New("xmpp: stream management account changed")
	}
	m.account = origin.Bare()

	ctx, cancel := context.WithTimeout(ctx, smHandshakeTimeout)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	resume := StreamFeature{
		Name:      smName("sm"),
		Necessary: Secure | Authn,
		Parse: func(_ context.Context, d *xml.Decoder, start *xml.StartElement) (bool, any, error) {
			return false, nil, d.Skip()
		},
		Negotiate: func(ctx context.Context, s *Session, _ any) (SessionState, io.ReadWriter, error) {
			if m.state == nil || m.state.id == "" {
				return 0, nil, nil
			}
			ok, err := m.resume(ctx, s)
			if ok {
				return Ready, nil, nil
			}
			return 0, nil, err
		},
	}
	s, err := NewClientSession(ctx, origin, conn, append([]StreamFeature{resume}, features...)...)
	if err != nil {
		s.reportSMCountError(err)
		return nil, err
	}
	if s.sm == nil {
		if m.state != nil {
			m.state.discard()
			m.state = nil
		}
		_, advertised := s.Feature(smNamespace)
		_, bound := s.negotiated[ns.Bind]
		if advertised && bound && s.State()&(Secure|Authn) == Secure|Authn && s.LocalAddr().Resourcepart() != "" {
			if err := m.enable(ctx, s); err != nil {
				return nil, err
			}
		}
	}
	if !stop() {
		return nil, ctx.Err()
	}
	if s.sm != nil {
		s.smDone = make(chan struct{})
		// The top-level stanza encoder adds IDs and normalizes namespaces before
		// the stream management writer serializes the final bytes.
		stanzaWriter := s.out.e.(*stanzaEncoder)
		s.out.Locker = newSMOutputLock()
		var wire io.Writer = s.Conn()
		if s.smResumed && len(s.sm.pending) != 0 {
			s.smReplay = &smReplay{session: s, done: make(chan struct{})}
			wire = s.smReplay
		}
		s.smWriter = &smWriter{s: s, destination: wire, TokenWriteFlusher: xml.NewEncoder(wire)}
		stanzaWriter.TokenWriteFlusher = s.smWriter
		if s.smReplay != nil {
			// Detach the slice from live ACK trimming. Individual byte
			// slices are immutable after their initial serialization.
			go s.smReplay.run(append([][]byte(nil), s.sm.pending...))
		}
		m.previous = s
	} else {
		m.previous = nil
	}
	return s, nil
}

func (m *StreamManagement) enable(ctx context.Context, s *Session) error {
	if err := smSend(ctx, s, "enable", xml.Attr{Name: xml.Name{Local: "resume"}, Value: "true"}); err != nil {
		return err
	}
	start, err := smReply(s, true)
	if err != nil {
		return err
	}
	switch start.Name {
	case smName("failed"):
		return nil
	case smName("enabled"):
		id, _ := smAttr(start, "id")
		resume, _ := smAttr(start, "resume")
		switch resume {
		case "true", "1":
			if id == "" || len(id) > 4000 {
				return fmt.Errorf("%w: missing or oversized resumption ID", errStreamManagementProtocol)
			}
		case "false", "0", "":
			id = ""
		default:
			return fmt.Errorf("%w: invalid resume attribute", errStreamManagementProtocol)
		}
		location, _ := smAttr(start, "location")
		s.sm = &smState{
			id: id, bound: s.LocalAddr(), location: location,
			replies: stanzaReplies{ended: make(chan struct{})},
		}
		m.state = s.sm
		return nil
	default:
		return fmt.Errorf("%w: expected enabled or failed, got %v", errStreamManagementProtocol, start.Name)
	}
}

func (m *StreamManagement) resume(ctx context.Context, s *Session) (bool, error) {
	state := m.state
	if err := smSend(ctx, s, "resume",
		xml.Attr{Name: xml.Name{Local: "previd"}, Value: state.id},
		smCounter(state.handled),
	); err != nil {
		return false, err
	}
	start, err := smReply(s, false)
	if err != nil {
		if permanentSMError(err) {
			state.discard()
		}
		return false, err
	}
	if start.Name == smName("failed") {
		defer state.discard()
		if _, ok := smAttr(start, "h"); ok {
			return false, state.acknowledge(start)
		}
		return false, nil
	}
	if id, _ := smAttr(start, "previd"); start.Name != smName("resumed") || id != state.id {
		state.discard()
		return false, fmt.Errorf("%w: mismatched resumption reply", errStreamManagementProtocol)
	}
	if err := state.acknowledge(start); err != nil {
		state.discard()
		return false, err
	}
	if !s.UpdateAddr(state.bound) {
		return false, errors.New("xmpp: unable to restore the resumed resource")
	}
	s.sm, s.smResumed = state, true
	return true, nil
}

func smName(local string) xml.Name { return xml.Name{Space: smNamespace, Local: local} }

func smCounter(h uint32) xml.Attr {
	return xml.Attr{Name: xml.Name{Local: "h"}, Value: strconv.FormatUint(uint64(h), 10)}
}

func smAttr(start xml.StartElement, name string) (string, bool) {
	for _, attr := range start.Attr {
		if attr.Name.Space == "" && attr.Name.Local == name {
			return attr.Value, true
		}
	}
	return "", false
}

func smSend(ctx context.Context, s *Session, local string, attrs ...xml.Attr) error {
	return s.Send(ctx, xmlstream.Wrap(nil, xml.StartElement{Name: smName(local), Attr: attrs}))
}

func smReply(s *Session, enabling bool) (xml.StartElement, error) {
	var prefix []xml.Token
	var buffered int64
	for {
		tok, err := decl.TrimLeftSpace(s.in.d).Token()
		if err != nil {
			return xml.StartElement{}, err
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			return xml.StartElement{}, fmt.Errorf("%w: expected a stream management reply", errStreamManagementProtocol)
		}
		if err := decodeStreamErr(start, s.in.d); err != nil {
			return xml.StartElement{}, err
		}
		if enabling && stanza.Is(start.Name, s.in.XMLNS) {
			if s.smPreEnabled == smMaxStanzas {
				return xml.StartElement{}, errors.New("xmpp: too many stanzas received before stream management was enabled")
			}
			r := xmlstream.MultiReader(xmlstream.Token(start), xmlstream.InnerElement(s.in.d))
			for {
				t, err := r.Token()
				if err != nil && err != io.EOF {
					return xml.StartElement{}, err
				}
				if t != nil {
					buffered += smTokenSize(t)
					if buffered > smMaxBytes {
						return xml.StartElement{}, errors.New("xmpp: stream management input buffer limit exceeded")
					}
					prefix = append(prefix, xml.CopyToken(t))
				}
				if err == io.EOF {
					break
				}
			}
			s.smPreEnabled++
			continue
		}
		if start.Name.Space != smNamespace {
			return xml.StartElement{}, fmt.Errorf("%w: unexpected reply %v", errStreamManagementProtocol, start.Name)
		}
		if start.Name.Local == "failed" {
			_, err = xmlstream.Copy(xmlstream.Discard(), xmlstream.InnerElement(s.in.d))
		} else {
			err = smEnd(s.in.d, start)
		}
		if err == nil && len(prefix) != 0 {
			s.in.d = xmlstream.MultiReader(xmlstream.ReaderFunc(func() (xml.Token, error) {
				if len(prefix) == 0 {
					return nil, io.EOF
				}
				t := prefix[0]
				prefix[0] = nil
				prefix = prefix[1:]
				return t, nil
			}), s.in.d)
		}
		return start, err
	}
}

func smEnd(r xml.TokenReader, start xml.StartElement) error {
	tok, err := r.Token()
	if err != nil {
		return err
	}
	if end, ok := tok.(xml.EndElement); !ok || end.Name != start.Name {
		return fmt.Errorf("%w: expected end of %v", errStreamManagementProtocol, start.Name)
	}
	return nil
}

func (s *Session) handleSM(r xml.TokenReader, start xml.StartElement) error {
	if err := smEnd(r, start); err != nil {
		return err
	}
	switch start.Name.Local {
	case "a":
		return s.sm.acknowledge(start)
	case "r":
		s.sm.mu.Lock()
		h := s.sm.handled
		s.sm.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), smHandshakeTimeout)
		defer cancel()
		err := send(ctx, s, xmlstream.Wrap(nil, xml.StartElement{Name: smName("a"), Attr: []xml.Attr{smCounter(h)}}), nil)
		if errors.Is(err, ErrOutputStreamClosed) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("%w: unexpected element %v", errStreamManagementProtocol, start.Name)
	}
}

func (s *Session) finishManagedServe(err error, graceful bool) {
	defer close(s.smDone)
	s.reportSMCountError(err)
	if s.smReadAhead != nil {
		s.smReadAhead.stop()
	}
	if graceful {
		_ = s.Conn().SetWriteDeadline(time.Now().Add(smHandshakeTimeout))
		// Reply to a graceful peer close and send our final handled count.
		_ = s.Close()
	}
	// Wake blocked writers before taking the output lock. A transport failure
	// or handler error must leave the logical stream available for resumption.
	_ = s.Conn().Close()
	_ = s.waitForReplay(context.Background())
	<-s.smReadAhead.done
	s.closeInputStream()
	s.out.Lock()
	defer s.out.Unlock()
	if graceful || !s.StreamManagement().Resumable || s.State()&OutputStreamClosed != 0 || permanentSMError(err) {
		s.sm.discard()
	}
	s.stateMutex.Lock()
	s.state |= OutputStreamClosed
	s.stateMutex.Unlock()
}

func permanentSMError(err error) bool {
	var se stream.Error
	var syntax *xml.SyntaxError
	return errors.Is(err, errStreamManagementProtocol) || errors.As(err, &se) ||
		errors.Is(err, intstream.ErrUnexpectedRestart) || errors.Is(err, intstream.ErrUnknownStreamElement) ||
		(errors.As(err, &syntax) && syntax.Msg != "unexpected EOF")
}

func (s *Session) closeManagedStream() error {
	s.sm.mu.Lock()
	h := s.sm.handled
	s.sm.id = ""
	s.sm.mu.Unlock()
	if err := s.Conn().SetWriteDeadline(time.Now().Add(smHandshakeTimeout)); err != nil {
		return err
	}
	defer s.Conn().SetWriteDeadline(time.Time{})
	ackErr := smWrite(s.Conn(), []byte(fmt.Sprintf(`<a xmlns="%s" h="%d"/>`, smNamespace, h)))
	return errors.Join(ackErr, intstream.Close(s.Conn(), &s.out.Info))
}

type smState struct {
	mu        sync.Mutex
	bound     jid.JID
	id        string
	handled   uint32
	acked     uint32
	pending   [][]byte
	bytes     int
	location  string
	replies   stanzaReplies
	discarded bool
}

func (s *smState) discard() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.id = ""
	s.pending = nil
	s.bytes = 0
	if !s.discarded && s.replies.ended != nil {
		close(s.replies.ended)
	}
	s.discarded = true
}

func (s *smState) acknowledge(start xml.StartElement) error {
	raw, _ := smAttr(start, "h")
	h, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return fmt.Errorf("%w: invalid handled count", errStreamManagementProtocol)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// The queue is kept well below 2^32 entries, so subtraction also
	// distinguishes an old or out-of-range acknowledgment after wrapping.
	delta := uint32(h) - s.acked
	if uint64(delta) > uint64(len(s.pending)) {
		return smCountError{handled: uint32(h), sent: s.acked + uint32(len(s.pending))}
	}
	for _, wire := range s.pending[:delta] {
		s.bytes -= len(wire)
	}
	clear(s.pending[:delta])
	s.pending = s.pending[delta:]
	s.acked = uint32(h)
	return nil
}

type smCountError struct {
	handled uint32
	sent    uint32
}

func (e smCountError) Error() string {
	return fmt.Sprintf("xmpp: peer acknowledged %d stanzas after %d were sent", e.handled, e.sent)
}

func (e smCountError) Unwrap() error { return errStreamManagementProtocol }

func (s *Session) reportSMCountError(err error) {
	var count smCountError
	if !errors.As(err, &count) {
		return
	}
	if s.smReplay != nil {
		// During replay the transport has an active writer. Close it
		// without interleaving a stream error with retransmitted bytes.
		select {
		case <-s.smReplay.done:
			if s.smReplay.err != nil {
				return
			}
		default:
			return
		}
	}
	_ = s.Conn().SetWriteDeadline(time.Now().Add(smHandshakeTimeout))
	s.out.Lock()
	defer s.out.Unlock()
	s.stateMutex.Lock()
	if s.state&OutputStreamClosed != 0 {
		s.stateMutex.Unlock()
		return
	}
	s.state |= OutputStreamClosed
	s.stateMutex.Unlock()
	payload := xmlstream.Wrap(nil, xml.StartElement{
		Name: smName("handled-count-too-high"),
		Attr: []xml.Attr{
			smCounter(count.handled),
			{Name: xml.Name{Local: "send-count"}, Value: strconv.FormatUint(uint64(count.sent), 10)},
		},
	})
	encoder := xml.NewEncoder(s.Conn())
	se := stream.UndefinedCondition.ApplicationError(payload)
	if _, err := se.WriteXML(encoder); err != nil {
		return
	}
	if err := encoder.Flush(); err == nil {
		_ = intstream.Close(s.Conn(), &s.out.Info)
	}
}
