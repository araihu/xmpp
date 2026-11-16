// Copyright 2026 The Mellium Contributors.
// Use of this source code is governed by the BSD 2-clause
// license that can be found in the LICENSE file.

package xmpp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"mellium.im/xmlstream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const smTestStream = `<stream:stream from='example.net' id='fixture' xmlns='jabber:client' xmlns:stream='http://etherx.jabber.org/streams' version='1.0'>`

func TestStreamManagementResume(t *testing.T) {
	var management StreamManagement
	peer := newSMPeer(t, &management, true)
	peer.bind("alice@example.net/original", "original-id")
	session := peer.session()

	commitFailed := errors.New("commit failed")
	served := make(chan error, 1)
	go func() {
		served <- session.Serve(HandlerFunc(func(_ xmlstream.TokenReadEncoder, start *xml.StartElement) error {
			if id, _ := smAttr(*start, "id"); id == "uncommitted" {
				return commitFailed
			}
			return nil
		}))
	}()

	// Exercise Encode and TokenWriter, including automatic ID generation.
	peer.sendMessage(session, "first")
	peer.sendMessage(session, "second")
	sent := make(chan error, 1)
	go func() {
		w := session.TokenWriter()
		_, err := xmlstream.Copy(w, xml.NewDecoder(strings.NewReader(`<message><body>third &amp; original</body></message>`)))
		sent <- errors.Join(err, w.Close())
	}()
	_, original := peer.read("message")
	if err := <-sent; err != nil {
		t.Fatal(err)
	}

	peer.write(`<a xmlns='urn:xmpp:sm:3' h='1'/><message id='committed'/><r xmlns='urn:xmpp:sm:3'/>`)
	ack, _ := peer.read("a")
	peer.attr(ack, "h", "1")
	if state := session.StreamManagement(); state.Acknowledged != 1 || state.Unacknowledged != 2 {
		t.Fatalf("ordinary acknowledgment did not trim the queue: %+v", state)
	}
	peer.write(`<message id='uncommitted'/>`)
	if err := <-served; !errors.Is(err, commitFailed) {
		t.Fatalf("Serve = %v, want commit failure", err)
	}
	if state := session.StreamManagement(); !state.Resumable || state.Handled != 1 {
		t.Fatalf("failed handler advanced the committed count: %+v", state)
	}

	peer = newSMPeer(t, &management, true)
	resume, _ := peer.read("resume")
	peer.attr(resume, "h", "1")
	peer.attr(resume, "previd", "original-id")
	peer.write(`<resumed xmlns='urn:xmpp:sm:3' previd='original-id' h='2'/>`)
	// A net.Pipe forces the client and peer to read concurrently. The peer
	// writes its replay before reading the client's replay.
	peer.write(`<message id='uncommitted'/>`)
	_, replay := peer.read("message")
	if replay != original {
		t.Fatalf("replay changed stanza bytes:\noriginal: %s\nreplayed: %s", original, replay)
	}
	resumed := peer.session()
	if state := resumed.StreamManagement(); !state.Resumed || state.Acknowledged != 2 || state.Unacknowledged != 1 {
		t.Fatalf("unexpected resumed state: %+v", state)
	}
	if got := resumed.LocalAddr().String(); got != "alice@example.net/original" {
		t.Fatalf("restored address = %s", got)
	}
	go func() { served <- resumed.Serve(nil) }()
	peer.write(`<r xmlns='urn:xmpp:sm:3'/>`)
	ack, _ = peer.read("a")
	peer.attr(ack, "h", "2")
	peer.write(`</stream:stream>`)
	ack, _ = peer.read("a")
	peer.attr(ack, "h", "2")
	peer.read("stream close")
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if resumed.StreamManagement().Resumable {
		t.Fatal("graceful close kept the resumption ID")
	}
}

func TestStreamManagementRefused(t *testing.T) {
	var management StreamManagement
	peer := newSMPeer(t, &management, true)
	peer.bind("alice@example.net/original", "original-id")
	session := peer.session()
	peer.sendMessage(session, "ambiguous")
	peer.conn.Close()
	if err := session.Serve(nil); err == nil {
		t.Fatal("missing transport error")
	}

	peer = newSMPeer(t, &management, true)
	peer.read("resume")
	peer.write(`<failed xmlns='urn:xmpp:sm:3' h='0'><item-not-found xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'/></failed>`)
	peer.bind("alice@example.net/fresh", "fresh-id")
	fresh := peer.session()
	if state := fresh.StreamManagement(); !state.Enabled || state.Resumed || state.Unacknowledged != 0 {
		t.Fatalf("fresh stream inherited an ambiguous send: %+v", state)
	}
	peer.sendMessage(fresh, "fresh")
}

func TestStreamManagementInterruptedReplay(t *testing.T) {
	var management StreamManagement
	peer := newSMPeer(t, &management, true)
	peer.bind("alice@example.net/original", "original-id")
	session := peer.session()
	for _, id := range []string{"first", "second", "third"} {
		peer.sendMessage(session, id)
	}
	peer.conn.Close()
	if err := session.Serve(nil); err == nil {
		t.Fatal("missing transport error")
	}

	peer = newSMPeer(t, &management, true)
	peer.read("resume")
	peer.write(`<resumed xmlns='urn:xmpp:sm:3' previd='original-id' h='0'/>`)
	first, _ := peer.read("message")
	peer.attr(first, "id", "first")
	peer.conn.Close()
	if got := <-peer.ready; got.err == nil || got.s != nil {
		t.Fatalf("interrupted replay returned an active session: %+v", got)
	}

	peer = newSMPeer(t, &management, true)
	peer.read("resume")
	peer.write(`<resumed xmlns='urn:xmpp:sm:3' previd='original-id' h='1'/>`)
	for _, id := range []string{"second", "third"} {
		msg, _ := peer.read("message")
		peer.attr(msg, "id", id)
	}
	if state := peer.session().StreamManagement(); !state.Resumed || state.Acknowledged != 1 || state.Unacknowledged != 2 {
		t.Fatalf("unexpected resumed state after replay interruption: %+v", state)
	}
}

func TestStreamManagementUnsupported(t *testing.T) {
	var management StreamManagement
	peer := newSMPeer(t, &management, false)
	peer.bind("alice@example.net/original", "")
	session := peer.session()
	if state := session.StreamManagement(); state.Enabled {
		t.Fatalf("enabled against a server that did not advertise stream management: %+v", state)
	}
	peer.sendMessage(session, "ordinary stream")
}

func TestStreamManagementRejectsExcessiveAck(t *testing.T) {
	var management StreamManagement
	peer := newSMPeer(t, &management, true)
	peer.bind("alice@example.net/original", "original-id")
	session := peer.session()
	peer.sendMessage(session, "first")
	served := make(chan error, 1)
	go func() { served <- session.Serve(nil) }()
	peer.write(`<a xmlns='urn:xmpp:sm:3' h='2'/>`)
	_, wire := peer.read("error")
	if !strings.Contains(wire, "handled-count-too-high") || !strings.Contains(wire, `send-count="1"`) {
		t.Fatalf("unexpected stream error: %s", wire)
	}
	peer.read("stream close")
	if err := <-served; !errors.Is(err, errStreamManagementProtocol) {
		t.Fatalf("Serve = %v, want protocol error", err)
	}
	if state := session.StreamManagement(); state.Resumable || state.Unacknowledged != 0 {
		t.Fatalf("kept replay state after a bad acknowledgment: %+v", state)
	}
}

func TestStreamManagementAckWrapAndValidation(t *testing.T) {
	state := smState{acked: ^uint32(0) - 1, bytes: 2, pending: [][]byte{{'1'}, {'2'}}}
	ack := xml.StartElement{Name: smName("a"), Attr: []xml.Attr{smCounter(0)}}
	if err := state.acknowledge(ack); err != nil || state.acked != 0 || len(state.pending) != 0 || state.bytes != 0 {
		t.Fatalf("wraparound acknowledgment: %v, counted %d, pending %d", err, state.acked, len(state.pending))
	}
	ack.Attr[0] = smCounter(1)
	if err := state.acknowledge(ack); !errors.Is(err, errStreamManagementProtocol) {
		t.Fatalf("out-of-range acknowledgment = %v", err)
	}
}

type smConnectResult struct {
	s   *Session
	err error
}

type smPeer struct {
	ready <-chan smConnectResult
	t     *testing.T
	conn  net.Conn
	d     *xml.Decoder
	wire  bytes.Buffer
}

func newSMPeer(t *testing.T, management *StreamManagement, advertise bool) *smPeer {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	if err := server.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	feature := StartTLS(nil)
	// The script starts a second XML stream after a simulated authentication.
	// Real STARTTLS and SASL are exercised separately with an XMPP server.
	feature.Negotiate = func(_ context.Context, s *Session, _ any) (SessionState, io.ReadWriter, error) {
		return Secure | Authn, s.Conn(), nil
	}
	ready := make(chan smConnectResult, 1)
	go func() {
		s, err := management.Connect(context.Background(), jid.MustParse("alice@example.net"), client, feature, BindResource())
		ready <- smConnectResult{s, err}
	}()
	peer := &smPeer{t: t, conn: server, ready: ready}
	br := bufio.NewReader(server)
	readHeader := func() {
		t.Helper()
		hdr, err := br.ReadString('>')
		if strings.HasPrefix(hdr, "<?xml") && err == nil {
			hdr, err = br.ReadString('>')
		}
		if err != nil || !strings.HasPrefix(strings.TrimSpace(hdr), "<stream:stream") {
			select {
			case got := <-ready:
				t.Fatalf("client stream header %q: %v (connect: %v)", hdr, err, got.err)
			case <-time.After(100 * time.Millisecond):
				t.Fatalf("client stream header %q: %v", hdr, err)
			}
		}
	}
	readHeader()
	peer.write(smTestStream + `<stream:features><starttls xmlns='urn:ietf:params:xml:ns:xmpp-tls'><required/></starttls></stream:features>`)
	readHeader()
	features := `<bind xmlns='urn:ietf:params:xml:ns:xmpp-bind'/>`
	if advertise {
		features += `<sm xmlns='urn:xmpp:sm:3'/>`
	}
	peer.write(smTestStream + `<stream:features>` + features + `</stream:features>`)
	peer.d = xml.NewDecoder(io.TeeReader(br, &peer.wire))
	return peer
}

func (p *smPeer) session() *Session {
	p.t.Helper()
	got := <-p.ready
	if got.err != nil {
		p.t.Fatal(got.err)
	}
	return got.s
}

func (p *smPeer) write(text string) {
	p.t.Helper()
	if _, err := io.WriteString(p.conn, text); err != nil {
		p.t.Fatal(err)
	}
}

func (p *smPeer) read(local string) (xml.StartElement, string) {
	p.t.Helper()
	offset := p.d.InputOffset()
	if local == "stream close" {
		// The decoder is reading child elements rather than the stream wrapper,
		// so the raw close marker must be checked before feeding it to XML.
		if raw, _ := io.ReadAll(p.conn); string(raw) != "</stream:stream>" {
			p.t.Fatalf("stream close on the wire: %q", raw)
		}
		return xml.StartElement{}, ""
	}
	tok, err := p.d.Token()
	start, ok := tok.(xml.StartElement)
	if err != nil || !ok || start.Name.Local != local {
		p.t.Fatalf("read %v, %v; want %s", tok, err, local)
	}
	if err := p.d.Skip(); err != nil {
		p.t.Fatal(err)
	}
	return start, string(p.wire.Bytes()[offset:p.d.InputOffset()])
}

func (p *smPeer) attr(start xml.StartElement, name, want string) {
	p.t.Helper()
	if got, _ := smAttr(start, name); got != want {
		p.t.Fatalf("%s %s = %q, want %q", start.Name.Local, name, got, want)
	}
}

func (p *smPeer) bind(addr, id string) {
	p.t.Helper()
	bind, _ := p.read("iq")
	request, _ := smAttr(bind, "id")
	p.write(fmt.Sprintf(`<iq type='result' id='%s'><bind xmlns='urn:ietf:params:xml:ns:xmpp-bind'><jid>%s</jid></bind></iq>`, request, addr))
	if id != "" {
		enable, _ := p.read("enable")
		p.attr(enable, "resume", "true")
		p.write(fmt.Sprintf(`<enabled xmlns='urn:xmpp:sm:3' id='%s' resume='true'/>`, id))
	}
}

func (p *smPeer) sendMessage(s *Session, id string) {
	p.t.Helper()
	done := make(chan error, 1)
	go func() { done <- s.Encode(context.Background(), stanza.Message{ID: id}) }()
	msg, _ := p.read("message")
	p.attr(msg, "id", id)
	if err := <-done; err != nil {
		p.t.Fatal(err)
	}
}
