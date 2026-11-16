// Copyright 2026 The Mellium Contributors.
// Use of this source code is governed by the BSD 2-clause
// license that can be found in the LICENSE file.

package muc

import (
	"encoding/xml"
	"io"
	"testing"

	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

func TestDepartureBeforeLeaveWaits(t *testing.T) {
	addr := jid.MustParse("room@example.net/me")
	channel := &Channel{addr: addr, depart: make(chan struct{})}
	client := &Client{managed: map[string]*Channel{addr.String(): channel}}
	p := stanza.Presence{From: addr, Type: stanza.UnavailablePresence}
	r := struct {
		xml.TokenReader
		*xml.Encoder
	}{p.Wrap(nil), xml.NewEncoder(io.Discard)}
	if err := client.HandlePresence(p, r); err != nil {
		t.Fatal(err)
	}

	select {
	case <-channel.depart:
	default:
		t.Fatal("departure was lost before the caller began waiting")
	}
}
