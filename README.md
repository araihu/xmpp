# XMPP

[![GoDoc](https://godoc.org/mellium.im/xmpp?status.svg)][docs]
[![Chat](https://img.shields.io/badge/XMPP-users@mellium.chat-orange.svg)](https://mellium.chat)
[![License](https://img.shields.io/badge/license-FreeBSD-blue.svg)](https://opensource.org/licenses/BSD-2-Clause)
[![Build Status](https://ci.codeberg.org/api/badges/mellium/xmpp/status.svg)](https://ci.codeberg.org/mellium/xmpp)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/6086/badge)](https://bestpractices.coreinfrastructure.org/projects/6086)

<a href="https://opencollective.com/mellium" alt="Donate on Open Collective"><img src="https://opencollective.com/mellium/donate/button@2x.png?color=blue" width="200"/></a>


An Extensible Messaging and Presence Protocol (XMPP) library in Go.
XMPP (sometimes known as "Jabber") is a protocol for near-real-time data
transmission, most commonly used for instant messaging, video chat signaling,
and related functionality.
This library aims to provide general protocol support with additional packages
that focus on modern instant messaging use cases.

This library supports instant messaging features such as:

- Individual and group chat,
- Blocking and unblocking users,
- Forms and commands (eg. for controlling bots and gateways),
- Retrieving message history,
- General publish-subscribe semantics for storing state and data,
- Parsing simple text styling (eg. bold, italic, quotes, etc.),
- and more!

To use it in your project, import it (or any of its other packages) like so:

```go
import mellium.im/xmpp
```

If you're looking to get started and need some help, see the [API docs][docs] or
look in the `examples/` tree for several simple usage examples.

If you'd like to contribute to the project, see [CONTRIBUTING.md].


## Client stream management

This fork adds opt-in [XEP-0198] for authenticated TLS client streams using
SASL and regular resource binding. Keep one `xmpp.StreamManagement` per
account and call its `Connect` method with each new TCP connection and the
usual STARTTLS, SASL, and bind features.

Run `Session.Serve` to consume incoming stanzas. Return success only after the
application has accepted responsibility for the stanza. For a stored message,
that means after committing it or confirming that it was already stored. Close
the transport and wait for `Serve` to return before reconnecting, or call
`Session.Close` to end the logical stream deliberately.

`Session.StreamManagement().Resumed` distinguishes an accepted resumption
from a fresh bind. A resumed stream keeps its resource and replays the exact
bytes of its unacknowledged outbound stanzas. An unavailable or refused
resumption discards the old replay queue. The application can use its own
archive recovery and reconciliation on the fresh stream.

The replay queue is limited to 256 stanzas and 4 MiB. If it fills, the send
returns `ErrStreamManagementQueueFull` and the connection closes so the
remaining stanzas can be resumed. Resumption state stays in memory and lasts
for the lifetime of the process.

[XEP-0198]: https://xmpp.org/extensions/xep-0198.html


## License

The package may be used under the terms of the BSD 2-Clause License a copy of
which may be found in the file "[LICENSE]".
Some code in this package has been copied from Go and is used under the terms of
Go's modified BSD license, a copy of which can be found in the [LICENSE-GO]
file.

Unless you explicitly state otherwise, any contribution submitted for inclusion
in the work by you shall be licensed as above, without any additional terms or
conditions.


[docs]: https://pkg.go.dev/mellium.im/xmpp
[CONTRIBUTING.md]: https://mellium.im/docs/CONTRIBUTING
[LICENSE]: https://codeberg.org/mellium/xmpp/src/branch/main/LICENSE
[LICENSE-GO]: https://codeberg.org/mellium/xmpp/src/branch/main/LICENSE-GO
