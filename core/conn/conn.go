// Copyright 2018 Comcast Cable Communications Management, LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conn

import (
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/wolfstudy/pulsar-client-go/core/frame"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
)

// NewTCPConn creates a core using a TCPv4 connection to the given
// (pulsar server) address.
func NewTCPConn(addr string, timeout time.Duration) (*Conn, error) {
	addr = strings.TrimPrefix(addr, "pulsar://")

	d := net.Dialer{
		DualStack: false,
		Timeout:   timeout,
	}
	c, err := d.Dial("tcp4", addr)
	if err != nil {
		return nil, err
	}

	return &Conn{
		Rc:      c,
		W:       c,
		Closedc: make(chan struct{}),
	}, nil
}

// NewTLSConn creates a core using a TCPv4+TLS connection to the given
// (pulsar server) address.
func NewTLSConn(addr string, tlsCfg *tls.Config, timeout time.Duration) (*Conn, error) {
	addr = strings.TrimPrefix(addr, "pulsar://")

	d := net.Dialer{
		DualStack: false,
		Timeout:   timeout,
	}
	c, err := tls.DialWithDialer(&d, "tcp4", addr, tlsCfg)
	if err != nil {
		return nil, err
	}

	return &Conn{
		Rc:      c,
		W:       c,
		Closedc: make(chan struct{}),
	}, nil
}

// Conn is responsible for writing and reading
// Frames to and from the underlying connection (r and w).
type Conn struct {
	Rc io.ReadCloser

	Wmu sync.Mutex // protects w to ensure frames aren't interleaved
	W   io.Writer

	Cmu      sync.Mutex // protects following
	IsClosed bool
	Closedc  chan struct{}
}

// Close closes the underlaying connection.
// This will cause read() to unblock and return
// an error. It will also cause the closed channel
// to unblock.
func (c *Conn) Close() error {
	c.Cmu.Lock()
	defer c.Cmu.Unlock()

	if c.IsClosed {
		return nil
	}

	err := c.Rc.Close()
	close(c.Closedc)
	c.IsClosed = true

	return err
}

// Closed returns a channel that will unblock
// when the connection has been closed and is no
// longer usable.
func (c *Conn) Closed() <-chan struct{} {
	return c.Closedc
}

// Read blocks while it reads from r until an error occurs.
// It passes all frames to the provided handler, sequentially
// and from the same goroutine as called with. Any error encountered
// will close the connection. Also if close() is called,
// read() will unblock. Once read returns, the core should
// be considered unusable.
func (c *Conn) Read(frameHandler func(f frame.Frame)) error {
	for {
		var f frame.Frame
		if err := f.Decode(c.Rc); err != nil {
			// It's very possible that the connection is already closed at this
			// point, since any connection closed errors would bubble up
			// from Decode. But just in case it's a decode error (bad data for example),
			// we attempt to close the connection. Any error is ignored
			// since the Decode error is the primary one.
			_ = c.Close()
			return err
		}
		frameHandler(f)
	}
}

// SendSimpleCmd writes a "simple" frame to the wire. It
// is safe to use concurrently.
func (c *Conn) SendSimpleCmd(cmd api.BaseCommand) error {
	return c.writeFrame(&frame.Frame{
		BaseCmd: &cmd,
	})
}

// SendPayloadCmd writes a "payload" frame to the wire. It
// is safe to use concurrently.
func (c *Conn) SendPayloadCmd(cmd api.BaseCommand, metadata api.MessageMetadata, payload []byte) error {
	return c.writeFrame(&frame.Frame{
		BaseCmd:  &cmd,
		Metadata: &metadata,
		Payload:  payload,
	})
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, frame.MaxFrameSize))
	},
}

// writeFrame encodes the given frame and writes
// it to the wire in a thread-safe manner.
func (c *Conn) writeFrame(f *frame.Frame) error {
	b := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(b)
	b.Reset()

	if err := f.Encode(b); err != nil {
		return err
	}

	c.Wmu.Lock()
	_, err := b.WriteTo(c.W)
	c.Wmu.Unlock()

	return err
}
