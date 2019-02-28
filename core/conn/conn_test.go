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
	"context"
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/wolfstudy/pulsar-client-go/core/frame"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
)

// mockReadCloser wraps a io.Reader with a no-op Close method.
// It satisfies the io.ReadCloser interface.
type mockReadCloser struct {
	io.Reader

	closed   uint32 // atomically updated number of times Close() was called
	closeErr error  // return value of Close()
}

func (m *mockReadCloser) Close() error {
	atomic.AddUint32(&m.closed, 1)
	return m.closeErr
}

func TestConn_Read(t *testing.T) {
	f := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_CONNECTED.Enum(),
			Connected: &api.CommandConnected{
				ProtocolVersion: proto.Int32(9),
				ServerVersion:   proto.String("Pulsar Server"),
			},
		},
	}

	var b bytes.Buffer
	if err := f.Encode(&b); err != nil {
		t.Fatal(err)
	}

	c := Conn{
		Rc: &mockReadCloser{
			Reader: &b,
		},
		Closedc: make(chan struct{}),
	}

	var gotFrames []frame.Frame
	handler := func(f frame.Frame) { gotFrames = append(gotFrames, f) }
	// read should read the frame, then reach
	// and return EOF
	if err := c.Read(handler); err != io.EOF {
		t.Fatalf("core.read() err = %v; expected EOF", err)
	}

	if got, expected := len(gotFrames), 1; got != expected {
		t.Fatalf("core.read() read %d frame(s); expected %d", got, expected)
	}

	if got := gotFrames[0]; !got.Equal(f) {
		t.Fatalf("got frame:\n%+v\nnot equal to expected frame:\n%+v\n", got, f)
	} else {
		t.Logf("got frame:\n%+v", got)
	}
}

func TestConn_Close(t *testing.T) {
	c := Conn{
		Rc: &mockReadCloser{
			Reader: new(bytes.Buffer),
		},
		Closedc: make(chan struct{}),
	}

	// no-op
	handler := func(f frame.Frame) {}

	// read should reach and return EOF
	err := c.Read(handler)
	if err != io.EOF {
		t.Fatalf("core.read() err = %v; expected EOF", err)
	}
	t.Logf("core.read() err (expected) = %v", err)

	// closed should unblock
	select {
	case <-c.Closed():
	default:
		t.Fatal("core.closed() is blocking; expected to unblock")
	}
}

func TestConn_GarbageInput(t *testing.T) {
	mrc := &mockReadCloser{
		Reader: bytes.NewBufferString("this isn't a valid Pulsar frame"),
	}
	c := Conn{
		Rc:      mrc,
		Closedc: make(chan struct{}),
	}

	var gotFrames []frame.Frame
	handler := func(f frame.Frame) {
		gotFrames = append(gotFrames, f)
	}

	// read should not be able to decode the frame,
	// so it should close the connection and return
	err := c.Read(handler)
	if err == nil {
		t.Fatalf("core.read() err = %v; expected non-nil", err)
	}
	t.Logf("core.read() err (expected) = %v", err)

	if got, expected := len(gotFrames), 0; got != expected {
		t.Fatalf("core.read() read %d frame(s); expected %d", got, expected)
	}

	if got, expected := atomic.LoadUint32(&mrc.closed), uint32(1); got != expected {
		t.Fatalf("core.rc.Close() called %d times; expected %d", got, expected)
	}
}

func TestConn_TimeoutReader(t *testing.T) {
	f := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_CONNECTED.Enum(),
			Connected: &api.CommandConnected{
				ProtocolVersion: proto.Int32(9),
				ServerVersion:   proto.String("Pulsar Server"),
			},
		},
	}

	var b bytes.Buffer
	if err := f.Encode(&b); err != nil {
		t.Fatal(err)
	}

	// the Reader here will return ErrTimeout
	// on the second call to Read.
	mrc := &mockReadCloser{
		Reader: iotest.TimeoutReader(&b),
	}
	c := Conn{
		Rc:      mrc,
		Closedc: make(chan struct{}),
	}

	var gotFrames []frame.Frame
	handler := func(f frame.Frame) {
		gotFrames = append(gotFrames, f)
	}

	// read should attempt to read the frame,
	// then reach and return ErrTimeout
	err := c.Read(handler)
	if err != iotest.ErrTimeout {
		t.Fatalf("core.read() err = %v; expected %v", err, iotest.ErrTimeout)
	}
	t.Logf("core.read() err (expected) = %v", err)

	if got, expected := len(gotFrames), 0; got != expected {
		t.Fatalf("core.read() read %d frame(s); expected %d", got, expected)
	}
}

func TestConn_Read_SlowSrc(t *testing.T) {
	f := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type: api.BaseCommand_CONNECTED.Enum(),
			Connected: &api.CommandConnected{
				ProtocolVersion: proto.Int32(9),
				ServerVersion:   proto.String("Pulsar Server"),
			},
		},
	}

	var b bytes.Buffer
	if err := f.Encode(&b); err != nil {
		t.Fatal(err)
	}

	c := Conn{
		// OneByteReader returns a single byte per read,
		// regardless of how big its input buffer is.
		Rc: &mockReadCloser{
			Reader: iotest.OneByteReader(&b),
		},
		Closedc: make(chan struct{}),
	}

	var gotFrames []frame.Frame
	handler := func(f frame.Frame) {
		gotFrames = append(gotFrames, f)
	}
	// read should read the frame, then reach
	// and return EOF
	if err := c.Read(handler); err != io.EOF {
		t.Fatalf("core.read() err = %v; expected EOF", err)
	}

	if got, expected := len(gotFrames), 1; got != expected {
		t.Fatalf("core.read() read %d frame(s); expected %d", got, expected)
	}

	if got := gotFrames[0]; !got.Equal(f) {
		t.Fatalf("got frame:\n%+v\nnot equal to expected frame:\n%+v\n", got, f)
	} else {
		t.Logf("got frame:\n%+v", got)
	}
}

func TestConn_Read_MutliFrame(t *testing.T) {
	N := 16

	// create input frames
	frames := make([]frame.Frame, N)
	for i := range frames {
		frames[i] = frame.Frame{
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_MESSAGE.Enum(),
				Message: &api.CommandMessage{
					ConsumerId: proto.Uint64(uint64(i)),
					MessageId: &api.MessageIdData{
						LedgerId: proto.Uint64(uint64(i)),
						EntryId:  proto.Uint64(uint64(i)),
					},
				},
			},
			Metadata: &api.MessageMetadata{
				ProducerName: proto.String(fmt.Sprintf("test %d", i)),
				SequenceId:   proto.Uint64(0),
				PublishTime:  proto.Uint64(1513027321000),
			},
			Payload: []byte(fmt.Sprintf("test message %d", i)),
		}
	}

	// write frames to read buffer
	var b bytes.Buffer
	for _, f := range frames {
		if err := f.Encode(&b); err != nil {
			t.Fatal(err)
		}
	}

	c := Conn{
		Rc: &mockReadCloser{
			Reader: &b,
		},
		Closedc: make(chan struct{}),
	}

	var gotFrames []frame.Frame
	handler := func(f frame.Frame) { gotFrames = append(gotFrames, f) }
	// read should read the frames, then reach
	// and return EOF
	if err := c.Read(handler); err != io.EOF {
		t.Fatalf("core.read() err = %v; expected EOF", err)
	}

	if got, expected := len(gotFrames), len(frames); got != expected {
		t.Fatalf("core.read() read %d frame(s); expected %d", got, expected)
	}

	for i, got := range gotFrames {
		if expected := frames[i]; !got.Equal(expected) {
			t.Fatalf("got frame:\n%+v\nnot equal to expected frame:\n%+v\n", got, expected)
		} else {
			t.Logf("got frame:\n%+v", got)
		}
	}
}

func TestConn_writeFrame(t *testing.T) {
	N := 64

	// mapping of frame payload to frame.
	// Since they will be written in an undetermined ordered,
	// this helps look them up and match them.
	frames := make([]frame.Frame, N)
	for i := 0; i < N; i++ {
		payload := fmt.Sprintf("%02d - test message", i) // test expects that payload as string sorts properly
		frames[i] = frame.Frame{
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_MESSAGE.Enum(),
				Message: &api.CommandMessage{
					ConsumerId: proto.Uint64(uint64(i)),
					MessageId: &api.MessageIdData{
						LedgerId: proto.Uint64(uint64(i)),
						EntryId:  proto.Uint64(uint64(i)),
					},
				},
			},
			Metadata: &api.MessageMetadata{
				ProducerName: proto.String(fmt.Sprintf("test %d", i)),
				SequenceId:   proto.Uint64(0),
				PublishTime:  proto.Uint64(1513027321000),
			},
			Payload: []byte(payload),
		}
	}

	// same buffer is used for reads and writes
	var rw bytes.Buffer
	c := Conn{
		Rc: &mockReadCloser{
			Reader: &rw,
		},
		W:       &rw,
		Closedc: make(chan struct{}),
	}

	// write the frames in parallel (order will
	// be non-deterministic).
	t.Run("writeFrame", func(t *testing.T) {
		for _, f := range frames {
			f := f
			t.Run(string(f.Payload), func(t *testing.T) {
				t.Parallel()
				if err := c.writeFrame(&f); err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	var gotFrames []frame.Frame
	handler := func(f frame.Frame) { gotFrames = append(gotFrames, f) }
	// read the encoded frames, which the handler
	// will store in `gotFrames`.
	if err := c.Read(handler); err != io.EOF {
		t.Fatalf("core.read() err = %v; expected EOF", err)
	}

	// ensure that all the expected frames were read

	sort.Slice(gotFrames, func(i, j int) bool {
		return string(gotFrames[i].Payload) < string(gotFrames[j].Payload)
	})

	if got, expected := len(gotFrames), len(frames); got != expected {
		t.Fatalf("read %d frames; expected %d", got, expected)
	}

	for i, f := range frames {
		if got, expected := string(gotFrames[i].Payload), string(f.Payload); got != expected {
			t.Errorf("frame[%d] payload = %q; expected %q", i, got, expected)
		}
	}
}

func TestConn_TCP_Read(t *testing.T) {
	testFrames := map[string]frame.Frame{
		"ping": {
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_PING.Enum(),
				Ping: &api.CommandPing{},
			},
		},
		"pong": {
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_PONG.Enum(),
				Pong: &api.CommandPong{},
			},
		},
		"message": {
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_MESSAGE.Enum(),
				Message: &api.CommandMessage{
					ConsumerId: proto.Uint64(1234),
					MessageId: &api.MessageIdData{
						EntryId:  proto.Uint64(84),
						LedgerId: proto.Uint64(42),
					},
				},
			},
			Metadata: &api.MessageMetadata{
				ProducerName: proto.String("test"),
				SequenceId:   proto.Uint64(12),
				PublishTime:  proto.Uint64(998877),
			},
			Payload: []byte("hello!"),
		},
	}

	// create a mock Pulsar server
	srvCtx, closeSrv := context.WithCancel(context.Background())
	defer closeSrv()
	srv, err := NewMockPulsarServer(srvCtx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Mock server addr: %q", srv.Addr)

	// create a core connected to the mock Pulsar server
	c, err := NewTCPConn(srv.Addr, time.Second)
	if err != nil {
		t.Fatalf("newTCPConn(%q) err = %v; nil expected", srv.Addr, err)
	}

	// wait for the Pulsar server to accept the connection
	var srvConn *Conn
	select {
	case srvConn = <-srv.Conns:
		t.Log("server received connection")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server to receive connection")
	}

	// start reading frames off the core
	received := make(chan frame.Frame, len(testFrames))
	readErr := make(chan error, 1)
	go func() {
		readErr <- c.Read(func(f frame.Frame) { received <- f })
	}()

	// send frames from the Pulsar server to the core, and
	// ensure that they are received by the core as expected
	for name, expected := range testFrames {
		if err := srvConn.writeFrame(&expected); err != nil {
			t.Fatalf("writeFrame(%q) err = %v; nil expected", name, err)
		}

		select {
		case err := <-readErr:
			t.Fatalf("unexpected core.read() err = %v", err)
		case err := <-srv.Errs:
			t.Fatalf("unexpected mockPulsarServer err = %v", err)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for frame %q", name)
		case got := <-received:
			if !got.Equal(expected) {
				t.Fatalf("unexpected frame received:\n%+v\nexpected %q:\n%+v", got, name, expected)
			}
			t.Logf("frame %q received:\n%+v", name, got)
		}
	}
}

func TestConn_TCP_Write(t *testing.T) {
	testFrames := map[string]frame.Frame{
		"ping": {
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_PING.Enum(),
				Ping: &api.CommandPing{},
			},
		},
		"pong": {
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_PONG.Enum(),
				Pong: &api.CommandPong{},
			},
		},
		"message": {
			BaseCmd: &api.BaseCommand{
				Type: api.BaseCommand_MESSAGE.Enum(),
				Message: &api.CommandMessage{
					ConsumerId: proto.Uint64(1234),
					MessageId: &api.MessageIdData{
						EntryId:  proto.Uint64(84),
						LedgerId: proto.Uint64(42),
					},
				},
			},
			Metadata: &api.MessageMetadata{
				ProducerName: proto.String("test"),
				SequenceId:   proto.Uint64(12),
				PublishTime:  proto.Uint64(998877),
			},
			Payload: []byte("hello!"),
		},
	}

	// create a mock Pulsar server
	srvCtx, closeSrv := context.WithCancel(context.Background())
	defer closeSrv()
	srv, err := NewMockPulsarServer(srvCtx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Mock server addr: %q", srv.Addr)

	// create a core connected to the mock Pulsar server
	c, err := NewTCPConn(srv.Addr, time.Second)
	if err != nil {
		t.Fatalf("newTCPConn(%q) err = %v; nil expected", srv.Addr, err)
	}
	defer c.Close()

	// wait for the Pulsar server to accept the connection
	var srvConn *Conn
	select {
	case srvConn = <-srv.Conns:
		t.Log("server received connection")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server to receive connection")
	}

	srvReceived := make(chan frame.Frame)
	go func() {
		defer close(srvReceived)
		srvConn.Read(func(f frame.Frame) {
			srvReceived <- f
		})
	}()

	// send frames from the core to the Pulsar server, and
	// ensure that they are received by the server as expected
	for name, expected := range testFrames {
		var err error
		if expected.Metadata == nil {
			err = c.SendSimpleCmd(*expected.BaseCmd)
		} else {
			err = c.SendPayloadCmd(*expected.BaseCmd, *expected.Metadata, expected.Payload)
		}
		if err != nil {
			t.Fatalf("core.send(%q) err = %v; expected nil", name, err)
		}

		select {
		case err := <-srv.Errs:
			t.Fatalf("unexpected mockPulsarServer err = %v", err)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for frame %q", name)
		case got, ok := <-srvReceived:
			if !ok {
				t.Fatal("server core.read() unexpectedly closed")
			}
			if !got.Equal(expected) {
				t.Fatalf("unexpected frame received:\n%+v\nexpected %q:\n%+v", got, name, expected)
			}
			t.Logf("frame %q received:\n%+v", name, got)
		}
	}
}

func TestConn_TCP_ReadLocalClose(t *testing.T) {
	// create a mock Pulsar server
	srvCtx, closeSrv := context.WithCancel(context.Background())
	defer closeSrv()
	srv, err := NewMockPulsarServer(srvCtx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Mock server addr: %q", srv.Addr)

	// create a core connected to the mock Pulsar server
	c, err := NewTCPConn(srv.Addr, time.Second)
	if err != nil {
		t.Fatalf("newTCPConn(%q) err = %v; nil expected", srv.Addr, err)
	}
	defer c.Close()

	// wait for the Pulsar server to accept the connection
	var srvConn *Conn
	select {
	case srvConn = <-srv.Conns:
		t.Log("server received connection")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server to receive connection")
	}

	// start reading from the remote core.
	// send read errors to srvConnReadErr chan
	srvConnReadErr := make(chan error, 1)
	go func() {
		srvConnReadErr <- srvConn.Read(func(f frame.Frame) {})
	}()

	// start reading from the core.
	// send read errors to readErr chan
	readErr := make(chan error, 1)
	go func() {
		readErr <- c.Read(func(f frame.Frame) {})
	}()

	// close the connection from the local core's end
	if err := c.Close(); err != nil {
		t.Fatalf("close() err = %v; expected nil", err)
	}

	select {
	case err := <-readErr:
		if err == nil {
			t.Fatalf("read() err = %v; expected non-nil", err)
		}
		t.Logf("read() err (expected) = %v", err)

		// ensure the connection was closed on the remote end too
		select {
		case err := <-srvConnReadErr:
			t.Logf("remote core read() err (expected) = %v", err)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for remote read() to unblock")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for read() to unblock")
	}
}

func TestConn_TCP_ReadRemoteClose(t *testing.T) {
	// create a mock Pulsar server
	srvCtx, closeSrv := context.WithCancel(context.Background())
	defer closeSrv()
	srv, err := NewMockPulsarServer(srvCtx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Mock server addr: %q", srv.Addr)

	// create a core connected to the mock Pulsar server
	c, err := NewTCPConn(srv.Addr, time.Second)
	if err != nil {
		t.Fatalf("newTCPConn(%q) err = %v; nil expected", srv.Addr, err)
	}
	defer c.Close()

	// wait for the Pulsar server to accept the connection
	var srvConn *Conn
	select {
	case srvConn = <-srv.Conns:
		t.Log("server received connection")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server to receive connection")
	}

	// start reading from the core.
	// Send read errors to readErr chan
	readErr := make(chan error, 1)
	go func() {
		readErr <- c.Read(func(f frame.Frame) {})
	}()

	// server initiated connection closure
	if err := srvConn.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-c.Closed():
		t.Log("server received re-connection")
	case <-time.After(time.Second):
		t.Fatal("core.closed() blocked; expected to unblock after connection closure")
	}

	select {
	case err := <-readErr:
		if err != io.EOF {
			t.Fatalf("read() err = %v; expected io.EOF", err)
		}
		t.Logf("read() err (expected) = %v", err)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for read() to unblock")
	}
}

func TestConn_TCP_SendOnClosed(t *testing.T) {
	// create a mock Pulsar server
	srvCtx, closeSrv := context.WithCancel(context.Background())
	defer closeSrv()
	srv, err := NewMockPulsarServer(srvCtx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Mock server addr: %q", srv.Addr)

	// create a core connected to the mock Pulsar server
	c, err := NewTCPConn(srv.Addr, time.Second)
	if err != nil {
		t.Fatalf("newTCPConn(%q) err = %v; nil expected", srv.Addr, err)
	}
	defer c.Close()

	// wait for the Pulsar server to accept the connection
	select {
	case <-srv.Conns:
		t.Log("server received connection")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server to receive connection")
	}

	// close the local core
	if err := c.Close(); err != nil {
		t.Fatalf("close() err = %v; expected nil", err)
	}

	// attempt to send a message on a closed connection
	ping := api.BaseCommand{
		Type: api.BaseCommand_PING.Enum(),
		Ping: &api.CommandPing{},
	}
	if err := c.SendSimpleCmd(ping); err == nil {
		t.Fatalf("sendSimpleCmd() err = %v; expected non-nil for a closed core", err)
	} else {
		t.Logf("sendSimpleCmd() err (expected for a closed core) = %v", err)
	}
}
