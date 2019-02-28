package conn

import (
	"context"
	"fmt"
	"net"
)

// MockPulsarServer emulates a Pulsar server
type MockPulsarServer struct {
	Addr  string
	Errs  chan error
	Conns chan *Conn
}

func NewMockPulsarServer(ctx context.Context) (*MockPulsarServer, error) {
	l, err := net.ListenTCP("tcp4", &net.TCPAddr{
		IP:   net.IPv4zero,
		Port: 0,
	})
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		l.Close()
	}()

	mock := MockPulsarServer{
		Addr:  fmt.Sprintf("pulsar://%s", l.Addr().String()),
		Errs:  make(chan error),
		Conns: make(chan *Conn, 1),
	}

	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				mock.Errs <- err
				return
			}

			// close all connections when
			// context is canceled
			go func() {
				<-ctx.Done()
				c.Close()
			}()

			mock.Conns <- &Conn{
				Rc:      c,
				W:       c,
				Closedc: make(chan struct{}),
			}
		}
	}()

	return &mock, nil
}


