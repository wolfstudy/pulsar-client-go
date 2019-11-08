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

package srv

import (
	"context"

	"github.com/wolfstudy/pulsar-client-go/core/frame"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
)

// NewPinger returns a ready-to-use pinger.
func NewPinger(s frame.CmdSender, dispatcher *frame.Dispatcher) *Pinger {
	return &Pinger{
		s:          s,
		dispatcher: dispatcher,
	}
}

// Pinger is responsible for the PING <-> PONG
// (Keep Alive) interactions.
//
// It responds to all PING requests with a PONG. It also
// enables PINGing the Pulsar server.
//
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#KeepAlive-53utwq
type Pinger struct {
	s          frame.CmdSender
	dispatcher *frame.Dispatcher // used to manage the request/response state
}

// Ping sends a PING message to the Pulsar server, then
// waits for either a PONG response or the context to
// timeout.
func (p *Pinger) Ping(ctx context.Context) error {
	resp, cancel, err := p.dispatcher.RegisterGlobal()
	if err != nil {
		return err
	}
	defer cancel()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_PING.Enum(),
		Ping: &api.CommandPing{},
	}

	if err := p.s.SendSimpleCmd(cmd); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-resp:
		// PONG received
	}

	return nil
}

// HandlePing responds immediately with a PONG message.
//
// A valid client implementation must respond to PINGs
// with PONGs, and may optionally send periodic pings.
func (p *Pinger) HandlePing(msgType api.BaseCommand_Type, msg *api.CommandPing) error {
	cmd := api.BaseCommand{
		Type: api.BaseCommand_PONG.Enum(),
		Pong: &api.CommandPong{},
	}

	return p.s.SendSimpleCmd(cmd)
}
