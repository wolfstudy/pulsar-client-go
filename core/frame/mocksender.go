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

package frame

import (
	"sync"

	"github.com/Comcast/pulsar-client-go/pkg/api"
)

// CmdSender is an interface that is capable of sending
// commands to Pulsar. It allows abstraction of a core.
type CmdSender interface {
	SendSimpleCmd(cmd api.BaseCommand) error
	SendPayloadCmd(cmd api.BaseCommand, metadata api.MessageMetadata, payload []byte) error
	Closed() <-chan struct{} // closed unblocks when the connection has been closed
}

// MockSender implements the sender interface
type MockSender struct {
	Mu      sync.Mutex // protects following
	Frames  []Frame
	Closedc chan struct{}
}

func (m *MockSender) GetFrames() []Frame {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	cp := make([]Frame, len(m.Frames))
	copy(cp, m.Frames)

	return cp
}

func (m *MockSender) SendSimpleCmd(cmd api.BaseCommand) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.Frames = append(m.Frames, Frame{
		BaseCmd: &cmd,
	})

	return nil
}

func (m *MockSender) SendPayloadCmd(cmd api.BaseCommand, metadata api.MessageMetadata, payload []byte) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	m.Frames = append(m.Frames, Frame{
		BaseCmd:  &cmd,
		Metadata: &metadata,
		Payload:  payload,
	})

	return nil
}

func (m *MockSender) Closed() <-chan struct{} {
	return m.Closedc
}
