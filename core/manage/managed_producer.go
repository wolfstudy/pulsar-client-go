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

package manage

import (
	"context"
	"sync"
	"time"

	"github.com/Comcast/pulsar-client-go/core/pub"
	"github.com/Comcast/pulsar-client-go/pkg/api"
	"github.com/Comcast/pulsar-client-go/utils"
)

// ProducerConfig is used to configure a ManagedProducer.
type ProducerConfig struct {
	ClientConfig

	Topic string
	Name  string

	NewProducerTimeout    time.Duration // maximum duration to create Producer, including topic lookup
	InitialReconnectDelay time.Duration // how long to initially wait to reconnect Producer
	MaxReconnectDelay     time.Duration // maximum time to wait to attempt to reconnect Producer
}

// setDefaults returns a modified config with appropriate zero values set to defaults.
func (m ProducerConfig) setDefaults() ProducerConfig {
	if m.NewProducerTimeout <= 0 {
		m.NewProducerTimeout = 5 * time.Second
	}
	if m.InitialReconnectDelay <= 0 {
		m.InitialReconnectDelay = 1 * time.Second
	}
	if m.MaxReconnectDelay <= 0 {
		m.MaxReconnectDelay = 5 * time.Minute
	}

	return m
}

// NewManagedProducer returns an initialized ManagedProducer. It will create and re-create
// a Producer for the given discovery address and topic on a background goroutine.
func NewManagedProducer(cp *ClientPool, cfg ProducerConfig) *ManagedProducer {
	cfg = cfg.setDefaults()

	m := ManagedProducer{
		ClientPool: cp,
		Cfg:        cfg,
		AsyncErrs:  utils.AsyncErrors(cfg.Errs),
		Waitc:      make(chan struct{}),
	}

	go m.manage()

	return &m
}

// ManagedProducer wraps a Producer with re-connect logic.
type ManagedProducer struct {
	ClientPool *ClientPool
	Cfg        ProducerConfig
	AsyncErrs  utils.AsyncErrors

	Mu       sync.RWMutex  // protects following
	Producer *pub.Producer // either producer is nil and wait isn't or vice versa
	Waitc    chan struct{} // if producer is nil, this will unblock when it's been re-set
}

// Send attempts to use the Producer's Send method if available. If not available,
// an error is returned.
func (m *ManagedProducer) Send(ctx context.Context, payload []byte) (*api.CommandSendReceipt, error) {
	for {
		m.Mu.RLock()
		producer := m.Producer
		wait := m.Waitc
		m.Mu.RUnlock()

		if producer != nil {
			return producer.Send(ctx, payload)
		}

		select {
		case <-wait:
			// a new producer was established.
			// Re-enter read-lock to obtain it.
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Set unblocks the "wait" channel (if not nil),
// and sets the producer under lock.
func (m *ManagedProducer) Set(p *pub.Producer) {
	m.Mu.Lock()

	m.Producer = p

	if m.Waitc != nil {
		close(m.Waitc)
		m.Waitc = nil
	}

	m.Mu.Unlock()
}

// Unset creates the "wait" channel (if nil),
// and sets the producer to nil under lock.
func (m *ManagedProducer) Unset() {
	m.Mu.Lock()

	if m.Waitc == nil {
		// allow unset() to be called
		// multiple times by only creating
		// wait chan if its nil
		m.Waitc = make(chan struct{})
	}
	m.Producer = nil

	m.Mu.Unlock()
}

// NewProducer attempts to create a Producer.
func (m *ManagedProducer) NewProducer(ctx context.Context) (*pub.Producer, error) {
	mc, err := m.ClientPool.ForTopic(ctx, m.Cfg.ClientConfig, m.Cfg.Topic)
	if err != nil {
		return nil, err
	}

	client, err := mc.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Create the topic producer. A blank producer name will
	// cause Pulsar to generate a unique name.
	return client.NewProducer(ctx, m.Cfg.Topic, m.Cfg.Name)
}

// Reconnect blocks while a new Producer is created.
func (m *ManagedProducer) Reconnect(initial bool) *pub.Producer {
	retryDelay := m.Cfg.InitialReconnectDelay

	for attempt := 1; ; attempt++ {
		if initial {
			initial = false
		} else {
			<-time.After(retryDelay)
			if retryDelay < m.Cfg.MaxReconnectDelay {
				// double retry delay until we reach the max
				if retryDelay *= 2; retryDelay > m.Cfg.MaxReconnectDelay {
					retryDelay = m.Cfg.MaxReconnectDelay
				}
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), m.Cfg.NewProducerTimeout)
		newProducer, err := m.NewProducer(ctx)
		cancel()
		if err != nil {
			m.AsyncErrs.Send(err)
			continue
		}

		return newProducer
	}
}

// managed Monitors the Producer for conditions
// that require it to be recreated.
func (m *ManagedProducer) manage() {
	defer m.Unset()

	producer := m.Reconnect(true)
	m.Set(producer)

	for {
		select {
		case <-producer.Closed():
		case <-producer.ConnClosed():
		}

		m.Unset()
		producer = m.Reconnect(false)
		m.Set(producer)
	}
}

// Monitor a scoped deferrable lock
func (m *ManagedProducer) Monitor() func() {
	m.Mu.Lock()
	return m.Mu.Unlock
}

// Close producer
func (m *ManagedProducer) Close(ctx context.Context) error {
	defer m.Monitor()()
	return m.Producer.Close(ctx)
}
