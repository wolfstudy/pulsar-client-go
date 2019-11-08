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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wolfstudy/pulsar-client-go/core/pub"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
	"github.com/wolfstudy/pulsar-client-go/pkg/log"
	"github.com/wolfstudy/pulsar-client-go/utils"
)

// ManagedProducerConfig is used to configure a ManagedProducer.
type ManagedProducerConfig struct {
	ManagedClientConfig

	Topic      string
	Name       string
	HashScheme pub.HashingScheme
	Router     pub.MessageRoutingMode

	NewProducerTimeout    time.Duration // maximum duration to create Producer, including topic lookup
	InitialReconnectDelay time.Duration // how long to initially wait to reconnect Producer
	MaxReconnectDelay     time.Duration // maximum time to wait to attempt to reconnect Producer
}

// setDefaults returns a modified config with appropriate zero values set to defaults.
func (m ManagedProducerConfig) setDefaults() ManagedProducerConfig {
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
func NewManagedProducer(cp *ClientPool, cfg ManagedProducerConfig) *ManagedProducer {
	cfg = cfg.setDefaults()

	m := ManagedProducer{
		clientPool: cp,
		cfg:        cfg,
		asyncErrs:  utils.AsyncErrors(cfg.Errs),
		waitc:      make(chan struct{}),
	}

	go m.manage()

	return &m
}

// ManagedProducer wraps a Producer with re-connect logic.
type ManagedProducer struct {
	clientPool *ClientPool
	cfg        ManagedProducerConfig
	asyncErrs  utils.AsyncErrors

	mu       sync.RWMutex  // protects following
	producer *pub.Producer // either producer is nil and wait isn't or vice versa
	waitc    chan struct{} // if producer is nil, this will unblock when it's been re-set
}

// Send attempts to use the Producer's Send method if available. If not available,
// an error is returned.
func (m *ManagedProducer) Send(ctx context.Context, payload []byte, msgKey string) (*api.CommandSendReceipt, error) {
	for {
		m.mu.RLock()
		producer := m.producer
		wait := m.waitc
		m.mu.RUnlock()

		if producer != nil {
			return producer.Send(ctx, payload, msgKey)
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
	m.mu.Lock()

	m.producer = p

	if m.waitc != nil {
		close(m.waitc)
		m.waitc = nil
	}

	m.mu.Unlock()
}

// Unset creates the "wait" channel (if nil),
// and sets the producer to nil under lock.
func (m *ManagedProducer) Unset() {
	m.mu.Lock()

	if m.waitc == nil {
		// allow unset() to be called
		// multiple times by only creating
		// wait chan if its nil
		m.waitc = make(chan struct{})
	}
	m.producer = nil

	m.mu.Unlock()
}

// NewProducer attempts to create a Producer.
func (m *ManagedProducer) NewProducer(ctx context.Context) (*pub.Producer, error) {
	mc, err := m.clientPool.ForTopic(ctx, m.cfg.ManagedClientConfig, m.cfg.Topic)
	if err != nil {
		return nil, err
	}

	client, err := mc.Get(ctx)
	if err != nil {
		return nil, err
	}

	// Create the topic producer. A blank producer name will
	// cause Pulsar to generate a unique name.
	return client.NewProducer(ctx, m.cfg.Topic, m.cfg.Name)
}

// Reconnect blocks while a new Producer is created.
func (m *ManagedProducer) Reconnect(initial bool) *pub.Producer {
	retryDelay := m.cfg.InitialReconnectDelay

	for attempt := 1; ; attempt++ {
		if initial {
			initial = false
		} else {
			<-time.After(retryDelay)
			if retryDelay < m.cfg.MaxReconnectDelay {
				// double retry delay until we reach the max
				if retryDelay *= 2; retryDelay > m.cfg.MaxReconnectDelay {
					retryDelay = m.cfg.MaxReconnectDelay
				}
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), m.cfg.NewProducerTimeout)
		newProducer, err := m.NewProducer(ctx)
		cancel()
		if err != nil {
			m.asyncErrs.Send(err)
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
	m.mu.Lock()
	return m.mu.Unlock
}

// Close producer
func (m *ManagedProducer) Close(ctx context.Context) error {
	defer m.Monitor()()
	return m.producer.Close(ctx)
}

func NewManagedPartitionProducer(cp *ClientPool, cfg ManagedProducerConfig) (*ManagedPartitionProducer, error) {
	cfg = cfg.setDefaults()
	ctx := context.Background()

	m := ManagedPartitionProducer{
		clientPool:       cp,
		cfg:              cfg,
		asyncErrs:        utils.AsyncErrors(cfg.Errs),
		waitc:            make(chan struct{}),
		managedProducers: make([]*ManagedProducer, 0),
	}

	managedClient := cp.Get(cfg.ManagedClientConfig)
	client, err := managedClient.Get(ctx)
	if err != nil {
		log.Errorf("create client error:%s", err.Error())
		return nil, err
	}
	res, err := client.discoverer.PartitionedMetadata(ctx, cfg.Topic)
	if err != nil {
		log.Errorf("get partition metadata error:%s", err.Error())
		return nil, err
	}
	numPartitions := res.GetPartitions()
	m.numPartitions = numPartitions
	topicName := cfg.Topic
	for i := 0; uint32(i) < numPartitions; i++ {
		cfg.Topic = fmt.Sprintf("%s-partition-%d", topicName, i)
		m.managedProducers = append(m.managedProducers, NewManagedProducer(cp, cfg))
	}

	var router pub.MessageRouter
	if m.cfg.Router == pub.UseSinglePartition {
		router = &pub.SinglePartitionRouter{
			Partition: numPartitions,
		}
	} else {
		router = &pub.RoundRobinRouter{
			Counter: 0,
		}
	}

	m.messageRouter = router

	return &m, nil
}

type ManagedPartitionProducer struct {
	clientPool *ClientPool
	cfg        ManagedProducerConfig
	asyncErrs  utils.AsyncErrors

	mu               sync.RWMutex  // protects following
	waitc            chan struct{} // if producer is nil, this will unblock when it's been re-set
	managedProducers []*ManagedProducer
	messageRouter    pub.MessageRouter
	numPartitions    uint32
}

func (m *ManagedPartitionProducer) Send(ctx context.Context, payload []byte, msgKey string) (*api.CommandSendReceipt, error) {
	partition := m.messageRouter.ChoosePartition(msgKey, m.numPartitions)
	log.Debugf("choose partition is: %d, msg payload is: %s, msg key is: %s", partition, string(payload), msgKey)

	return m.managedProducers[partition].Send(ctx, payload, msgKey)
}

func (m *ManagedPartitionProducer) Close(ctx context.Context) error {
	var errMsg string
	for _, producer := range m.managedProducers {
		if err := producer.Close(ctx); err != nil {
			errMsg += fmt.Sprintf("topic %s, name %s: %s ", producer.cfg.Topic, producer.cfg.Name, err.Error())
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}
