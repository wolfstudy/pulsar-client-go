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

	"github.com/wolfstudy/pulsar-client-go/core/msg"
	"github.com/wolfstudy/pulsar-client-go/core/sub"
	"github.com/wolfstudy/pulsar-client-go/pkg/log"
	"github.com/wolfstudy/pulsar-client-go/utils"
)

// SubscriptionMode represents Pulsar's three subscription models
type SubscriptionMode int

const (
	// SubscriptionModeExclusive , only one consumer can be bound to a subscription.
	// If more than one consumer attempts to subscribe to the topic in the same way,
	// the consumer will receive an error.
	SubscriptionModeExclusive SubscriptionMode = iota + 1 // 1
	// SubscriptionModeShard In shared or round robin mode,
	// multiple consumers can be bound to the same subscription.
	// Messages are distributed to different consumers via the round robin polling mechanism,
	// and each message is only distributed to one consumer.
	// When the consumer disconnects, all messages sent to him
	// but not confirmed will be rescheduled and distributed to other surviving consumers.
	SubscriptionModeShard  // 2

	// SubscriptionModeFailover multiple consumers can be bound to the same subscription.
	// Consumers will be sorted in lexicographic order,
	// and the first consumer is initialized to the only consumer who accepts the message.
	// This consumer is called the master consumer.
	// When the master consumer is disconnected,
	// all messages (unconfirmed and subsequently entered) will be distributed to the next consumer in the queue.
	SubscriptionModeFailover  // 3

	SubscriptionModeKeyShared  //4
)

// ErrorInvalidSubMode When SubscriptionMode is not one of SubscriptionModeExclusive, SubscriptionModeShard, SubscriptionModeFailover
var ErrorInvalidSubMode = errors.New("invalid subscription mode")

// ConsumerConfig is used to configure a ManagedConsumer.
type ConsumerConfig struct {
	ClientConfig

	Topic     string
	Name      string           // subscription name
	SubMode   SubscriptionMode // SubscriptionMode
	Earliest  bool             // if true, subscription cursor set to beginning
	QueueSize int              // number of messages to buffer before dropping messages

	NewConsumerTimeout    time.Duration // maximum duration to create Consumer, including topic lookup
	InitialReconnectDelay time.Duration // how long to initially wait to reconnect Producer
	MaxReconnectDelay     time.Duration // maximum time to wait to attempt to reconnect Producer

	AckTimeoutMillis time.Duration
}

// SetDefaults returns a modified config with appropriate zero values set to defaults.
func (m ConsumerConfig) SetDefaults() ConsumerConfig {
	if m.NewConsumerTimeout <= 0 {
		m.NewConsumerTimeout = 5 * time.Second
	}
	if m.InitialReconnectDelay <= 0 {
		m.InitialReconnectDelay = 1 * time.Second
	}
	if m.MaxReconnectDelay <= 0 {
		m.MaxReconnectDelay = 5 * time.Minute
	}
	// unbuffered queue not allowed
	if m.QueueSize <= 0 {
		m.QueueSize = 128
	}

	return m
}

// NewManagedConsumer returns an initialized ManagedConsumer. It will create and recreate
// a Consumer for the given discovery address and topic on a background goroutine.
func NewManagedConsumer(cp *ClientPool, cfg ConsumerConfig) *ManagedConsumer {
	cfg = cfg.SetDefaults()

	m := ManagedConsumer{
		clientPool: cp,
		cfg:        cfg,
		asyncErrs:  utils.AsyncErrors(cfg.Errs),
		queue:      make(chan msg.Message, cfg.QueueSize),
		waitc:      make(chan struct{}),
	}

	go m.manage()

	return &m
}

// NewManagedConsumer returns an initialized ManagedConsumer. It will create and recreate
// a Consumer for the given discovery address and topic on a background goroutine.
func NewPartitionManagedConsumer(cp *ClientPool, cfg ConsumerConfig) (*ManagedPartitionConsumer, error) {
	cfg = cfg.SetDefaults()
	ctx := context.Background()

	mpc := ManagedPartitionConsumer{
		clientPool: cp,
		cfg:        cfg,
		asyncErrs:  utils.AsyncErrors(cfg.Errs),
		queue:      make(chan msg.Message, cfg.QueueSize),
		waitc:      make(chan struct{}),
		MConsumer:  make([]*ManagedConsumer, 10),
	}

	manageClient := cp.Get(cfg.ClientConfig)

	client, err := manageClient.Get(ctx)
	if err != nil {
		log.Errorf("create client error:%s", err.Error())
		return nil, err
	}

	res, err := client.Discoverer.PartitionedMetadata(ctx, cfg.Topic)
	if err != nil {
		log.Errorf("get partition metadata error:%s", err.Error())
		return nil, err
	}
	numPartitions := res.GetPartitions()

	for i := 0; uint32(i) < numPartitions; i++ {
		cfg.Topic = fmt.Sprintf("%s-partition-%d", cfg.Topic, i)
		mpc.MConsumer = append(mpc.MConsumer, NewManagedConsumer(cp, cfg))
	}






	return &mpc, nil
}

type ManagedPartitionConsumer struct {
	clientPool *ClientPool
	cfg        ConsumerConfig
	asyncErrs  utils.AsyncErrors

	queue chan msg.Message

	mu        sync.RWMutex // protects following
	waitc     chan struct{}
	MConsumer []*ManagedConsumer
}

// ManagedConsumer wraps a Consumer with reconnect logic.
type ManagedConsumer struct {
	clientPool *ClientPool
	cfg        ConsumerConfig
	asyncErrs  utils.AsyncErrors

	queue chan msg.Message

	mu       sync.RWMutex          // protects following
	consumer sub.ConsumerInterface // either consumer is nil and wait isn't or vice versa
	waitc    chan struct{}         // if consumer is nil, this will unblock when it's been re-set
}


func (m *ManagedPartitionConsumer) Receive(ctx context.Context) (msg.Message, error) {
	for {





		m.mu.RLock()
		consumer := m.consumer
		wait := m.waitc
		m.mu.RUnlock()

		if consumer == nil {
			select {
			case <-wait:
				// a new consumer was established.
				// Re-enter read-lock to obtain it.
				continue
			case <-ctx.Done():
				return msg.Message{}, ctx.Err()
			}
		}

		// TODO: determine when, if ever, to call
		// consumer.RedeliverOverflow

		if len(m.queue) < 1 {
			if err := consumer.Flow(1); err != nil {
				return msg.Message{}, err
			}
		}

		select {
		case msg, ok := <-m.queue:
			if ok {
				if consumer.GetUnAckTracker() != nil {
					consumer.GetUnAckTracker().Add(msg.Msg.MessageId)
				}
				return msg, nil
			}

		case <-ctx.Done():
			return msg.Message{}, ctx.Err()

		case <-consumer.Closed():
			return msg.Message{}, errors.New("consumer closed")

		case <-consumer.ConnClosed():
			return msg.Message{}, errors.New("consumer connection closed")
		}

	}
}


// Ack acquires a consumer and Sends an ACK message for the given message.
func (m *ManagedConsumer) Ack(ctx context.Context, msg msg.Message) error {
	for {
		m.mu.RLock()
		consumer := m.consumer
		wait := m.waitc
		m.mu.RUnlock()

		if consumer == nil {
			select {
			case <-wait:
				// a new consumer was established.
				// Re-enter read-lock to obtain it.
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return consumer.Ack(msg)
	}
}

// Receive returns a single Message, if available.
// A reasonable context should be provided that will be used
// to wait for an incoming message if none are available.
func (m *ManagedConsumer) Receive(ctx context.Context) (msg.Message, error) {
	for {
		m.mu.RLock()
		consumer := m.consumer
		wait := m.waitc
		m.mu.RUnlock()

		if consumer == nil {
			select {
			case <-wait:
				// a new consumer was established.
				// Re-enter read-lock to obtain it.
				continue
			case <-ctx.Done():
				return msg.Message{}, ctx.Err()
			}
		}

		// TODO: determine when, if ever, to call
		// consumer.RedeliverOverflow

		if len(m.queue) < 1 {
			if err := consumer.Flow(1); err != nil {
				return msg.Message{}, err
			}
		}

		select {
		case msg, ok := <-m.queue:
			if ok {
				if consumer.GetUnAckTracker() != nil {
					consumer.GetUnAckTracker().Add(msg.Msg.MessageId)
				}
				return msg, nil
			}

		case <-ctx.Done():
			return msg.Message{}, ctx.Err()

		case <-consumer.Closed():
			return msg.Message{}, errors.New("consumer closed")

		case <-consumer.ConnClosed():
			return msg.Message{}, errors.New("consumer connection closed")
		}

	}
}

// ReceiveAsync blocks until the context is done. It continuously reads messages from the
// consumer and Sends them to the provided channel. It manages flow control internally based
// on the queue size.
func (m *ManagedConsumer) ReceiveAsync(ctx context.Context, msgs chan<- msg.Message) error {
	// Send flow request after 1/2 of the queue
	// has been consumed
	highwater := uint32(cap(m.queue)) / 2

	drain := func() {
		for {
			select {
			case msg := <-m.queue:
				msgs <- msg
			default:
				return
			}
		}
	}

CONSUMER:
	for {
		// ensure that the message queue is empty
		drain()

		// gain lock on consumer
		m.mu.RLock()
		consumer := m.consumer
		wait := m.waitc
		m.mu.RUnlock()

		if consumer == nil {
			select {
			case <-wait:
				// a new consumer was established.
				// Re-enter read-lock to obtain it.
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// TODO: determine when, if ever, to call
		// consumer.RedeliverOverflow

		// request half the buffer's capacity
		if err := consumer.Flow(highwater); err != nil {
			m.asyncErrs.Send(err)
			continue CONSUMER
		}

		var receivedSinceFlow uint32

		for {
			select {
			case msg := <-m.queue:
				msgs <- msg

				if consumer.GetUnAckTracker() != nil {
					consumer.GetUnAckTracker().Add(msg.Msg.MessageId)
				}

				if receivedSinceFlow++; receivedSinceFlow >= highwater {
					if err := consumer.Flow(receivedSinceFlow); err != nil {
						m.asyncErrs.Send(err)
						continue CONSUMER
					}
					receivedSinceFlow = 0
				}
				continue

			case <-ctx.Done():
				return ctx.Err()

			case <-consumer.Closed():
				m.asyncErrs.Send(errors.New("consumer closed"))
				continue CONSUMER

			case <-consumer.ConnClosed():
				m.asyncErrs.Send(errors.New("consumer connection closed"))
				continue CONSUMER
			}
		}
	}
}

// set unblocks the "wait" channel (if not nil),
// and sets the consumer under lock.
func (m *ManagedConsumer) set(c sub.ConsumerInterface) {
	m.mu.Lock()

	m.consumer = c

	if m.waitc != nil {
		close(m.waitc)
		m.waitc = nil
	}

	m.mu.Unlock()
}

// unset creates the "wait" channel (if nil),
// and sets the consumer to nil under lock.
func (m *ManagedConsumer) unset() {
	m.mu.Lock()

	if m.waitc == nil {
		// allow unset() to be called
		// multiple times by only creating
		// wait chan if its nil
		m.waitc = make(chan struct{})
	}
	m.consumer = nil

	m.mu.Unlock()
}

// newConsumer attempts to create a Consumer.
func (m *ManagedConsumer) newConsumer(ctx context.Context) (sub.ConsumerInterface, error) {
	mc, err := m.clientPool.ForTopic(ctx, m.cfg.ClientConfig, m.cfg.Topic)
	if err != nil {
		return nil, err
	}

	client, err := mc.Get(ctx)
	if err != nil {
		return nil, err
	}

	log.Info("create partition consumer...")
	//res, err := client.Discoverer.PartitionedMetadata(ctx, m.cfg.Topic)
	//if err != nil {
	//	log.Errorf("get partitioned metadata error:%s", err.Error())
	//}
	//
	//partitionNums := res.GetPartitions()

	var partitionNums uint32
	// Create the topic consumer. A non-blank consumer name is required.

	switch m.cfg.SubMode {
	case SubscriptionModeExclusive:
		return client.NewExclusiveConsumer(ctx, m.cfg.Topic, m.cfg.Name, m.cfg.Earliest, m.queue)
	case SubscriptionModeFailover:
		return client.NewFailoverConsumer(ctx, m.cfg.Topic, m.cfg.Name, m.queue)
	case SubscriptionModeShard:
		if m.cfg.AckTimeoutMillis != 0 {
			return client.NewConsumerWithCfg(ctx, m.cfg, m.queue, partitionNums)
		}
		return client.NewSharedConsumer(ctx, m.cfg.Topic, m.cfg.Name, m.queue)
	case SubscriptionModeKeyShared:
		return client.NewConsumerWithCfg(ctx, m.cfg, m.queue, partitionNums)
	default:
		return nil, ErrorInvalidSubMode
	}
}

// reconnect blocks while a new Consumer is created.
func (m *ManagedConsumer) reconnect(initial bool) sub.ConsumerInterface {
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

		ctx, cancel := context.WithTimeout(context.Background(), m.cfg.NewConsumerTimeout)
		newConsumer, err := m.newConsumer(ctx)
		cancel()
		if err != nil {
			m.asyncErrs.Send(err)
			continue
		}

		return newConsumer
	}
}

// manage Monitors the Consumer for conditions
// that require it to be recreated.
func (m *ManagedConsumer) manage() {
	defer m.unset()

	consumer := m.reconnect(true)
	m.set(consumer)

	for {
		select {
		case <-consumer.ReachedEndOfTopic():
			// TODO: What to do here? For now, reconnect
			// reconnect

		case <-consumer.Closed():
			// reconnect

		case <-consumer.ConnClosed():
			// reconnect

		}

		m.unset()
		consumer = m.reconnect(false)
		m.set(consumer)
	}
}

// RedeliverUnacknowledged sends of REDELIVER_UNACKNOWLEDGED_MESSAGES request
// for all messages that have not been acked.
func (m *ManagedConsumer) RedeliverUnacknowledged(ctx context.Context) error {
	for {
		m.mu.RLock()
		consumer := m.consumer
		wait := m.waitc
		m.mu.RUnlock()

		if consumer == nil {
			select {
			case <-wait:
				// a new consumer was established.
				// Re-enter read-lock to obtain it.
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return consumer.RedeliverUnacknowledged(ctx)
	}
}

// RedeliverOverflow sends of REDELIVER_UNACKNOWLEDGED_MESSAGES request
// for all messages that were dropped because of full message buffer. Note that
// for all subscription types other than `shared`, _all_ unacknowledged messages
// will be redelivered.
// https://github.com/apache/incubator-pulsar/issues/2003
func (m *ManagedConsumer) RedeliverOverflow(ctx context.Context) (int, error) {
	for {
		m.mu.RLock()
		consumer := m.consumer
		wait := m.waitc
		m.mu.RUnlock()

		if consumer == nil {
			select {
			case <-wait:
				// a new consumer was established.
				// Re-enter read-lock to obtain it.
				continue
			case <-ctx.Done():
				return -1, ctx.Err()
			}
		}
		return consumer.RedeliverOverflow(ctx)
	}
}

// Unsubscribe the consumer from its topic.
func (m *ManagedConsumer) Unsubscribe(ctx context.Context) error {
	for {
		m.mu.RLock()
		consumer := m.consumer
		wait := m.waitc
		m.mu.RUnlock()

		if consumer == nil {
			select {
			case <-wait:
				// a new consumer was established.
				// Re-enter read-lock to obtain it.
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return consumer.Unsubscribe(ctx)
	}
}

// Monitor a scoped deferrable lock
func (m *ManagedConsumer) Monitor() func() {
	m.mu.Lock()
	return m.mu.Unlock
}

// Close consumer
func (m *ManagedConsumer) Close(ctx context.Context) error {
	defer m.Monitor()()
	return m.consumer.Close(ctx)
}
