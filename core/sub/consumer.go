// Copyright 2018 Comcast Cable ComMunications Management, LLC
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

package sub

import (
	"context"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/wolfstudy/pulsar-client-go/core/frame"
	"github.com/wolfstudy/pulsar-client-go/core/msg"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
)

// maxRedeliverUnacknowledged is the maxiMum number of
// message IDs to include in a REDELIVER_UNACKNOWLEDGED_MESSAGES
// message.
const maxRedeliverUnacknowledged = 1000

// newConsumer returns a ready-to-use consumer.
// A consumer is used to attach to a subscription and
// consumes messages from it. The provided channel is sent
// all messages the consumer receives.
func NewConsumer(s frame.CmdSender, dispatcher *frame.Dispatcher, topic string,
	reqID *msg.MonotonicID, ConsumerID uint64, queue chan msg.Message) *Consumer {
	return &Consumer{
		S:           s,
		Topic:       topic,
		ConsumerID:  ConsumerID,
		reqID:       reqID,
		dispatcher:  dispatcher,
		queue:       queue,
		closedc:     make(chan struct{}),
		endOfTopicc: make(chan struct{}),
	}
}

// Consumer handles all consumer related state.
type Consumer struct {
	S frame.CmdSender

	Topic      string
	ConsumerID uint64

	reqID      *msg.MonotonicID
	dispatcher *frame.Dispatcher // handles request/response state

	queue chan msg.Message

	omu      sync.Mutex           // protects following
	overflow []*api.MessageIdData // IDs of messages that were dropped because of full buffer

	mu           sync.Mutex // protects following
	isClosed     bool
	closedc      chan struct{}
	isEndOfTopic bool
	endOfTopicc  chan struct{}
}

// Messages returns a read-only channel of messages
// received by the consumer. The channel will never be
// closed by the consumer.
func (c *Consumer) Messages() <-chan msg.Message {
	return c.queue
}

// Ack is used to signal to the broker that a given message has been
// successfully processed by the application and can be discarded by the broker.
func (c *Consumer) Ack(msg msg.Message) error {
	cmd := api.BaseCommand{
		Type: api.BaseCommand_ACK.Enum(),
		Ack: &api.CommandAck{
			ConsumerId: proto.Uint64(c.ConsumerID),
			MessageId:  []*api.MessageIdData{msg.Msg.GetMessageId()},
			AckType:    api.CommandAck_Individual.Enum(),
		},
	}

	return c.S.SendSimpleCmd(cmd)
}

// Flow command gives additional permits to send messages to the consumer.
// A typical consumer implementation will use a queue to accuMulate these messages
// before the application is ready to consume them. After the consumer is ready,
// the client needs to give permission to the broker to push messages.
func (c *Consumer) Flow(permits uint32) error {
	if permits <= 0 {
		return fmt.Errorf("invalid number of permits requested: %d", permits)
	}

	cmd := api.BaseCommand{
		Type: api.BaseCommand_FLOW.Enum(),
		Flow: &api.CommandFlow{
			ConsumerId:     proto.Uint64(c.ConsumerID),
			MessagePermits: proto.Uint32(permits),
		},
	}

	return c.S.SendSimpleCmd(cmd)
}

// Closed returns a channel that will block _unless_ the
// consumer has been closed, in which case the channel will have
// been closed and unblocked.
func (c *Consumer) Closed() <-chan struct{} {
	return c.closedc
}

// ConnClosed unblocks when the consumer's connection has been closed. Once that
// happens, it's necessary to first recreate the client and then the consumer.
func (c *Consumer) ConnClosed() <-chan struct{} {
	return c.S.Closed()
}

// Close closes the consumer. The channel returned from the Closed method
// will then unblock upon successful closure.
func (c *Consumer) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed {
		return nil
	}

	requestID := c.reqID.Next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_CLOSE_CONSUMER.Enum(),
		CloseConsumer: &api.CommandCloseConsumer{
			RequestId:  requestID,
			ConsumerId: proto.Uint64(c.ConsumerID),
		},
	}

	resp, cancel, err := c.dispatcher.RegisterReqID(*requestID)
	if err != nil {
		return err
	}
	defer cancel()

	if err := c.S.SendSimpleCmd(cmd); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-resp:
		c.isClosed = true
		close(c.closedc)

		return nil
	}
}

// Unsubscribe the consumer from its topic.
func (c *Consumer) Unsubscribe(ctx context.Context) error {
	requestID := c.reqID.Next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_UNSUBSCRIBE.Enum(),
		Unsubscribe: &api.CommandUnsubscribe{
			RequestId:  requestID,
			ConsumerId: proto.Uint64(c.ConsumerID),
		},
	}

	resp, cancel, err := c.dispatcher.RegisterReqID(*requestID)
	if err != nil {
		return err
	}
	defer cancel()

	if err := c.S.SendSimpleCmd(cmd); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-resp:
		// Response type is SUCCESS
		return nil
	}
}

// HandleCloseConsumer should be called when a CLOSE_CONSUMER message is received
// associated with this consumer.
func (c *Consumer) HandleCloseConsumer(f frame.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed {
		return nil
	}

	c.isClosed = true
	close(c.closedc)

	return nil
}

// ReachedEndOfTopic unblocks whenever the topic has been "terminated" and
// all the messages on the subscription were acknowledged.
func (c *Consumer) ReachedEndOfTopic() <-chan struct{} {
	return c.endOfTopicc
}

// HandleReachedEndOfTopic should be called for all received REACHED_END_OF_TOPIC messages
// associated with this consumer.
func (c *Consumer) HandleReachedEndOfTopic(f frame.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isEndOfTopic {
		return nil
	}

	c.isEndOfTopic = true
	close(c.endOfTopicc)

	return nil
}

// RedeliverUnacknowledged uses the protocol option
// REDELIVER_UNACKNOWLEDGED_MESSAGES to re-retrieve unacked messages.
func (c *Consumer) RedeliverUnacknowledged(ctx context.Context) error {
	cmd := api.BaseCommand{
		Type: api.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES.Enum(),
		RedeliverUnacknowledgedMessages: &api.CommandRedeliverUnacknowledgedMessages{
			ConsumerId: proto.Uint64(c.ConsumerID),
		},
	}

	if err := c.S.SendSimpleCmd(cmd); err != nil {
		return err
	}

	// clear overflow slice
	c.omu.Lock()
	c.overflow = nil
	c.omu.Unlock()

	return nil
}

// RedeliverOverflow sends of REDELIVER_UNACKNOWLEDGED_MESSAGES request
// for all messages that were dropped because of full message buffer. Note that
// for all subscription types other than `shared`, _all_ unacknowledged messages
// will be redelivered.
// https://github.com/apache/incubator-pulsar/issues/2003
func (c *Consumer) RedeliverOverflow(ctx context.Context) (int, error) {
	c.omu.Lock()
	defer c.omu.Unlock()

	l := len(c.overflow)

	if l == 0 {
		return l, nil
	}

	// Send REDELIVER_UNACKNOWLEDGED_MESSAGES commands, with at most
	// maxRedeliverUnacknowledged message ids at a time.
	for i := 0; i < l; i += maxRedeliverUnacknowledged {
		end := i + maxRedeliverUnacknowledged
		if end > l {
			end = l
		}

		cmd := api.BaseCommand{
			Type: api.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES.Enum(),
			RedeliverUnacknowledgedMessages: &api.CommandRedeliverUnacknowledgedMessages{
				ConsumerId: proto.Uint64(c.ConsumerID),
				MessageIds: c.overflow[i:end],
			},
		}

		if err := c.S.SendSimpleCmd(cmd); err != nil {
			return 0, err
		}
	}

	// clear overflow slice
	c.overflow = nil

	return l, nil
}

// HandleMessage should be called for all MESSAGE messages received for
// this consumer.
func (c *Consumer) HandleMessage(f frame.Frame) error {
	m := msg.Message{
		Topic:      c.Topic,
		ConsumerID: c.ConsumerID,
		Msg:        f.BaseCmd.GetMessage(),
		Meta:       f.Metadata,
		Payload:    f.Payload,
	}

	select {
	case c.queue <- m:
		return nil

	default:
		// Add messageId to overflow buffer, avoiding duplicates.
		newMid := f.BaseCmd.GetMessage().GetMessageId()

		var dup bool
		c.omu.Lock()
		for _, mid := range c.overflow {
			if proto.Equal(mid, newMid) {
				dup = true
				break
			}
		}
		if !dup {
			c.overflow = append(c.overflow, newMid)
		}
		c.omu.Unlock()

		return fmt.Errorf("consumer message queue on topic %q is full (capacity = %d)", c.Topic, cap(c.queue))
	}
}
