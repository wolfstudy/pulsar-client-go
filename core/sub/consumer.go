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
	"errors"
	"fmt"
	"sync"
	"time"

	set "github.com/deckarep/golang-set"
	"github.com/golang/protobuf/proto"
	"github.com/wolfstudy/pulsar-client-go/core/frame"
	"github.com/wolfstudy/pulsar-client-go/core/msg"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
	"github.com/wolfstudy/pulsar-client-go/pkg/log"
)

// maxRedeliverUnacknowledged is the maxiMum number of
// message IDs to include in a REDELIVER_UNACKNOWLEDGED_MESSAGES
// message.
const maxRedeliverUnacknowledged = 1000

type ConsumerInterface interface {
	Close(ctx context.Context) error
	Ack(msg msg.Message) error
	Unsubscribe(ctx context.Context) error
	RedeliverUnacknowledged(ctx context.Context) error
	Flow(permits uint32) error
	Closed() <-chan struct{}
	ConnClosed() <-chan struct{}
	GetUnAckTracker() *UnackedMessageTracker
	ReachedEndOfTopic() <-chan struct{}
	RedeliverOverflow(ctx context.Context) (int, error)
}

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
		ReqID:       reqID,
		Dispatcher:  dispatcher,
		Queue:       queue,
		Closedc:     make(chan struct{}),
		EndOfTopicc: make(chan struct{}),
	}
}

// Consumer handles all consumer related state.
type Consumer struct {
	S frame.CmdSender

	Topic      string
	ConsumerID uint64

	ReqID      *msg.MonotonicID
	Dispatcher *frame.Dispatcher // handles request/response state

	Queue chan msg.Message

	Omu      sync.Mutex           // protects following
	Overflow []*api.MessageIdData // IDs of messages that were dropped because of full buffer

	Mu           sync.Mutex // protects following
	IsClosed     bool
	Closedc      chan struct{}
	IsEndOfTopic bool
	EndOfTopicc  chan struct{}

	UnAckTracker *UnackedMessageTracker
}

type PartitionConsumer struct {
	Consumers      []*Consumer
	UnAckTracker   *UnackedMessageTracker
	PartitionQueue chan msg.Message
}

func (pc *PartitionConsumer) Close(ctx context.Context) error {
	var errMsg string
	for _, consumer := range pc.Consumers {
		if err := consumer.Close(ctx); err != nil {
			errMsg += fmt.Sprintf("topic %s, consumer is %d: %s ", consumer.Topic, consumer.ConsumerID, err)
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

func (pc *PartitionConsumer) Ack(msg msg.Message) error {
	err := pc.Consumers[msg.Msg.GetMessageId().GetPartition()].Ack(msg)
	if err != nil {
		log.Errorf("Ack message error:%s", err.Error())
		return err
	}

	if pc.UnAckTracker != nil {
		pc.UnAckTracker.Remove(msg.Msg.MessageId)
	}
	return nil
}

func (pc *PartitionConsumer) Unsubscribe(ctx context.Context) error {
	for _, consumer := range pc.Consumers {
		err := consumer.Unsubscribe(ctx)
		if err != nil {
			log.Errorf("sub consumer unsubscribe error:%s", err.Error())
			return err
		}
	}

	return nil
}

func (pc *PartitionConsumer) RedeliverUnacknowledged(ctx context.Context) error {
	for _, consumer := range pc.Consumers {
		err := consumer.RedeliverUnacknowledged(ctx)
		if err != nil {
			log.Errorf("sub consumer unsubscribe error:%s", err.Error())
			return err
		}
		pc.PartitionQueue = make(chan msg.Message)
		if pc.UnAckTracker != nil {
			pc.UnAckTracker.clear()
		}
	}
	return nil
}

func (pc *PartitionConsumer) Flow(permits uint32) error {
	for i := 0; uint32(i) < permits; i++ {
		//TODO: random pc.Consumers
		for _, consumer := range pc.Consumers {
			if err := consumer.Flow(1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (pc *PartitionConsumer) Closed() <-chan struct{} {
	for _, consumer := range pc.Consumers {
		return consumer.Closedc
	}
	return nil
}

func (pc *PartitionConsumer) ConnClosed() <-chan struct{} {
	for _, consumer := range pc.Consumers {
		consumer.S.Closed()
	}
	return nil
}

func (pc *PartitionConsumer) ReachedEndOfTopic() <-chan struct{} {
	for _, consumer := range pc.Consumers {
		return consumer.EndOfTopicc
	}
	return nil
}

func (pc *PartitionConsumer) RedeliverOverflow(ctx context.Context) (int, error) {
	return 0, nil
}

func (pc *PartitionConsumer) GetUnAckTracker() *UnackedMessageTracker {
	return pc.UnAckTracker
}

func (c *Consumer) GetUnAckTracker() *UnackedMessageTracker {
	return c.UnAckTracker
}

// Messages returns a read-only channel of messages
// received by the consumer. The channel will never be
// closed by the consumer.
func (c *Consumer) Messages() <-chan msg.Message {
	return c.Queue
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

	if c.UnAckTracker != nil {
		c.UnAckTracker.Remove(msg.Msg.MessageId)
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
	return c.Closedc
}

// ConnClosed unblocks when the consumer's connection has been closed. Once that
// happens, it's necessary to first recreate the client and then the consumer.
func (c *Consumer) ConnClosed() <-chan struct{} {
	return c.S.Closed()
}

// Close closes the consumer. The channel returned from the Closed method
// will then unblock upon successful closure.
func (c *Consumer) Close(ctx context.Context) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.IsClosed {
		return nil
	}

	requestID := c.ReqID.Next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_CLOSE_CONSUMER.Enum(),
		CloseConsumer: &api.CommandCloseConsumer{
			RequestId:  requestID,
			ConsumerId: proto.Uint64(c.ConsumerID),
		},
	}

	resp, cancel, err := c.Dispatcher.RegisterReqID(*requestID)
	if err != nil {
		return err
	}
	defer cancel()

	if c.UnAckTracker != nil {
		c.UnAckTracker.Stop()
	}

	if err := c.S.SendSimpleCmd(cmd); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-resp:
		c.IsClosed = true
		close(c.Closedc)

		return nil
	}
}

// Unsubscribe the consumer from its topic.
func (c *Consumer) Unsubscribe(ctx context.Context) error {
	requestID := c.ReqID.Next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_UNSUBSCRIBE.Enum(),
		Unsubscribe: &api.CommandUnsubscribe{
			RequestId:  requestID,
			ConsumerId: proto.Uint64(c.ConsumerID),
		},
	}

	resp, cancel, err := c.Dispatcher.RegisterReqID(*requestID)
	if err != nil {
		return err
	}
	defer cancel()

	if c.UnAckTracker != nil {
		c.UnAckTracker.Stop()
	}

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
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.IsClosed {
		return nil
	}

	c.IsClosed = true
	close(c.Closedc)

	return nil
}

// ReachedEndOfTopic unblocks whenever the topic has been "terminated" and
// all the messages on the subscription were acknowledged.
func (c *Consumer) ReachedEndOfTopic() <-chan struct{} {
	return c.EndOfTopicc
}

// HandleReachedEndOfTopic should be called for all received REACHED_END_OF_TOPIC messages
// associated with this consumer.
func (c *Consumer) HandleReachedEndOfTopic(f frame.Frame) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.IsEndOfTopic {
		return nil
	}

	c.IsEndOfTopic = true
	close(c.EndOfTopicc)

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

	if c.UnAckTracker != nil {
		c.UnAckTracker.clear()
	}
	// clear Overflow slice
	c.Omu.Lock()
	c.Overflow = nil
	c.Omu.Unlock()

	return nil
}

// RedeliverOverflow sends of REDELIVER_UNACKNOWLEDGED_MESSAGES request
// for all messages that were dropped because of full message buffer. Note that
// for all subscription types other than `shared`, _all_ unacknowledged messages
// will be redelivered.
// https://github.com/apache/incubator-pulsar/issues/2003
func (c *Consumer) RedeliverOverflow(ctx context.Context) (int, error) {
	c.Omu.Lock()
	defer c.Omu.Unlock()

	l := len(c.Overflow)

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
				MessageIds: c.Overflow[i:end],
			},
		}

		if err := c.S.SendSimpleCmd(cmd); err != nil {
			return 0, err
		}
	}

	// clear Overflow slice
	c.Overflow = nil

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
	case c.Queue <- m:
		//fmt.Printf("sub consumer receive messages, queue size: %d, message ID:%v\n", cap(c.Queue), m.Msg.GetMessageId())
		//fmt.Println(string(m.Payload))

		//tmpch := <-c.Queue
		//fmt.Println(tmpch)

		return nil

	default:
		// Add messageId to Overflow buffer, avoiding duplicates.
		newMid := f.BaseCmd.GetMessage().GetMessageId()

		var dup bool
		c.Omu.Lock()
		for _, mid := range c.Overflow {
			if proto.Equal(mid, newMid) {
				dup = true
				break
			}
		}
		if !dup {
			c.Overflow = append(c.Overflow, newMid)
		}
		c.Omu.Unlock()

		return fmt.Errorf("consumer message queue on topic %q is full (capacity = %d)", c.Topic, cap(c.Queue))
	}
}

type UnackedMessageTracker struct {
	cmu        sync.RWMutex // protects following
	currentSet set.Set
	oldOpenSet set.Set
	timeout    time.Ticker
	consumer   Consumer
}

func NewUnackedMessageTracker(consumer *Consumer) *UnackedMessageTracker {
	UnAckTracker := &UnackedMessageTracker{
		currentSet: set.NewSet(),
		oldOpenSet: set.NewSet(),
		consumer:   *consumer,
	}

	return UnAckTracker
}

func (t *UnackedMessageTracker) Size() int {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	return t.currentSet.Cardinality() + t.oldOpenSet.Cardinality()
}

func (t *UnackedMessageTracker) IsEmpty() bool {
	t.cmu.RLock()
	defer t.cmu.RUnlock()

	return t.currentSet.Cardinality() == 0 && t.oldOpenSet.Cardinality() == 0
}

func (t *UnackedMessageTracker) Add(id *api.MessageIdData) bool {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	t.oldOpenSet.Remove(id)
	return t.currentSet.Add(id)
}

func (t *UnackedMessageTracker) Remove(id *api.MessageIdData) {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	t.currentSet.Remove(id)
	t.oldOpenSet.Remove(id)
}

func (t *UnackedMessageTracker) clear() {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	t.currentSet.Clear()
	t.oldOpenSet.Clear()
}

func (t *UnackedMessageTracker) toggle() {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	t.currentSet, t.oldOpenSet = t.oldOpenSet, t.currentSet
}

func (t *UnackedMessageTracker) isAckTimeout() bool {
	t.cmu.RLock()
	defer t.cmu.RUnlock()

	return !(t.oldOpenSet.Cardinality() == 0)
}

func (t *UnackedMessageTracker) lessThanOrEqual(id1, id2 api.MessageIdData) bool {
	return id1.GetPartition() == id2.GetPartition() &&
		(id1.GetLedgerId() < id2.GetLedgerId() || id1.GetEntryId() <= id2.GetEntryId())
}

func (t *UnackedMessageTracker) RemoveMessagesTill(id api.MessageIdData) int {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	counter := 0

	t.currentSet.Each(func(elem interface{}) bool {
		if t.lessThanOrEqual(elem.(api.MessageIdData), id) {
			t.currentSet.Remove(elem)
			counter ++
		}
		return true
	})

	t.oldOpenSet.Each(func(elem interface{}) bool {
		if t.lessThanOrEqual(elem.(api.MessageIdData), id) {
			t.currentSet.Remove(elem)
			counter ++
		}
		return true
	})

	return counter
}

func (t *UnackedMessageTracker) Start(ackTimeoutMillis int64) {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	t.timeout = *time.NewTicker((time.Duration(ackTimeoutMillis)) * time.Millisecond)

	go func() {
		for tick := range t.timeout.C {
			if t.isAckTimeout() {
				log.Warn("%d messages have timed - out", t.oldOpenSet.Cardinality())
				messageIds := make([]*api.MessageIdData, t.oldOpenSet.Cardinality())

				t.oldOpenSet.Each(func(i interface{}) bool {
					messageIds = append(messageIds, i.(*api.MessageIdData))
					return true
				})

				t.oldOpenSet.Clear()

				cmd := api.BaseCommand{
					Type: api.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES.Enum(),
					RedeliverUnacknowledgedMessages: &api.CommandRedeliverUnacknowledgedMessages{
						ConsumerId: proto.Uint64(t.consumer.ConsumerID),
						MessageIds: messageIds,
					},
				}

				if err := t.consumer.S.SendSimpleCmd(cmd); err != nil {
					return
				}
			}

			t.toggle()
			log.Debug("Tick at ", tick)
		}
	}()
}

func (t *UnackedMessageTracker) Stop() {
	t.cmu.Lock()
	defer t.cmu.Unlock()

	t.timeout.Stop()
	log.Debug("stop ticker ", t.timeout)

	t.clear()
}
