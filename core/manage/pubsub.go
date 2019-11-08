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
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/wolfstudy/pulsar-client-go/core/frame"
	"github.com/wolfstudy/pulsar-client-go/core/msg"
	"github.com/wolfstudy/pulsar-client-go/core/pub"
	"github.com/wolfstudy/pulsar-client-go/core/sub"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
	"github.com/wolfstudy/pulsar-client-go/utils"
)

// NewPubsub returns a ready-to-use pubsub.
func NewPubsub(s frame.CmdSender, dispatcher *frame.Dispatcher, subscriptions *Subscriptions, reqID *msg.MonotonicID) *Pubsub {
	return &Pubsub{
		s:             s,
		reqID:         reqID,
		producerID:    &msg.MonotonicID{ID: 0},
		consumerID:    &msg.MonotonicID{ID: 0},
		dispatcher:    dispatcher,
		subscriptions: subscriptions,
	}
}

// Pubsub is responsible for creating producers and consumers on a give topic.
type Pubsub struct {
	s          frame.CmdSender
	reqID      *msg.MonotonicID
	producerID *msg.MonotonicID
	consumerID *msg.MonotonicID

	dispatcher    *frame.Dispatcher // handles request response state
	subscriptions *Subscriptions
}

// Subscribe subscribes to the given topic. The queueSize determines the buffer
// size of the Consumer.Messages() channel.
func (t *Pubsub) Subscribe(ctx context.Context, topic, subscribe string, subType api.CommandSubscribe_SubType,
	initialPosition api.CommandSubscribe_InitialPosition, queue chan msg.Message) (*sub.Consumer, error) {
	requestID := t.reqID.Next()
	consumerID := t.consumerID.Next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_SUBSCRIBE.Enum(),
		Subscribe: &api.CommandSubscribe{
			SubType:         subType.Enum(),
			Topic:           proto.String(topic),
			Subscription:    proto.String(subscribe),
			RequestId:       requestID,
			ConsumerId:      consumerID,
			InitialPosition: initialPosition.Enum(),
		},
	}

	resp, cancel, errs := t.dispatcher.RegisterReqID(*requestID)
	if errs != nil {
		return nil, errs
	}
	defer cancel()

	c := sub.NewConsumer(t.s, t.dispatcher, topic, t.reqID, *consumerID, queue)
	// the new subscription needs to be added to the map
	// before sending the subscribe command, otherwise there'd
	// be a race between receiving the success result and
	// a possible message to the subscription
	t.subscriptions.AddConsumer(c)

	if errs := t.s.SendSimpleCmd(cmd); errs != nil {
		t.subscriptions.DelConsumer(c)
		return nil, errs
	}

	// wait for a response or timeout
	select {
	case <-ctx.Done():
		t.subscriptions.DelConsumer(c)
		return nil, ctx.Err()

	case f := <-resp:
		msgType := f.BaseCmd.GetType()
		// Possible responses types are:
		//  - Success (why not SubscribeSuccess?)
		//  - Error
		switch msgType {
		case api.BaseCommand_SUCCESS:
			return c, nil

		case api.BaseCommand_ERROR:
			t.subscriptions.DelConsumer(c)

			errMsg := f.BaseCmd.GetError()
			return nil, fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())

		default:
			t.subscriptions.DelConsumer(c)

			return nil, utils.NewUnexpectedErrMsg(msgType, *requestID)
		}
	}
}

// TODO: replace Subscribe() method above

func (t *Pubsub) SubscribeWithCfg(ctx context.Context, cfg ManagedConsumerConfig, queue chan msg.Message) (*sub.Consumer, error) {
	requestID := t.reqID.Next()
	consumerID := t.consumerID.Next()

	subType, subPos := t.GetCfgMode(cfg)

	cmd := api.BaseCommand{
		Type: api.BaseCommand_SUBSCRIBE.Enum(),
		Subscribe: &api.CommandSubscribe{
			SubType:         &subType,
			Topic:           proto.String(cfg.Topic),
			Subscription:    proto.String(cfg.Name),
			RequestId:       requestID,
			ConsumerId:      consumerID,
			InitialPosition: &subPos,
		},
	}

	resp, cancel, errs := t.dispatcher.RegisterReqID(*requestID)
	if errs != nil {
		return nil, errs
	}
	defer cancel()

	c := sub.NewConsumer(t.s, t.dispatcher, cfg.Topic, t.reqID, *consumerID, queue)

	// the new subscription needs to be added to the map
	// before sending the subscribe command, otherwise there'd
	// be a race between receiving the success result and
	// a possible message to the subscription
	t.subscriptions.AddConsumer(c)

	if errs := t.s.SendSimpleCmd(cmd); errs != nil {
		t.subscriptions.DelConsumer(c)
		return nil, errs
	}

	// wait for a response or timeout

	select {
	case <-ctx.Done():
		t.subscriptions.DelConsumer(c)
		return nil, ctx.Err()

	case f := <-resp:
		msgType := f.BaseCmd.GetType()
		// Possible responses types are:
		//  - Success (why not SubscribeSuccess?)
		//  - Error
		switch msgType {
		case api.BaseCommand_SUCCESS:
			return c, nil

		case api.BaseCommand_ERROR:
			t.subscriptions.DelConsumer(c)

			errMsg := f.BaseCmd.GetError()
			return nil, fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())

		default:
			t.subscriptions.DelConsumer(c)

			return nil, utils.NewUnexpectedErrMsg(msgType, *requestID)
		}
	}
}

func (t *Pubsub) GetCfgMode(cfg ManagedConsumerConfig) (api.CommandSubscribe_SubType, api.CommandSubscribe_InitialPosition) {
	var (
		subType api.CommandSubscribe_SubType
		subPos  api.CommandSubscribe_InitialPosition
	)

	switch cfg.SubMode {
	case SubscriptionModeKeyShared:
		subType = api.CommandSubscribe_Key_Shared
	case SubscriptionModeShard:
		subType = api.CommandSubscribe_Shared
	case SubscriptionModeExclusive:
		subType = api.CommandSubscribe_Exclusive
	case SubscriptionModeFailover:
		subType = api.CommandSubscribe_Failover
	default:
		subType = api.CommandSubscribe_Exclusive
	}

	if cfg.Earliest {
		subPos = api.CommandSubscribe_Earliest
	} else {
		subPos = api.CommandSubscribe_Latest
	}

	return subType, subPos
}

// Producer creates a new producer for the given topic and producerName.
func (t *Pubsub) Producer(ctx context.Context, topic, producerName string) (*pub.Producer, error) {
	requestID := t.reqID.Next()
	producerID := t.producerID.Next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_PRODUCER.Enum(),
		Producer: &api.CommandProducer{
			RequestId:  requestID,
			ProducerId: producerID,
			Topic:      proto.String(topic),
		},
	}
	if producerName != "" {
		cmd.Producer.ProducerName = proto.String(producerName)
	}

	resp, cancel, err := t.dispatcher.RegisterReqID(*requestID)
	if err != nil {
		return nil, err
	}
	defer cancel()

	p := pub.NewProducer(t.s, t.dispatcher, t.reqID, *producerID)
	// the new producer needs to be added to subscriptions before sending
	// the create command to avoid potential race conditions
	t.subscriptions.AddProducer(p)

	if err := t.s.SendSimpleCmd(cmd); err != nil {
		t.subscriptions.DelProducer(p)
		return nil, err
	}

	// wait for the success response, error, or timeout

	select {
	case <-ctx.Done():
		t.subscriptions.DelProducer(p)
		return nil, ctx.Err()

	case f := <-resp:
		msgType := f.BaseCmd.GetType()
		// Possible responses types are:
		//  - ProducerSuccess
		//  - Error
		switch msgType {
		case api.BaseCommand_PRODUCER_SUCCESS:
			success := f.BaseCmd.GetProducerSuccess()
			// TODO: is this a race?
			p.ProducerName = success.GetProducerName()
			return p, nil

		case api.BaseCommand_ERROR:
			t.subscriptions.DelProducer(p)

			errMsg := f.BaseCmd.GetError()
			return nil, fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())

		default:
			t.subscriptions.DelProducer(p)

			return nil, utils.NewUnexpectedErrMsg(msgType, *requestID)
		}
	}
}
