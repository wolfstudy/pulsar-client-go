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

package sub

import (
	"context"
	"fmt"

	"github.com/Comcast/pulsar-client-go/core/frame"
	"github.com/Comcast/pulsar-client-go/core/msg"
	"github.com/Comcast/pulsar-client-go/core/pub"
	"github.com/Comcast/pulsar-client-go/pkg/api"
	"github.com/Comcast/pulsar-client-go/utils"
	"github.com/golang/protobuf/proto"
)

// NewPubsub returns a ready-to-use pubsub.
func NewPubsub(s frame.CmdSender, dispatcher *frame.Dispatcher, subscriptions *Subscriptions, reqID *msg.MonotonicID) *Pubsub {
	return &Pubsub{
		S:             s,
		ReqID:         reqID,
		ProducerID:    &msg.MonotonicID{ID: 0},
		ConsumerID:    &msg.MonotonicID{ID: 0},
		Dispatcher:    dispatcher,
		Subscriptions: subscriptions,
	}
}

// Pubsub is responsible for creating producers and consumers on a give topic.
type Pubsub struct {
	S          frame.CmdSender
	ReqID      *msg.MonotonicID
	ProducerID *msg.MonotonicID
	ConsumerID *msg.MonotonicID

	Dispatcher    *frame.Dispatcher // handles request response state
	Subscriptions *Subscriptions
}

// Subscribe subscribes to the given topic. The queueSize determines the buffer
// size of the Consumer.Messages() channel.
func (t *Pubsub) Subscribe(ctx context.Context, topic, sub string, subType api.CommandSubscribe_SubType,
	initialPosition api.CommandSubscribe_InitialPosition, queue chan msg.Message) (*Consumer, error) {
	requestID := t.ReqID.Next()
	consumerID := t.ConsumerID.Next()

	cmd := api.BaseCommand{
		Type: api.BaseCommand_SUBSCRIBE.Enum(),
		Subscribe: &api.CommandSubscribe{
			SubType:         subType.Enum(),
			Topic:           proto.String(topic),
			Subscription:    proto.String(sub),
			RequestId:       requestID,
			ConsumerId:      consumerID,
			InitialPosition: initialPosition.Enum(),
		},
	}

	resp, cancel, errs := t.Dispatcher.RegisterReqID(*requestID)
	if errs != nil {
		return nil, errs
	}
	defer cancel()

	c := newConsumer(t.S, t.Dispatcher, topic, t.ReqID, *consumerID, queue)
	// the new subscription needs to be added to the map
	// before sending the subscribe command, otherwise there'd
	// be a race between receiving the success result and
	// a possible message to the subscription
	t.Subscriptions.AddConsumer(c)

	if errs := t.S.SendSimpleCmd(cmd); errs != nil {
		t.Subscriptions.DelConsumer(c)
		return nil, errs
	}

	// wait for a response or timeout

	select {
	case <-ctx.Done():
		t.Subscriptions.DelConsumer(c)
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
			t.Subscriptions.DelConsumer(c)

			errMsg := f.BaseCmd.GetError()
			return nil, fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())

		default:
			t.Subscriptions.DelConsumer(c)

			return nil, utils.NewUnexpectedErrMsg(msgType, *requestID)
		}
	}
}

// Producer creates a new producer for the given topic and producerName.
func (t *Pubsub) Producer(ctx context.Context, topic, producerName string) (*pub.Producer, error) {
	requestID := t.ReqID.Next()
	producerID := t.ProducerID.Next()

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

	resp, cancel, err := t.Dispatcher.RegisterReqID(*requestID)
	if err != nil {
		return nil, err
	}
	defer cancel()

	p := pub.NewProducer(t.S, t.Dispatcher, t.ReqID, *producerID)
	// the new producer needs to be added to subscriptions before sending
	// the create command to avoid potential race conditions
	t.Subscriptions.AddProducer(p)

	if err := t.S.SendSimpleCmd(cmd); err != nil {
		t.Subscriptions.DelProducer(p)
		return nil, err
	}

	// wait for the success response, error, or timeout

	select {
	case <-ctx.Done():
		t.Subscriptions.DelProducer(p)
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
			t.Subscriptions.DelProducer(p)

			errMsg := f.BaseCmd.GetError()
			return nil, fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())

		default:
			t.Subscriptions.DelProducer(p)

			return nil, utils.NewUnexpectedErrMsg(msgType, *requestID)
		}
	}
}
