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
	"github.com/wolfstudy/pulsar-client-go/pkg/log"
	"github.com/wolfstudy/pulsar-client-go/utils"
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
func (t *Pubsub) Subscribe(ctx context.Context, topic, subscribe string, subType api.CommandSubscribe_SubType,
	initialPosition api.CommandSubscribe_InitialPosition, queue chan msg.Message) (*sub.Consumer, error) {
	requestID := t.ReqID.Next()
	consumerID := t.ConsumerID.Next()

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

	resp, cancel, errs := t.Dispatcher.RegisterReqID(*requestID)
	if errs != nil {
		return nil, errs
	}
	defer cancel()

	c := sub.NewConsumer(t.S, t.Dispatcher, topic, t.ReqID, *consumerID, queue)
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

func (t *Pubsub) SubOnce(ctx context.Context, cfg ConsumerConfig, partitionName string, queue chan msg.Message) (*sub.Consumer, error) {
	requestID := t.ReqID.Next()
	consumerID := t.ConsumerID.Next()

	subType, subPos := t.GetCfgMode(cfg)

	cmd := api.BaseCommand{
		Type: api.BaseCommand_SUBSCRIBE.Enum(),
		Subscribe: &api.CommandSubscribe{
			SubType:         &subType,
			Topic:           proto.String(partitionName),
			Subscription:    proto.String(cfg.Name),
			RequestId:       requestID,
			ConsumerId:      consumerID,
			InitialPosition: &subPos,
		},
	}

	resp, cancel, errs := t.Dispatcher.RegisterReqID(*requestID)
	if errs != nil {
		return nil, errs
	}
	defer cancel()

	c := sub.NewConsumer(t.S, t.Dispatcher, cfg.Topic, t.ReqID, *consumerID, queue)

	if cfg.AckTimeoutMillis != 0 {
		c.UnAckTracker = sub.NewUnackedMessageTracker(c)
	}

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

func (t *Pubsub) GetCfgMode(cfg ConsumerConfig) (api.CommandSubscribe_SubType, api.CommandSubscribe_InitialPosition) {
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

// TODO: replace Subscribe() method above
func (t *Pubsub) SubscribeWithCfg(ctx context.Context, cfg ConsumerConfig, queue chan msg.Message, numPartitions uint32) (sub.ConsumerInterface, error) {
	var i, j uint32
	var err error

	type ConsumerError struct {
		err       error
		partition uint32
		cons      *sub.Consumer
	}

	ch := make(chan ConsumerError, numPartitions)
	if numPartitions > 1 {
		pc := &sub.PartitionConsumer{
			Consumers:      make([]*sub.Consumer, numPartitions),
			PartitionQueue: queue,
		}
		for i = 0; i < numPartitions; i++ {
			subQueue := make(chan msg.Message)
			partitionName := fmt.Sprintf("%s-partition-%d", cfg.Topic, i)
			consumer, err := t.SubOnce(ctx, cfg, partitionName, subQueue)
			if err != nil {
				log.Errorf("create sub consumer error:%s", err.Error())
				return nil, err
			}

			ch <- ConsumerError{
				partition: i,
				cons:      consumer,
				err:       err,
			}
		}

		for j = 0; j < numPartitions; j++ {
			pe := <-ch
			err = pe.err
			pc.Consumers[j] = pe.cons
		}

		if err != nil {
			// Since there were some failures, cleanup all the partitions that succeeded in creating the producers
			for _, consumer := range pc.Consumers {
				if consumer != nil {
					_ = consumer.Close(ctx)
				}
			}
			return nil, err
		} else {
			for _, subConsumer := range pc.Consumers {
				if err := subConsumer.Flow(uint32(cfg.QueueSize)); err != nil {
					return nil, err
				}
			}

			log.Infof("wait message receive...")
			// TODO: please fix me, fix receive logic, now once receive one message
			go func() {
				var counter int
				for _, subConsumer := range pc.Consumers {
					OUTER:
					for {
						select {
						case tmpMsg, ok := <-subConsumer.Queue:
							if ok {
								counter++
								queue <- tmpMsg
								if counter >= cap(queue)/2 {
									return
								}
							}
							continue OUTER
						}
					}
				}
			}()

			return pc, nil
		}
	}
	consumer, err := t.SubOnce(ctx, cfg, cfg.Topic, queue)
	if err != nil {
		log.Errorf("create sub consumer error:%s", err.Error())
		return nil, err
	}

	return consumer, nil
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

func (t *Pubsub) PartitionedProducer(ctx context.Context, topic, producerName string,
	partitionNums uint32, router pub.MessageRouter) (partitionProducer *pub.PartitionedProducer, err error) {
	var i, j uint32

	type ProducerError struct {
		partition uint32
		prod      *pub.Producer
		err       error
	}

	pp := &pub.PartitionedProducer{
		Topic:         topic,
		NumPartitions: partitionNums,
		Router:        router,
		Producers:     make([]*pub.Producer, partitionNums, partitionNums),
	}

	c := make(chan ProducerError, partitionNums)
	for i = 0; i < partitionNums; i++ {
		partitionName := fmt.Sprintf("%s-partition-%d", topic, i)
		log.Infof("create producer name is: %s", partitionName)
		producer, err := t.Producer(ctx, partitionName, producerName)
		c <- ProducerError{
			partition: i,
			prod:      producer,
			err:       err,
		}
	}

	for j = 0; j < partitionNums; j++ {
		pe := <-c
		err = pe.err
		pp.Producers[j] = pe.prod
	}

	if err != nil {
		// Since there were some failures, cleanup all the partitions that succeeded in creating the producers
		for _, producer := range pp.Producers {
			if producer != nil {
				_ = producer.Close(ctx)
			}
		}
		log.Errorf("create partition producer error: %s", err.Error())

		return nil, err
	} else {
		return pp, nil
	}
}
