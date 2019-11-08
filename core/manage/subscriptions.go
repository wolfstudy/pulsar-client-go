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
	"log"
	"sync"

	"github.com/wolfstudy/pulsar-client-go/core/frame"
	"github.com/wolfstudy/pulsar-client-go/core/pub"
	"github.com/wolfstudy/pulsar-client-go/core/sub"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
	"github.com/wolfstudy/pulsar-client-go/utils"
)

// NewSubscriptions returns a ready-to-use subscriptions.
func NewSubscriptions() *Subscriptions {
	return &Subscriptions{
		consumers: make(map[uint64]*sub.Consumer),
		producers: make(map[uint64]*pub.Producer),
	}
}

// Subscriptions is responsible for storing producers and consumers
// based on their IDs.
type Subscriptions struct {
	cmu       sync.RWMutex // protects following
	consumers map[uint64]*sub.Consumer

	pmu       sync.Mutex // protects following
	producers map[uint64]*pub.Producer
}

func (s *Subscriptions) AddConsumer(c *sub.Consumer) {
	s.cmu.Lock()
	s.consumers[c.ConsumerID] = c
	s.cmu.Unlock()
}

func (s *Subscriptions) DelConsumer(c *sub.Consumer) {
	s.cmu.Lock()
	delete(s.consumers, c.ConsumerID)
	s.cmu.Unlock()
}

func (s *Subscriptions) HandleCloseConsumer(consumerID uint64, f frame.Frame) error {
	s.cmu.Lock()
	defer s.cmu.Unlock()

	c, ok := s.consumers[consumerID]
	if !ok {
		return utils.NewUnexpectedErrMsg(f.BaseCmd.GetType(), consumerID)
	}

	delete(s.consumers, consumerID)

	return c.HandleCloseConsumer(f)
}

func (s *Subscriptions) HandleReachedEndOfTopic(consumerID uint64, f frame.Frame) error {
	s.cmu.Lock()
	defer s.cmu.Unlock()

	c, ok := s.consumers[consumerID]
	if !ok {
		return utils.NewUnexpectedErrMsg(f.BaseCmd.GetType(), consumerID)
	}

	return c.HandleReachedEndOfTopic(f)
}

func (s *Subscriptions) HandleMessage(consumerID uint64, f frame.Frame) error {
	s.cmu.RLock()
	c, ok := s.consumers[consumerID]
	s.cmu.RUnlock()

	if !ok {
		return utils.NewUnexpectedErrMsg(f.BaseCmd.GetType(), consumerID)
	}

	return c.HandleMessage(f)
}

func (s *Subscriptions) AddProducer(p *pub.Producer) {
	s.pmu.Lock()
	s.producers[p.ProducerID] = p
	s.pmu.Unlock()
}

func (s *Subscriptions) DelProducer(p *pub.Producer) {
	s.pmu.Lock()
	delete(s.producers, p.ProducerID)
	s.pmu.Unlock()
}

func (s *Subscriptions) HandleCloseProducer(producerID uint64, f frame.Frame) error {
	s.pmu.Lock()
	defer s.pmu.Unlock()

	p, ok := s.producers[producerID]
	if !ok {
		return utils.NewUnexpectedErrMsg(f.BaseCmd.GetType(), producerID)
	}

	delete(s.producers, producerID)

	return p.HandleCloseProducer(f)
}

func (s *Subscriptions) HandleActiveConsumerChange(consumerID uint64, f frame.Frame) error {
	log.Println("HandleActiveConsumerChange", api.BaseCommand_ACTIVE_CONSUMER_CHANGE, f.BaseCmd)
	return nil
}
