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
	"sync"

	"github.com/Comcast/pulsar-client-go/core/frame"
	"github.com/Comcast/pulsar-client-go/core/pub"
	"github.com/Comcast/pulsar-client-go/utils"
)

// NewSubscriptions returns a ready-to-use subscriptions.
func NewSubscriptions() *Subscriptions {
	return &Subscriptions{
		Consumers: make(map[uint64]*Consumer),
		Producers: make(map[uint64]*pub.Producer),
	}
}

// Subscriptions is responsible for storing producers and consumers
// based on their IDs.
type Subscriptions struct {
	Cmu       sync.RWMutex // protects following
	Consumers map[uint64]*Consumer

	Pmu       sync.Mutex // protects following
	Producers map[uint64]*pub.Producer
}

func (s *Subscriptions) AddConsumer(c *Consumer) {
	s.Cmu.Lock()
	s.Consumers[c.ConsumerID] = c
	s.Cmu.Unlock()
}

func (s *Subscriptions) DelConsumer(c *Consumer) {
	s.Cmu.Lock()
	delete(s.Consumers, c.ConsumerID)
	s.Cmu.Unlock()
}

func (s *Subscriptions) HandleCloseConsumer(consumerID uint64, f frame.Frame) error {
	s.Cmu.Lock()
	defer s.Cmu.Unlock()

	c, ok := s.Consumers[consumerID]
	if !ok {
		return utils.NewUnexpectedErrMsg(f.BaseCmd.GetType(), consumerID)
	}

	delete(s.Consumers, consumerID)

	return c.HandleCloseConsumer(f)
}

func (s *Subscriptions) HandleReachedEndOfTopic(consumerID uint64, f frame.Frame) error {
	s.Cmu.Lock()
	defer s.Cmu.Unlock()

	c, ok := s.Consumers[consumerID]
	if !ok {
		return utils.NewUnexpectedErrMsg(f.BaseCmd.GetType(), consumerID)
	}

	return c.HandleReachedEndOfTopic(f)
}

func (s *Subscriptions) HandleMessage(consumerID uint64, f frame.Frame) error {
	s.Cmu.RLock()
	c, ok := s.Consumers[consumerID]
	s.Cmu.RUnlock()

	if !ok {
		return utils.NewUnexpectedErrMsg(f.BaseCmd.GetType(), consumerID)
	}

	return c.HandleMessage(f)
}

func (s *Subscriptions) AddProducer(p *pub.Producer) {
	s.Pmu.Lock()
	s.Producers[p.ProducerID] = p
	s.Pmu.Unlock()
}

func (s *Subscriptions) DelProducer(p *pub.Producer) {
	s.Pmu.Lock()
	delete(s.Producers, p.ProducerID)
	s.Pmu.Unlock()
}

func (s *Subscriptions) HandleCloseProducer(producerID uint64, f frame.Frame) error {
	s.Pmu.Lock()
	defer s.Pmu.Unlock()

	p, ok := s.Producers[producerID]
	if !ok {
		return utils.NewUnexpectedErrMsg(f.BaseCmd.GetType(), producerID)
	}

	delete(s.Producers, producerID)

	return p.HandleCloseProducer(f)
}
