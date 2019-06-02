/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package manage

import (
	"sync"
	"time"

	set "github.com/deckarep/golang-set"
	"github.com/golang/protobuf/proto"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
	"github.com/wolfstudy/pulsar-client-go/pkg/log"
)

type UnackedMessageTracker struct {
	cmu               sync.RWMutex // protects following
	currentSet        set.Set
	oldOpenSet        set.Set
	timeout           time.Ticker
	consumer          ManagedConsumer
	partitionConsumer ManagedPartitionConsumer
}

func NewUnackedMessageTracker() *UnackedMessageTracker {
	UnAckTracker := &UnackedMessageTracker{
		currentSet: set.NewSet(),
		oldOpenSet: set.NewSet(),
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
						ConsumerId: proto.Uint64(t.consumer.consumer.ConsumerID),
						MessageIds: messageIds,
					},
				}

				if err := t.consumer.consumer.S.SendSimpleCmd(cmd); err != nil {
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
