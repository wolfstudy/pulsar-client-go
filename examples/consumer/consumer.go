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

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/wolfstudy/pulsar-client-go/core/manage"
)

var clientPool = manage.NewClientPool()

func main() {
	ctx := context.Background()

	consumerConf := manage.ConsumerConfig{
		ClientConfig: manage.ClientConfig{
			Addr: "localhost:6650",
		},

		Topic:   "multi-topic-10",
		Name:    "sub-2",
		SubMode: manage.SubscriptionModeKeyShared,
		//SubMode:          manage.SubscriptionModeShard,
		AckTimeoutMillis: 1000 * 5,
		QueueSize:        5,
	}
	//mp := manage.NewManagedConsumer(clientPool, consumerConf)
	mp, err := manage.NewPartitionManagedConsumer(clientPool, consumerConf)
	if err != nil {
		log.Fatal(err)
	}

	//mp2 := manage.NewManagedConsumer(clientPool, consumerConf)

	mp2, err := manage.NewPartitionManagedConsumer(clientPool, consumerConf)
	if err != nil {
		log.Fatal(err)
	}

	//messages := make(chan msg.Message, 16)
	go func() {
		for {
			msg, err := mp2.Receive(ctx)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("consumer2----> msg key is:%s  msg value is:%s\n", msg.Meta.GetPartitionKey(), string(msg.Payload))
			err = mp2.Ack(ctx, msg)
			if err != nil {
				log.Fatal(err)
			}

		}
	}()

	go func() {
		for {
			msg, err := mp.Receive(ctx)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("consumer1----> msg key is:%s,  msg value is:%s\n", msg.Meta.GetPartitionKey(), string(msg.Payload))
			err = mp.Ack(ctx, msg)

			if err != nil {
				log.Fatal(err)
			}
		}
	}()

	time.Sleep(time.Second * 1000)

}
