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

	"github.com/wolfstudy/pulsar-client-go/core/manage"
)

var clientPool = manage.NewClientPool()

func main() {
	ctx := context.Background()

	consumerConf := manage.ConsumerConfig{
		ClientConfig: manage.ClientConfig{
			Addr: "localhost:6650",
		},
		Topic:            "multi-topic",
		Name:             "sub-1",
		SubMode:          manage.SubscriptionModeKeyShared,
		AckTimeoutMillis: 10000,
	}
	mp := manage.NewManagedConsumer(clientPool, consumerConf)
	//messages := make(chan msg.Message, 16)
	for i := 0; i < 10; i++ {
		msg, err := mp.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(msg.Payload))
	}
}
