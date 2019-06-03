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

package pub

import (
	"github.com/wolfstudy/pulsar-client-go/pkg/log"
	"github.com/wolfstudy/pulsar-client-go/utils"
)

// MessageRoutingMode
type MessageRoutingMode int

const (
	// RoundRobinDistribution publish messages across all partitions in round-robin.
	RoundRobinDistribution MessageRoutingMode = iota

	// UseSinglePartition the producer will chose one single partition and publish all the messages into that partition
	UseSinglePartition

	// CustomPartition use custom message router implementation that will be called to determine the partition for a particular message.
	CustomPartition
)

// MessageRouterFunc
type MessageRouter interface {
	ChoosePartition(msgKey string, numPartitions uint32) uint32
}

type RoundRobinRouter struct {
	Counter uint32
}

func (router *RoundRobinRouter) ChoosePartition(msgKey string, numPartitions uint32) uint32 {
	if msgKey != "" {
		return signSafeMod(hashFunc(msgKey), numPartitions)
	}
	router.Counter++

	// TODO: not support batch
	return signSafeMod(router.Counter, numPartitions)
}

type SinglePartitionRouter struct {
	Partition uint32
}

func (router *SinglePartitionRouter) ChoosePartition(msgKey string, numPartitions uint32) uint32 {
	if msgKey != "" {
		return signSafeMod(hashFunc(msgKey), numPartitions)
	}
	return router.Partition
}

// TODO: user config
func hashFunc(value string) uint32 {
	return utils.Murmur3_32Hash(value)
}

func signSafeMod(dividend uint32, divisor uint32) uint32 {
	if divisor != 0 {
		mod := dividend % divisor
		if mod < 0 {
			mod += divisor
		}
		return mod
	}
	log.Errorf("the partition numbers:%d is zero, please check.", divisor)
	return 0
}

type HashingScheme int

const (
	JavaStringHash HashingScheme = iota // Java String.hashCode() equivalent
	Murmur3_32Hash                      // Use Murmur3 hashing function
)

func getHashingFunction(s HashingScheme) func(string) uint32 {
	switch s {
	case JavaStringHash:
		return utils.JavaStringHash
	case Murmur3_32Hash:
		return utils.Murmur3_32Hash
	default:
		return utils.JavaStringHash
	}
}
