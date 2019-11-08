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
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/wolfstudy/pulsar-client-go/core/frame"
	"github.com/wolfstudy/pulsar-client-go/core/msg"
	"github.com/wolfstudy/pulsar-client-go/core/pub"
	"github.com/wolfstudy/pulsar-client-go/core/sub"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
)

func TestPubsub_Subscribe_Success(t *testing.T) {
	var ms frame.MockSender
	id := uint64(42)
	consID := uint64(123)
	reqID := &msg.MonotonicID{ID: id}
	dispatcher := frame.NewFrameDispatcher()
	subs := NewSubscriptions()

	tp := NewPubsub(&ms, dispatcher, subs, reqID)
	// manually set consumerID to verify that it's correctly
	// being set on Consumer
	tp.consumerID = &msg.MonotonicID{ID: consID}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type response struct {
		c   *sub.Consumer
		err error
	}
	resp := make(chan response, 1)

	go func() {
		var r response
		r.c, r.err = tp.Subscribe(ctx, "test-topic", "test-subscription", api.CommandSubscribe_Exclusive,
			api.CommandSubscribe_Latest, make(chan msg.Message, 1))
		resp <- r
	}()

	// Allow goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	// send success response
	success := api.CommandSuccess{
		RequestId: proto.Uint64(id),
	}
	f := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type:    api.BaseCommand_SUCCESS.Enum(),
			Success: &success,
		},
	}
	if err := dispatcher.NotifyReqID(id, f); err != nil {
		t.Fatalf("dispatcher.HandleReqID() err = %v; nil expected", err)
	}

	r := <-resp
	if r.err != nil {
		t.Fatalf("subscribe() err = %v; expected nil", r.err)
	}

	got := r.c
	t.Logf("subscribe() got %+v", got)

	if got.ConsumerID != consID {
		t.Fatalf("got Consumer.consumerID = %d; expected %d", got.ConsumerID, consID)
	}

	if _, ok := subs.consumers[got.ConsumerID]; !ok {
		t.Fatalf("subscriptions.consumers[%d] is absent; expected consumer", got.ConsumerID)
	}
}

func TestPubsub_Subscribe_Error(t *testing.T) {
	var ms frame.MockSender
	id := uint64(42)
	reqID := &msg.MonotonicID{ID: id}
	dispatcher := frame.NewFrameDispatcher()
	subs := NewSubscriptions()

	tp := NewPubsub(&ms, dispatcher, subs, reqID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type response struct {
		c   *sub.Consumer
		err error
	}
	resp := make(chan response, 1)

	go func() {
		var r response
		r.c, r.err = tp.Subscribe(ctx, "test-topic", "test-subscription", api.CommandSubscribe_Exclusive,
			api.CommandSubscribe_Latest, make(chan msg.Message, 1))
		resp <- r
	}()

	// Allow goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	// send error response
	cmdErr := api.CommandError{
		RequestId: proto.Uint64(id),
		Message:   proto.String("oh noo"),
	}
	f := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type:  api.BaseCommand_ERROR.Enum(),
			Error: &cmdErr,
		},
	}
	if err := dispatcher.NotifyReqID(id, f); err != nil {
		t.Fatalf("dispatcher.HandleReqID() err = %v; nil expected", err)
	}

	r := <-resp
	if r.err == nil {
		t.Fatalf("subscribe() err = %v; expected non-nil", r.err)
	}
	t.Logf("subscribe() err = %v", r.err)

	if got, expected := len(subs.consumers), 0; got != expected {
		t.Fatalf("subscriptions.consumers has %d elements; expected %d", got, expected)
	}
}

func TestPubsub_Producer_Success(t *testing.T) {
	var ms frame.MockSender
	id := uint64(42)
	prodID := uint64(123)
	reqID := &msg.MonotonicID{ID: id}
	dispatcher := frame.NewFrameDispatcher()
	subs := NewSubscriptions()

	tp := NewPubsub(&ms, dispatcher, subs, reqID)
	// manually set producerID to verify that it's correctly
	// being set on Producer
	tp.producerID = &msg.MonotonicID{ID: prodID}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type response struct {
		p   *pub.Producer
		err error
	}
	resp := make(chan response, 1)

	go func() {
		var r response
		r.p, r.err = tp.Producer(ctx, "test-topic", "test-name")
		resp <- r
	}()

	// Allow goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	// send success response
	prodName := "returned producer name"
	success := api.CommandProducerSuccess{
		RequestId:      proto.Uint64(id),
		LastSequenceId: proto.Int64(-1),
		ProducerName:   proto.String(prodName),
	}
	f := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type:            api.BaseCommand_PRODUCER_SUCCESS.Enum(),
			ProducerSuccess: &success,
		},
	}
	if err := dispatcher.NotifyReqID(id, f); err != nil {
		t.Fatalf("dispatcher.HandleReqID() err = %v; nil expected", err)
	}

	r := <-resp
	if r.err != nil {
		t.Fatalf("subscribe() err = %v; expected nil", r.err)
	}

	got := r.p
	t.Logf("producer() got %+v", got)

	if got.ProducerID != prodID {
		t.Fatalf("got Producer.producerID = %d; expected %d", got.ProducerID, prodID)
	}
	if got.ProducerName != prodName {
		t.Fatalf("got Producer.producerName = %q; expected %q", got.ProducerName, prodName)
	}

	if _, ok := subs.producers[got.ProducerID]; !ok {
		t.Fatalf("subscriptions.producers[%d] is absent; expected producer", got.ProducerID)
	}
}

func TestPubsub_Producer_Error(t *testing.T) {
	var ms frame.MockSender
	id := uint64(42)
	prodID := uint64(123)
	reqID := &msg.MonotonicID{ID: id}
	dispatcher := frame.NewFrameDispatcher()
	subs := NewSubscriptions()

	tp := NewPubsub(&ms, dispatcher, subs, reqID)
	// manually set producerID to verify that it's correctly
	// being set on Producer
	tp.producerID = &msg.MonotonicID{ID: prodID}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type response struct {
		p   *pub.Producer
		err error
	}
	resp := make(chan response, 1)

	go func() {
		var r response
		r.p, r.err = tp.Producer(ctx, "test-topic", "test-name")
		resp <- r
	}()

	// Allow goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	// send error response
	cmdErr := api.CommandError{
		RequestId: proto.Uint64(id),
		Message:   proto.String("oh noo"),
	}
	f := frame.Frame{
		BaseCmd: &api.BaseCommand{
			Type:  api.BaseCommand_ERROR.Enum(),
			Error: &cmdErr,
		},
	}
	if err := dispatcher.NotifyReqID(id, f); err != nil {
		t.Fatalf("dispatcher.HandleReqID() err = %v; nil expected", err)
	}

	r := <-resp
	if r.err == nil {
		t.Fatalf("producer() err = %v; expected non-nil", r.err)
	}
	t.Logf("producer() err = %v", r.err)

	if got, expected := len(subs.producers), 0; got != expected {
		t.Fatalf("subscriptions.producers has %d elements; expected %d", got, expected)
	}
}
