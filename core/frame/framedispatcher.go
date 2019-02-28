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

package frame

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Comcast/pulsar-client-go/utils"
)

// NewFrameDispatcher returns an instantiated FrameDispatcher.
func NewFrameDispatcher() *Dispatcher {
	return &Dispatcher{
		ProdSeqIDs: make(map[ProdSeqKey]AsyncResp),
		ReqIDs:     make(map[uint64]AsyncResp),
	}
}

// Dispatcher is Responsible for handling the request/Response
// state of outstanding requests. It allows for users of this
// type to present a synchronous interface to an asynchronous
// process.
type Dispatcher struct {
	// Connected and Pong Responses have no requestID,
	// therefore a single channel is used as their
	// Respective FrameDispatcher. If the channel is
	// nil, there's no outstanding request.
	GlobalMu sync.Mutex // protects following
	Global   *AsyncResp

	// All Responses that are correlated by their
	// requestID
	ReqIDMu sync.Mutex // protects following
	ReqIDs  map[uint64]AsyncResp

	// All Responses that are correlated by their
	// (producerID, sequenceID) tuple
	ProdSeqIDsMu sync.Mutex // protects following
	ProdSeqIDs   map[ProdSeqKey]AsyncResp
}

// AsyncResp manages the state between a request
// and Response. Requestors wait on the `Resp` channel
// for the corResponding Response frame to their request.
// If they are no longer interested in the Response (timeout),
// then the `done` channel is closed, signaling to the Response
// side that the Response is not expected/needed.
type AsyncResp struct {
	Resp chan<- Frame
	Done <-chan struct{}
}

// prodSeqKey is a composite lookup key for the dispatchers
// that use producerID and sequenceID to correlate Responses,
// which are the SendReceipt and SendError Responses.
type ProdSeqKey struct {
	ProducerID uint64
	SequenceID uint64
}

// RegisterGlobal is used to wait for Responses that have no identifying
// id (Pong, Connected Responses). Only one outstanding global request
// is allowed at a time. Callers should always call cancel, specifically
// when they're not interested in the Response.
func (f *Dispatcher) RegisterGlobal() (Response <-chan Frame, cancel func(), err error) {
	var mu sync.Mutex
	done := make(chan struct{})
	cancel = func() {
		mu.Lock()
		defer mu.Unlock()
		if done == nil {
			return
		}

		f.GlobalMu.Lock()
		f.Global = nil
		f.GlobalMu.Unlock()

		close(done)
		done = nil
	}

	Resp := make(chan Frame)

	f.GlobalMu.Lock()
	if f.Global != nil {
		f.GlobalMu.Unlock()
		return nil, nil, errors.New("outstanding global request already in progress")
	}
	f.Global = &AsyncResp{
		Resp: Resp,
		Done: done,
	}
	f.GlobalMu.Unlock()

	return Resp, cancel, nil
}

// NotifyGlobal should be called with Response frames that have
// no identifying id (Pong, Connected).
func (f *Dispatcher) NotifyGlobal(frame Frame) error {
	f.GlobalMu.Lock()
	a := f.Global
	// ensure additional calls to notify
	// fail with UnexpectedMsg (unless register is called again)
	f.Global = nil
	f.GlobalMu.Unlock()

	if a == nil {
		return utils.NewUnexpectedErrMsg(frame.BaseCmd.GetType())
	}

	select {
	case a.Resp <- frame:
		// sent Response back to sender
		return nil
	case <-a.Done:
		return utils.NewUnexpectedErrMsg(frame.BaseCmd.GetType())
	}
}

// RegisterProdSeqID is used to wait for Responses that have (producerID, sequenceID)
// id tuples to correlate them to their request. Callers should always call cancel,
// specifically when they're not interested in the Response. It is an error
// to have multiple outstanding requests with the same id tuple.
func (f *Dispatcher) RegisterProdSeqIDs(producerID, sequenceID uint64) (Response <-chan Frame, cancel func(), err error) {
	key := ProdSeqKey{producerID, sequenceID}

	var mu sync.Mutex
	done := make(chan struct{})
	cancel = func() {
		mu.Lock()
		defer mu.Unlock()
		if done == nil {
			return
		}

		f.ProdSeqIDsMu.Lock()
		delete(f.ProdSeqIDs, key)
		f.ProdSeqIDsMu.Unlock()

		close(done)
		done = nil
	}

	Resp := make(chan Frame)

	f.ProdSeqIDsMu.Lock()
	if _, ok := f.ProdSeqIDs[key]; ok {
		f.ProdSeqIDsMu.Unlock()
		return nil, nil, fmt.Errorf("already exists an outstanding Response for producerID %d, sequenceID %d", producerID, sequenceID)
	}
	f.ProdSeqIDs[key] = AsyncResp{
		Resp: Resp,
		Done: done,
	}
	f.ProdSeqIDsMu.Unlock()

	return Resp, cancel, nil
}

// NotifyProdSeqIDs should be called with Response frames that have
// (producerID, sequenceID) id tuples to correlate them to their requests.
func (f *Dispatcher) NotifyProdSeqIDs(producerID, sequenceID uint64, frame Frame) error {
	key := ProdSeqKey{producerID, sequenceID}

	f.ProdSeqIDsMu.Lock()
	// fetch Response channel from cubbyhole
	a, ok := f.ProdSeqIDs[key]
	// ensure additional calls to notify with same key will
	// fail with UnexpectedMsg (unless registerProdSeqIDs with same key is called)
	delete(f.ProdSeqIDs, key)
	f.ProdSeqIDsMu.Unlock()

	if !ok {
		return utils.NewUnexpectedErrMsg(frame.BaseCmd.GetType(), producerID, sequenceID)
	}

	select {
	case a.Resp <- frame:
		// Response was correctly pushed into channel
		return nil
	case <-a.Done:
		return utils.NewUnexpectedErrMsg(frame.BaseCmd.GetType(), producerID, sequenceID)
	}
}

// RegisterReqID is used to wait for Responses that have a requestID
// id to correlate them to their request. Callers should always call cancel,
// specifically when they're not interested in the Response. It is an error
// to have multiple outstanding requests with the id.
func (f *Dispatcher) RegisterReqID(requestID uint64) (Response <-chan Frame, cancel func(), err error) {
	var mu sync.Mutex
	done := make(chan struct{})
	cancel = func() {
		mu.Lock()
		defer mu.Unlock()
		if done == nil {
			return
		}

		f.ReqIDMu.Lock()
		delete(f.ReqIDs, requestID)
		f.ReqIDMu.Unlock()

		close(done)
		done = nil
	}

	Resp := make(chan Frame)

	f.ReqIDMu.Lock()
	if _, ok := f.ReqIDs[requestID]; ok {
		f.ReqIDMu.Unlock()
		return nil, nil, fmt.Errorf("already exists an outstanding Response for requestID %d", requestID)
	}
	f.ReqIDs[requestID] = AsyncResp{
		Resp: Resp,
		Done: done,
	}
	f.ReqIDMu.Unlock()

	return Resp, cancel, nil
}

// NotifyReqID should be called with Response frames that have
// a requestID to correlate them to their requests.
func (f *Dispatcher) NotifyReqID(requestID uint64, frame Frame) error {
	f.ReqIDMu.Lock()
	// fetch Response channel from cubbyhole
	a, ok := f.ReqIDs[requestID]
	// ensure additional calls to notifyReqID with same key will
	// fail with UnexpectedMsg (unless addReqID with same key is called)
	delete(f.ReqIDs, requestID)
	f.ReqIDMu.Unlock()

	if !ok {
		return utils.NewUnexpectedErrMsg(frame.BaseCmd.GetType(), requestID)
	}

	// send received message to Response channel
	select {
	case a.Resp <- frame:
		// Response was correctly pushed into channel
		return nil
	case <-a.Done:
		return utils.NewUnexpectedErrMsg(frame.BaseCmd.GetType(), requestID)
	}
}
