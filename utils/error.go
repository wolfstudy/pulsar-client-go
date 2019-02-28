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

package utils

import (
	"fmt"

	"github.com/Comcast/pulsar-client-go/pkg/api"
)

// AsyncErrors provides idiom for sending in a non-blocking
// manner errors to a channel. Note: it's legal for asyncErrors
// to be nil
type AsyncErrors chan<- error

// Send places the error on the channel in a non-blocking way
func (a AsyncErrors) Send(err error) {
	// note: `a` can be nil and still work properly
	select {
	case a <- err:
	default:
	}
}

// NewUnexpectedErrMsg instantiates an ErrUnexpectedMsg error.
// Optionally provide a list of IDs associated with the message
// for additional context in the error message.
func NewUnexpectedErrMsg(msgType api.BaseCommand_Type, ids ...interface{}) *UnexpectedErrMsg {
	return &UnexpectedErrMsg{
		msgType: msgType,
		ids:     ids,
	}
}

// UnexpectedErrMsg is returned when an unexpected
// message is received.
type UnexpectedErrMsg struct {
	msgType api.BaseCommand_Type
	ids     []interface{}
}

// Error satisfies the error interface.
func (e *UnexpectedErrMsg) Error() string {
	msg := fmt.Sprintf("received unexpected message of type %q", e.msgType.String())
	for _, id := range e.ids {
		msg += fmt.Sprintf(" id=%v", id)
	}
	return msg
}
