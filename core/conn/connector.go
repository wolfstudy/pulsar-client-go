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

package conn

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/wolfstudy/pulsar-client-go/core/frame"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
)

const (
	// ProtoVersion is the Pulsar protocol version
	// used by this client.
	ProtoVersion = int32(api.ProtocolVersion_v12)

	// ClientVersion is an opaque string sent
	// by the client to the server on connect, eg:
	// "Pulsar-Client-Java-v1.15.2"
	ClientVersion = "pulsar-client-go"

	// undefRequestID defines a RequestID of -1.
	//
	// Usage example:
	// https://github.com/apache/incubator-pulsar/blob/fdc7b8426d8253c9437777ae51a4639239550f00/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/ServerCnx.java#L325
	undefRequestID = 1<<64 - 1
)

// NewConnector returns a ready-to-use connector.
func NewConnector(s frame.CmdSender, dispatcher *frame.Dispatcher) *Connector {
	return &Connector{
		s:          s,
		dispatcher: dispatcher,
	}
}

// connector encapsulates the logic for the CONNECT <-> (CONNECTED|ERROR)
// request-response cycle.
//
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Connectionestablishment-ly8l2n
type Connector struct {
	s          frame.CmdSender
	dispatcher *frame.Dispatcher // used to manage the request/response state
}

// Connect initiates the client's session. After sending,
// the client should wait for a `Connected` or `Error`
// response from the server.
//
// The provided context should have a timeout associated with it.
//
// It's required to have completed Connect/Connected before using the client.
func (c *Connector) Connect(ctx context.Context, authMethod string, authData []byte, proxyBrokerURL string) (*api.CommandConnected, error) {
	resp, cancel, err := c.dispatcher.RegisterGlobal()
	if err != nil {
		return nil, err
	}
	defer cancel()

	// NOTE: The source seems to indicate that the ERROR messages's
	// RequestID will be -1 (ie undefRequestID) in the case that it's
	// associated with a CONNECT request.
	// https://github.com/apache/incubator-pulsar/blob/fdc7b8426d8253c9437777ae51a4639239550f00/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/ServerCnx.java#L325
	errResp, cancel, err := c.dispatcher.RegisterReqID(undefRequestID)
	if err != nil {
		return nil, err
	}
	defer cancel()

	// create and send CONNECT msg

	connect := api.CommandConnect{
		ClientVersion:   proto.String(ClientVersion),
		ProtocolVersion: proto.Int32(ProtoVersion),
	}
	if authMethod != "" {
		connect.AuthMethodName = proto.String(authMethod)
		connect.AuthData = authData
	}
	if proxyBrokerURL != "" {
		connect.ProxyToBrokerUrl = proto.String(proxyBrokerURL)
	}

	cmd := api.BaseCommand{
		Type:    api.BaseCommand_CONNECT.Enum(),
		Connect: &connect,
	}

	if err := c.s.SendSimpleCmd(cmd); err != nil {
		return nil, err
	}

	// wait for the response, error, or timeout

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case connectedFrame := <-resp:
		return connectedFrame.BaseCmd.GetConnected(), nil

	case errFrame := <-errResp:
		err := errFrame.BaseCmd.GetError()
		return nil, fmt.Errorf("%s: %s", err.GetError().String(), err.GetMessage())
	}
}
