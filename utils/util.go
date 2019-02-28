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
	"flag"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Comcast/pulsar-client-go/pkg/api"
)

// ################
// helper functions
// ################

var (
	randStringChars = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	randStringMu    = new(sync.Mutex) //protects randStringRand, which isn't threadsafe
	randStringRand  = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// authMethodTLS is the name of the TLS authentication
// method, used in the CONNECT message.
const AuthMethodTLS = "tls"

const (
	// ProtoVersion is the Pulsar protocol version
	// used by this client.
	ProtoVersion = int32(api.ProtocolVersion_v12)

	// ClientVersion is an opaque string sent
	// by the client to the server on connect, eg:
	// "Pulsar-Client-Java-v1.15.2"
	ClientVersion = "pulsar-client-go"

	// NndefRequestID defines a RequestID of -1.
	//
	// Usage example:
	// https://github.com/apache/incubator-pulsar/blob/fdc7b8426d8253c9437777ae51a4639239550f00/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/ServerCnx.java#L325
	UndefRequestID = 1<<64 - 1
)

func RandString(n int) string {
	b := make([]rune, n)
	l := len(randStringChars)
	randStringMu.Lock()
	for i := range b {
		b[i] = randStringChars[randStringRand.Intn(l)]
	}
	randStringMu.Unlock()
	return string(b)
}

// pulsarAddr, if provided, is the Pulsar server to use for integration
// tests (most likely Pulsar standalone running on localhost).
var _PulsarAddr = flag.String("pulsar", "", "Address of Pulsar server to connect to. If blank, tests are skipped")

// PulsarAddr is a helper function that either returns the non-blank
// pulsar address, or skips the test.
func PulsarAddr(t *testing.T) string {
	v := *_PulsarAddr
	if v != "" {
		return v
	}
	t.Skip("pulsar address (-pulsar) not provided")
	return ""
}
