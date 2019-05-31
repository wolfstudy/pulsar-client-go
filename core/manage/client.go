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
	"fmt"

	"github.com/wolfstudy/pulsar-client-go/core/conn"
	"github.com/wolfstudy/pulsar-client-go/core/frame"
	"github.com/wolfstudy/pulsar-client-go/core/msg"
	"github.com/wolfstudy/pulsar-client-go/core/pub"
	"github.com/wolfstudy/pulsar-client-go/core/srv"
	"github.com/wolfstudy/pulsar-client-go/core/sub"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
	"github.com/wolfstudy/pulsar-client-go/utils"
)

// NewClient returns a Pulsar client for the given configuration options.
func NewClient(cfg ClientConfig) (*Client, error) {
	cfg = cfg.SetDefaults()

	var cnx *conn.Conn
	var err error

	if cfg.TLSConfig != nil {
		cnx, err = conn.NewTLSConn(cfg.ConnAddr(), cfg.TLSConfig, cfg.DialTimeout)
	} else {
		cnx, err = conn.NewTCPConn(cfg.ConnAddr(), cfg.DialTimeout)
	}
	if err != nil {
		return nil, err
	}

	reqID := msg.MonotonicID{ID: 0}

	dispatcher := frame.NewFrameDispatcher()
	subs := NewSubscriptions()

	c := &Client{
		C:         cnx,
		AsyncErrs: utils.AsyncErrors(cfg.Errs),

		Dispatcher:    dispatcher,
		Subscriptions: subs,
		Connector:     conn.NewConnector(cnx, dispatcher),
		Pinger:        srv.NewPinger(cnx, dispatcher),
		Discoverer:    srv.NewDiscoverer(cnx, dispatcher, &reqID),
		Pubsub:        NewPubsub(cnx, dispatcher, subs, &reqID),
	}

	handler := func(f frame.Frame) {
		// All message types can be handled in
		// parallel, since their ordering should not matter
		go c.handleFrame(f)
	}

	go func() {
		// If core.read() unblocks, it indicates that
		// the connection has been closed and is no longer usable.
		defer func() {
			if err := c.Close(); err != nil {
				c.AsyncErrs.Send(err)
			}
		}()

		if err := cnx.Read(handler); err != nil {
			c.AsyncErrs.Send(err)
		}
	}()

	return c, nil
}

// Client is a Pulsar client, capable of sending and receiving
// messages and managing the associated state.
type Client struct {
	C         *conn.Conn
	AsyncErrs utils.AsyncErrors

	Dispatcher *frame.Dispatcher

	Subscriptions *Subscriptions
	Connector     *conn.Connector
	Pinger        *srv.Pinger
	Discoverer    *srv.Discoverer
	Pubsub        *Pubsub
}

// Closed returns a channel that unblocks when the client's connection
// has been closed and is no longer usable. Users should monitor this
// channel and recreate the Client if closed.
// TODO: Rename to Done
func (c *Client) Closed() <-chan struct{} {
	return c.C.Closed()
}

// Close closes the connection. The channel returned from `Closed` will unblock.
// The client should no longer be used after calling Close.
func (c *Client) Close() error {
	return c.C.Close()
}

// Connect sends a Connect message to the Pulsar server, then
// waits for either a CONNECTED response or the context to
// timeout. Connect should be called immediately after
// creating a client, before sending any other messages.
// The "auth method" is not set in the CONNECT message.
// See ConnectTLS for TLS auth method.
// The proxyBrokerURL may be blank, or it can be used to indicate
// that the client is connecting through a proxy server.
// See "Connection establishment" for more info:
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Connectionestablishment-6pslvw
func (c *Client) Connect(ctx context.Context, proxyBrokerURL string) (*api.CommandConnected, error) {
	return c.Connector.Connect(ctx, "", proxyBrokerURL)
}

// ConnectTLS sends a Connect message to the Pulsar server, then
// waits for either a CONNECTED response or the context to
// timeout. Connect should be called immediately after
// creating a client, before sending any other messages.
// The "auth method" is set to tls in the CONNECT message.
// The proxyBrokerURL may be blank, or it can be used to indicate
// that the client is connecting through a proxy server.
// See "Connection establishment" for more info:
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Connectionestablishment-6pslvw
func (c *Client) ConnectTLS(ctx context.Context, proxyBrokerURL string) (*api.CommandConnected, error) {
	return c.Connector.Connect(ctx, utils.AuthMethodTLS, proxyBrokerURL)
}

// Ping sends a PING message to the Pulsar server, then
// waits for either a PONG response or the context to
// timeout.
func (c *Client) Ping(ctx context.Context) error {
	return c.Pinger.Ping(ctx)
}

// LookupTopic returns metadata about the given topic. Topic lookup needs
// to be performed each time a client needs to create or reconnect a
// producer or a consumer. Lookup is used to discover which particular
// broker is serving the topic we are about to use.
//
// The command has to be used in a connection that has already gone
// through the Connect / Connected initial handshake.
// See "Topic lookup" for more info:
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/#Topiclookup-rxds6i
func (c *Client) LookupTopic(ctx context.Context, topic string, authoritative bool) (*api.CommandLookupTopicResponse, error) {
	return c.Discoverer.LookupTopic(ctx, topic, authoritative)
}

// NewProducer creates a new producer capable of sending message to the
// given topic.
func (c *Client) NewProducer(ctx context.Context, topic, producerName string) (*pub.Producer, error) {
	return c.Pubsub.Producer(ctx, topic, producerName)
}

// NewProducer creates a new producer capable of sending message to the
// given topic.
func (c *Client) NewPartitionedProducer(ctx context.Context, topic, producerName string, partitionNums uint32, router pub.MessageRouter) (*pub.PartitionedProducer, error) {
	return c.Pubsub.PartitionedProducer(ctx, topic, producerName, partitionNums, router)
}

// NewSharedConsumer creates a new shared consumer capable of reading messages from the
// given topic.
// See "Subscription modes" for more information:
// https://pulsar.incubator.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Subscriptionmodes-jdrefl
func (c *Client) NewSharedConsumer(ctx context.Context, topic, subscriptionName string, queue chan msg.Message) (*sub.Consumer, error) {
	return c.Pubsub.Subscribe(ctx, topic, subscriptionName, api.CommandSubscribe_Shared, api.CommandSubscribe_Latest, queue)
}

// NewExclusiveConsumer creates a new exclusive consumer capable of reading messages from the
// given topic.
// See "Subscription modes" for more information:
// https://pulsar.incubator.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Subscriptionmodes-jdrefl
func (c *Client) NewExclusiveConsumer(ctx context.Context, topic, subscriptionName string, earliest bool, queue chan msg.Message) (*sub.Consumer, error) {
	initialPosition := api.CommandSubscribe_Latest
	if earliest {
		initialPosition = api.CommandSubscribe_Earliest
	}
	return c.Pubsub.Subscribe(ctx, topic, subscriptionName, api.CommandSubscribe_Exclusive, initialPosition, queue)
}

// NewFailoverConsumer creates a new failover consumer capable of reading messages from the
// given topic.
// See "Subscription modes" for more information:
// https://pulsar.incubator.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Subscriptionmodes-jdrefl
func (c *Client) NewFailoverConsumer(ctx context.Context, topic, subscriptionName string, queue chan msg.Message) (*sub.Consumer, error) {
	return c.Pubsub.Subscribe(ctx, topic, subscriptionName, api.CommandSubscribe_Failover, api.CommandSubscribe_Latest, queue)
}

// handleFrame is called by the underlaying core with
// all received Frames.
func (c *Client) handleFrame(f frame.Frame) {
	var err error

	msgType := f.BaseCmd.GetType()

	switch msgType {

	// Solicited responses with NO response ID associated

	case api.BaseCommand_CONNECTED:
		err = c.Dispatcher.NotifyGlobal(f)

	case api.BaseCommand_PONG:
		err = c.Dispatcher.NotifyGlobal(f)

	// Solicited responses with a requestID to correlate
	// it to its request

	case api.BaseCommand_SUCCESS:
		err = c.Dispatcher.NotifyReqID(f.BaseCmd.GetSuccess().GetRequestId(), f)

	case api.BaseCommand_ERROR:
		err = c.Dispatcher.NotifyReqID(f.BaseCmd.GetError().GetRequestId(), f)

	case api.BaseCommand_LOOKUP_RESPONSE:
		err = c.Dispatcher.NotifyReqID(f.BaseCmd.GetLookupTopicResponse().GetRequestId(), f)

	case api.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		err = c.Dispatcher.NotifyReqID(f.BaseCmd.GetPartitionMetadataResponse().GetRequestId(), f)

	case api.BaseCommand_PRODUCER_SUCCESS:
		err = c.Dispatcher.NotifyReqID(f.BaseCmd.GetProducerSuccess().GetRequestId(), f)

	// Solicited responses with a (producerID, sequenceID) tuple to correlate
	// it to its request

	case api.BaseCommand_SEND_RECEIPT:
		msg := f.BaseCmd.GetSendReceipt()
		err = c.Dispatcher.NotifyProdSeqIDs(msg.GetProducerId(), msg.GetSequenceId(), f)

	case api.BaseCommand_SEND_ERROR:
		msg := f.BaseCmd.GetSendError()
		err = c.Dispatcher.NotifyProdSeqIDs(msg.GetProducerId(), msg.GetSequenceId(), f)

	// Unsolicited responses that have a producer ID

	case api.BaseCommand_CLOSE_PRODUCER:
		err = c.Subscriptions.HandleCloseProducer(f.BaseCmd.GetCloseProducer().GetProducerId(), f)

	// Unsolicited responses that have a consumer ID

	case api.BaseCommand_CLOSE_CONSUMER:
		err = c.Subscriptions.HandleCloseConsumer(f.BaseCmd.GetCloseConsumer().GetConsumerId(), f)

	case api.BaseCommand_REACHED_END_OF_TOPIC:
		err = c.Subscriptions.HandleReachedEndOfTopic(f.BaseCmd.GetReachedEndOfTopic().GetConsumerId(), f)

	case api.BaseCommand_MESSAGE:
		err = c.Subscriptions.HandleMessage(f.BaseCmd.GetMessage().GetConsumerId(), f)

	// Unsolicited responses

	case api.BaseCommand_PING:
		err = c.Pinger.HandlePing(msgType, f.BaseCmd.GetPing())

	// In the failover subscription mode,
	// all consumers receive ACTIVE_CONSUMER_CHANGE when a new subscriber is created or a subscriber exits.
	case api.BaseCommand_ACTIVE_CONSUMER_CHANGE:
		err = c.Subscriptions.HandleActiveConsumerChange(f.BaseCmd.GetActiveConsumerChange().GetConsumerId(), f)
	default:
		err = fmt.Errorf("unhandled message of type %q", f.BaseCmd.GetType())
	}

	if err != nil {
		c.AsyncErrs.Send(err)
	}
}
