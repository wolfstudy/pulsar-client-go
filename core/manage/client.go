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
	"crypto/tls"
	"fmt"
	"time"

	"github.com/wolfstudy/pulsar-client-go/core/conn"
	"github.com/wolfstudy/pulsar-client-go/core/frame"
	"github.com/wolfstudy/pulsar-client-go/core/msg"
	"github.com/wolfstudy/pulsar-client-go/core/pub"
	"github.com/wolfstudy/pulsar-client-go/core/srv"
	"github.com/wolfstudy/pulsar-client-go/core/sub"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
	"github.com/wolfstudy/pulsar-client-go/utils"
)

// authMethodTLS is the name of the TLS authentication
// method, used in the CONNECT message.
const authMethodTLS = "tls"

// ClientConfig is used to configure a Pulsar client.
type ClientConfig struct {
	Addr        string        // pulsar broker address. May start with pulsar://
	phyAddr     string        // if set, the TCP connection should be made using this address. This is only ever set during Topic Lookup
	DialTimeout time.Duration // timeout to use when establishing TCP connection
	TLSConfig   *tls.Config   // TLS configuration. May be nil, in which case TLS will not be used
	Errs        chan<- error  // asynchronous errors will be sent here. May be nil
}

// connAddr returns the address that should be used
// for the TCP connection. It defaults to phyAddr if set,
// otherwise Addr. This is to support the proxying through
// a broker, as determined during topic lookup.
func (c ClientConfig) connAddr() string {
	if c.phyAddr != "" {
		return c.phyAddr
	}
	return c.Addr
}

// setDefaults returns a modified config with appropriate zero values set to defaults.
func (c ClientConfig) setDefaults() ClientConfig {
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}

	return c
}

// NewClient returns a Pulsar client for the given configuration options.
func NewClient(cfg ClientConfig) (*Client, error) {
	cfg = cfg.setDefaults()

	var cnx *conn.Conn
	var err error

	if cfg.TLSConfig != nil {
		cnx, err = conn.NewTLSConn(cfg.connAddr(), cfg.TLSConfig, cfg.DialTimeout)
	} else {
		cnx, err = conn.NewTCPConn(cfg.connAddr(), cfg.DialTimeout)
	}
	if err != nil {
		return nil, err
	}

	reqID := msg.MonotonicID{ID: 0}

	dispatcher := frame.NewFrameDispatcher()
	subs := NewSubscriptions()

	c := &Client{
		c:         cnx,
		asyncErrs: utils.AsyncErrors(cfg.Errs),

		dispatcher:    dispatcher,
		subscriptions: subs,
		connector:     conn.NewConnector(cnx, dispatcher),
		pinger:        srv.NewPinger(cnx, dispatcher),
		discoverer:    srv.NewDiscoverer(cnx, dispatcher, &reqID),
		pubsub:        NewPubsub(cnx, dispatcher, subs, &reqID),
	}

	handler := func(f frame.Frame) {
		c.handleFrame(f)
	}

	go func() {
		// If core.read() unblocks, it indicates that
		// the connection has been closed and is no longer usable.
		defer func() {
			if err := c.Close(); err != nil {
				c.asyncErrs.Send(err)
			}
		}()

		if err := cnx.Read(handler); err != nil {
			c.asyncErrs.Send(err)
		}
	}()

	return c, nil
}

// Client is a Pulsar client, capable of sending and receiving
// messages and managing the associated state.
type Client struct {
	c         *conn.Conn
	asyncErrs utils.AsyncErrors

	dispatcher *frame.Dispatcher

	subscriptions *Subscriptions
	connector     *conn.Connector
	pinger        *srv.Pinger
	discoverer    *srv.Discoverer
	pubsub        *Pubsub
}

// Closed returns a channel that unblocks when the client's connection
// has been closed and is no longer usable. Users should monitor this
// channel and recreate the Client if closed.
// TODO: Rename to Done
func (c *Client) Closed() <-chan struct{} {
	return c.c.Closed()
}

// Close closes the connection. The channel returned from `Closed` will unblock.
// The client should no longer be used after calling Close.
func (c *Client) Close() error {
	return c.c.Close()
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
	return c.connector.Connect(ctx, "", proxyBrokerURL)
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
	return c.connector.Connect(ctx, authMethodTLS, proxyBrokerURL)
}

// Ping sends a PING message to the Pulsar server, then
// waits for either a PONG response or the context to
// timeout.
func (c *Client) Ping(ctx context.Context) error {
	return c.pinger.Ping(ctx)
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
	return c.discoverer.LookupTopic(ctx, topic, authoritative)
}

// NewProducer creates a new producer capable of sending message to the
// given topic.
func (c *Client) NewProducer(ctx context.Context, topic, producerName string) (*pub.Producer, error) {
	return c.pubsub.Producer(ctx, topic, producerName)
}

// NewSharedConsumer creates a new shared consumer capable of reading messages from the
// given topic.
// See "Subscription modes" for more information:
// https://pulsar.incubator.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Subscriptionmodes-jdrefl
func (c *Client) NewSharedConsumer(ctx context.Context, topic, subscriptionName string, queue chan msg.Message) (*sub.Consumer, error) {
	return c.pubsub.Subscribe(ctx, topic, subscriptionName, api.CommandSubscribe_Shared, api.CommandSubscribe_Latest, queue)
}

func (c *Client) NewConsumerWithCfg(ctx context.Context, cfg ManagedConsumerConfig, queue chan msg.Message) (*sub.Consumer, error) {
	return c.pubsub.SubscribeWithCfg(ctx, cfg, queue)
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
	return c.pubsub.Subscribe(ctx, topic, subscriptionName, api.CommandSubscribe_Exclusive, initialPosition, queue)
}

// NewFailoverConsumer creates a new failover consumer capable of reading messages from the
// given topic.
// See "Subscription modes" for more information:
// https://pulsar.incubator.apache.org/docs/latest/getting-started/ConceptsAndArchitecture/#Subscriptionmodes-jdrefl
func (c *Client) NewFailoverConsumer(ctx context.Context, topic, subscriptionName string, queue chan msg.Message) (*sub.Consumer, error) {
	return c.pubsub.Subscribe(ctx, topic, subscriptionName, api.CommandSubscribe_Failover, api.CommandSubscribe_Latest, queue)
}

// handleFrame is called by the underlaying core with
// all received Frames.
func (c *Client) handleFrame(f frame.Frame) {
	var err error

	msgType := f.BaseCmd.GetType()

	switch msgType {

	// Solicited responses with NO response ID associated

	case api.BaseCommand_CONNECTED:
		err = c.dispatcher.NotifyGlobal(f)

	case api.BaseCommand_PONG:
		err = c.dispatcher.NotifyGlobal(f)

	// Solicited responses with a requestID to correlate
	// it to its request

	case api.BaseCommand_SUCCESS:
		err = c.dispatcher.NotifyReqID(f.BaseCmd.GetSuccess().GetRequestId(), f)

	case api.BaseCommand_ERROR:
		err = c.dispatcher.NotifyReqID(f.BaseCmd.GetError().GetRequestId(), f)

	case api.BaseCommand_LOOKUP_RESPONSE:
		err = c.dispatcher.NotifyReqID(f.BaseCmd.GetLookupTopicResponse().GetRequestId(), f)

	case api.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		err = c.dispatcher.NotifyReqID(f.BaseCmd.GetPartitionMetadataResponse().GetRequestId(), f)

	case api.BaseCommand_PRODUCER_SUCCESS:
		err = c.dispatcher.NotifyReqID(f.BaseCmd.GetProducerSuccess().GetRequestId(), f)

	// Solicited responses with a (producerID, sequenceID) tuple to correlate
	// it to its request

	case api.BaseCommand_SEND_RECEIPT:
		msg := f.BaseCmd.GetSendReceipt()
		err = c.dispatcher.NotifyProdSeqIDs(msg.GetProducerId(), msg.GetSequenceId(), f)

	case api.BaseCommand_SEND_ERROR:
		msg := f.BaseCmd.GetSendError()
		err = c.dispatcher.NotifyProdSeqIDs(msg.GetProducerId(), msg.GetSequenceId(), f)

	// Unsolicited responses that have a producer ID

	case api.BaseCommand_CLOSE_PRODUCER:
		err = c.subscriptions.HandleCloseProducer(f.BaseCmd.GetCloseProducer().GetProducerId(), f)

	// Unsolicited responses that have a consumer ID

	case api.BaseCommand_CLOSE_CONSUMER:
		err = c.subscriptions.HandleCloseConsumer(f.BaseCmd.GetCloseConsumer().GetConsumerId(), f)

	case api.BaseCommand_REACHED_END_OF_TOPIC:
		err = c.subscriptions.HandleReachedEndOfTopic(f.BaseCmd.GetReachedEndOfTopic().GetConsumerId(), f)

	case api.BaseCommand_MESSAGE:
		err = c.subscriptions.HandleMessage(f.BaseCmd.GetMessage().GetConsumerId(), f)

	// Unsolicited responses

	case api.BaseCommand_PING:
		err = c.pinger.HandlePing(msgType, f.BaseCmd.GetPing())

	// In the failover subscription mode,
	// all consumers receive ACTIVE_CONSUMER_CHANGE when a new subscriber is created or a subscriber exits.
	case api.BaseCommand_ACTIVE_CONSUMER_CHANGE:
		err = c.subscriptions.HandleActiveConsumerChange(f.BaseCmd.GetActiveConsumerChange().GetConsumerId(), f)
	default:
		err = fmt.Errorf("unhandled message of type %q", f.BaseCmd.GetType())
	}

	if err != nil {
		c.asyncErrs.Send(err)
	}
}
