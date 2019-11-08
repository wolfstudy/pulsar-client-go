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

// This program offers a simple CLI utility for interacting
// with a Pulsar server using the `pulsar` package.
//
// It's main goal is to aid in testing and debugging of the `pulsar`
// package.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wolfstudy/pulsar-client-go/core/auth"
	"github.com/wolfstudy/pulsar-client-go/core/manage"
	"github.com/wolfstudy/pulsar-client-go/core/msg"
)

var args = struct {
	pulsar        string
	tlsCert       string
	tlsKey        string
	tlsCA         string
	tlsSkipVerify bool
	name          string
	topic         string
	producer      bool
	message       string
	messageRate   time.Duration
	shared        bool
}{
	pulsar:        "localhost:6650",
	tlsCert:       "",
	tlsKey:        "",
	tlsCA:         "",
	tlsSkipVerify: false,
	name:          "demo",
	topic:         "persistent://sample/standalone/ns1/demo",
	producer:      false,
	message:       "--",
	messageRate:   time.Second,
	shared:        false,
}

func main() {
	flag.StringVar(&args.pulsar, "pulsar", args.pulsar, "pulsar address. May start with pulsar:// or pulsar+ssl://")
	flag.StringVar(&args.tlsCert, "tls-cert", args.tlsCert, "(optional) path to TLS certificate")
	flag.StringVar(&args.tlsKey, "tls-key", args.tlsKey, "(optional) path to TLS key")
	flag.StringVar(&args.tlsCA, "tls-ca", args.tlsKey, "(optional) path to root certificate")
	flag.BoolVar(&args.tlsSkipVerify, "tls-insecure", args.tlsSkipVerify, "if true, do not verify server certificate chain when using TLS")
	flag.StringVar(&args.name, "name", args.name, "producer/consumer name")
	flag.StringVar(&args.topic, "topic", args.topic, "producer/consumer topic")
	flag.BoolVar(&args.producer, "producer", args.producer, "if true, produce messages, otherwise consume")
	flag.StringVar(&args.message, "message", args.message, "If equal to '--', then STDIN will be used. Otherwise value with %03d $messageNumber tacked on the front will be sent")
	flag.DurationVar(&args.messageRate, "rate", args.messageRate, "rate at which to send messages")
	flag.BoolVar(&args.shared, "shared", args.shared, "if true, consumer is shared, otherwise exclusive")
	flag.Parse()

	asyncErrs := make(chan error, 8)
	go func() {
		for err := range asyncErrs {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	var authentication auth.Authentication
	tlsCfg := &tls.Config{
		InsecureSkipVerify: args.tlsSkipVerify,
	}

	if args.tlsCert != "" && args.tlsKey != "" {
		authentication = auth.NewAuthenticationTLS(args.tlsCert, args.tlsKey)

		// Inspect certificate and print the CommonName attribute,
		// since this may be used for authorization
		if certs := authentication.GetAuthData().GetTlsCertificates(); len(certs) > 0 && len(certs[0].Certificate) > 0 && len(certs[0].Certificate[0]) > 0 {
			x509Cert, err := x509.ParseCertificate(certs[0].Certificate[0])
			if err != nil {
				fmt.Fprintln(os.Stderr, "error loading public certificate:", err)
				os.Exit(1)
			}
			fmt.Printf("Using certificate pair with CommonName = %q\n", x509Cert.Subject.CommonName)
		}
	}

	if args.tlsCA != "" {
		rootCA, err := ioutil.ReadFile(args.tlsCA)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error loading certificate authority:", err)
			os.Exit(1)
		}
		tlsCfg.RootCAs = x509.NewCertPool()
		tlsCfg.RootCAs.AppendCertsFromPEM(rootCA)
	}

	mcp := manage.NewClientPool()

	switch args.producer {
	case true:
		// Create the managed producer
		mpCfg := manage.ManagedProducerConfig{
			Name:                  args.name,
			Topic:                 args.topic,
			NewProducerTimeout:    time.Second,
			InitialReconnectDelay: time.Second,
			MaxReconnectDelay:     time.Minute,
			ManagedClientConfig: manage.ManagedClientConfig{
				ClientConfig: manage.ClientConfig{
					Addr:           args.pulsar,
					UseTLS:         args.tlsCert != "" && args.tlsKey != "",
					TLSConfig:      tlsCfg,
					Authentication: authentication,
					Errs:           asyncErrs,
				},
			},
		}
		mp := manage.NewManagedProducer(mcp, mpCfg)
		fmt.Printf("Created producer on topic %q...\n", args.topic)

		// messages to produce are sent to this
		// channel
		messages := make(chan []byte)

		switch args.message {

		// read messages from STDIN
		case "--":
			go func() {
				scanner := bufio.NewScanner(os.Stdin)
				for scanner.Scan() {
					line := scanner.Bytes()
					cp := make([]byte, len(line))
					copy(cp, line)

					messages <- cp
				}
				close(messages)
			}()

		default:
			go func() {
				var i int
				for range time.NewTicker(args.messageRate).C {
					i++
					messages <- []byte(fmt.Sprintf("%03d %s", i, args.message))
				}
			}()
		}

		for {
			select {
			case payload, ok := <-messages:
				if !ok {
					return
				}
				sctx, cancel := context.WithTimeout(ctx, time.Second)
				_, err := mp.Send(sctx, payload, "")
				cancel()
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					continue
				}

			case <-ctx.Done():
				return
			}
		}

	case false:
		queue := make(chan msg.Message, 8)

		// Create managed consumer
		mcCfg := manage.ManagedConsumerConfig{
			Name:                  args.name,
			Topic:                 args.topic,
			NewConsumerTimeout:    time.Second,
			InitialReconnectDelay: time.Second,
			MaxReconnectDelay:     time.Minute,
			ManagedClientConfig: manage.ManagedClientConfig{
				ClientConfig: manage.ClientConfig{
					Addr:      args.pulsar,
					TLSConfig: tlsCfg,
					Errs:      asyncErrs,
				},
			},
		}

		mc := manage.NewManagedConsumer(mcp, mcCfg)
		go mc.ReceiveAsync(ctx, queue)
		fmt.Printf("Created consumer %q on topic %q...\n", args.name, args.topic)

		for {
			select {
			case <-ctx.Done():
				return

			case msg := <-queue:
				fmt.Println(string(msg.Payload))
				if err := mc.Ack(ctx, msg); err != nil {
					fmt.Fprintf(os.Stderr, "error acking message: %v", err)
				}
			}
		}
	}
}
