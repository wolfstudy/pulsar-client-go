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

package msg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/wolfstudy/pulsar-client-go/pkg/api"
)

// Message represents a received MESSAGE from the Pulsar server.
type Message struct {
	Topic      string
	ConsumerID uint64

	Msg  *api.CommandMessage
	Meta *api.MessageMetadata
	// Payload for the message
	Payload []byte
	// Sets the key of the message for routing policy
	Key string
	// Attach application defined properties on the message
	Properties map[string]string
	// Set the event time for a given message
	EventTime *time.Time
}

// Equal returns true if the provided other Message
// is equal to the receiver Message.
func (m *Message) Equal(other *Message) bool {
	return m.ConsumerID == other.ConsumerID &&
		proto.Equal(m.Msg, other.Msg) &&
		proto.Equal(m.Meta, other.Meta) &&
		bytes.Equal(m.Payload, other.Payload)
}

// SingleMessage represents one of the elements of the batch type payload
type SingleMessage struct {
	SingleMetaSize uint32
	SingleMeta     *api.SingleMessageMetadata
	SinglePayload  []byte
}

// DecodeBatchMessage decode message if num_messages_in_batch exist and bigger than 0
func DecodeBatchMessage(msg *Message) ([]*SingleMessage, error) {
	num := msg.Meta.GetNumMessagesInBatch()
	if num == 0 {
		return nil, errors.New("num_message_in_batch is nil or 0")
	}
	return DecodeBatchPayload(msg.Payload, num)
}

// DecodeBatchPayload parses the payload of the batch type
// If the producer uses the batch function, msg.Payload will be a SingleMessage array structure.
func DecodeBatchPayload(bp []byte, batchNum int32) ([]*SingleMessage, error) {
	buf32 := make([]byte, 4)
	rdBuf := bytes.NewReader(bp)
	list := make([]*SingleMessage, 0, batchNum)
	for i := int32(0); i < batchNum; i++ {
		// singleMetaSize
		if _, err := io.ReadFull(rdBuf, buf32); err != nil {
			return nil, err
		}
		singleMetaSize := binary.BigEndian.Uint32(buf32)
		// singleMeta
		singleMetaBuf := make([]byte, singleMetaSize)
		if _, err := io.ReadFull(rdBuf, singleMetaBuf); err != nil {
			return nil, err
		}
		singleMeta := new(api.SingleMessageMetadata)
		if err := proto.Unmarshal(singleMetaBuf, singleMeta); err != nil {
			return nil, err
		}
		// payload
		singlePayload := make([]byte, singleMeta.GetPayloadSize())
		if _, err := io.ReadFull(rdBuf, singlePayload); err != nil {
			return nil, err
		}
		d := &SingleMessage{}
		d.SingleMetaSize = singleMetaSize
		d.SingleMeta = singleMeta
		d.SinglePayload = singlePayload
		list = append(list, d)
	}
	return list, nil
}
