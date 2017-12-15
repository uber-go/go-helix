// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package model

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

// Message helps communication between nodes
// Mirrors org.apache.helix.model.Message
// Sample message:
// {
//     "id": "9ff57fc1-9f2a-41a5-af46-c4ae2a54c539",
//     "simpleFields": {
//         "CREATE_TIMESTAMP": "1425268051457",
//         "ClusterEventName": "currentStateChange",
//         "FROM_STATE": "OFFLINE",
//         "MSG_ID": "9ff57fc1-9f2a-41a5-af46-c4ae2a54c539",
//         "MSG_STATE": "new",
//         "MSG_TYPE": "STATE_TRANSITION",
//         "PARTITION_NAME": "myDB_5",
//         "RESOURCE_NAME": "myDB",
//         "SRC_NAME": "precise64-CONTROLLER",
//         "SRC_SESSION_ID": "14bd852c528004c",
//         "STATE_MODEL_DEF": "MasterSlave",
//         "STATE_MODEL_FACTORY_NAME": "DEFAULT",
//         "TGT_NAME": "localhost_12913",
//         "TGT_SESSION_ID": "93406067297878252",
//         "TO_STATE": "SLAVE"
//     },
//     "listFields": {},
//     "mapFields": {}
// }
type Message struct {
	ZNRecord
}

// MessageState is the state of the Helix message
// mirrors org.apache.helix.model.Message#MessageState
type MessageState int

// MessageState is the state of the message
const (
	MessageStateNew MessageState = iota
	MessageStateRead
	MessageStateUnprocessable
)

// String returns string representation of message state
func (state MessageState) String() string {
	switch state {
	case MessageStateNew:
		return "new"
	case MessageStateRead:
		return "read"
	case MessageStateUnprocessable:
		return "unprocessable"
	default:
		return "unprocessable"
	}
}

// NewMsg creates a new instance of Message instance
func NewMsg(id string) *Message {
	return &Message{ZNRecord: *NewRecord(id)}
}

// SetExecuteStartTime sets execute start time
func (m *Message) SetExecuteStartTime(t time.Time) {
	tMs := t.UnixNano() / int64(time.Millisecond)
	m.SetSimpleField(FieldKeyExecuteStartTimestamp, fmt.Sprintf("%d", tMs))
}

// GetStateModelDef returns state model definition
func (m Message) GetStateModelDef() string {
	return m.GetStringField(FieldKeyStateModelDef, "")
}

// SetStateModelDef returns state model definition
func (m *Message) SetStateModelDef(stateModelDef string) {
	m.SetSimpleField(FieldKeyStateModelDef, stateModelDef)
}

// GetMsgState returns the message state
func (m Message) GetMsgState() MessageState {
	state := m.GetStringField(FieldKeyMsgState, "")
	switch state {
	case "new":
		return MessageStateNew
	case "read":
		return MessageStateRead
	case "unprocessable":
		return MessageStateUnprocessable
	default:
		return MessageStateUnprocessable
	}
}

// SetMsgState sets the message state
func (m *Message) SetMsgState(s MessageState) {
	m.SetSimpleField(FieldKeyMsgState, s.String())
}

// GetPartitionName returns the partition name
func (m Message) GetPartitionName() (string, error) {
	partition := m.GetStringField(FieldKeyPartitionName, "")
	if partition == "" {
		return "", errors.Errorf("missing partition name in message: %v", m)
	}
	return partition, nil
}

// SetPartitionName sets the partition name of the message
func (m *Message) SetPartitionName(partitionName string) {
	m.SetSimpleField(FieldKeyPartitionName, partitionName)
}

// GetTargetSessionID returns target session ID and the message should only be consumed by the
// Helix node of the matching session
func (m Message) GetTargetSessionID() string {
	return m.GetStringField(FieldKeyTargetSessionID, "")
}

// GetTargetName returns the target of the message, such as "PARTICIPANT"
func (m Message) GetTargetName() string {
	return m.GetStringField(FieldKeyTargetName, "")
}

// GetMsgType returns the message type, such as "STATE_TRANSITION"
func (m Message) GetMsgType() string {
	return m.GetStringField(FieldKeyMsgType, "")
}

// GetResourceName returns the resource name
func (m Message) GetResourceName() string {
	return m.GetStringField(FieldKeyResourceName, "")
}

// GetToState returns the toState
func (m Message) GetToState() string {
	return m.GetStringField(FieldKeyToState, "")
}

// GetFromState returns the fromState
func (m Message) GetFromState() string {
	return m.GetStringField(FieldKeyFromState, "")
}

// GetParentMsgID returns the parent message ID
func (m Message) GetParentMsgID() string {
	return m.GetStringField(FieldKeyParentMsgID, "")
}

// GetCreateTimestamp returns the message creation timestamp
func (m Message) GetCreateTimestamp() int64 {
	return m.GetInt64Field(FieldKeyCreateTimestamp, 0)
}

// GetBucketSize return the bucket size of the message
func (m Message) GetBucketSize() int {
	return m.GetIntField(FieldKeyBucketSize, 0)
}

// GetBatchMsgMode returns if the batch message mode should be used
func (m *Message) GetBatchMsgMode() bool {
	return m.GetBooleanField(FieldKeyBatchMsgMode, false)
}

// GetStateModelFactoryName returns the state model factory name
func (m *Message) GetStateModelFactoryName() string {
	return m.GetStringField(FieldKeyStateModelFactoryName, _defaultStateModelFactoryName)
}
