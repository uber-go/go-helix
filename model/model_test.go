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
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	_testMsgJSONString = `{
    "id": "9ff57fc1-9f2a-41a5-af46-c4ae2a54c539",
    "simpleFields": {
        "CREATE_TIMESTAMP": "1425268051457",
        "ClusterEventName": "currentStateChange",
        "FROM_STATE": "OFFLINE",
        "MSG_ID": "9ff57fc1-9f2a-41a5-af46-c4ae2a54c539",
        "MSG_STATE": "new",
        "MSG_TYPE": "STATE_TRANSITION",
        "PARTITION_NAME": "myDB_5",
        "RESOURCE_NAME": "myDB",
        "SRC_NAME": "precise64-CONTROLLER",
        "SRC_SESSION_ID": "14bd852c528004c",
        "STATE_MODEL_DEF": "MasterSlave",
        "STATE_MODEL_FACTORY_NAME": "DEFAULT",
        "TGT_NAME": "localhost_12913",
        "TGT_SESSION_ID": "93406067297878252",
        "TO_STATE": "SLAVE"
    },
    "listFields": {},
    "mapFields": {}
}`
)

func TestMsg(t *testing.T) {
	msg := NewMsg("test_id")
	msg.SetMsgState(MessageStateNew)
	assert.Equal(t, MessageStateNew, msg.GetMsgState())
	assert.Equal(t, "", msg.GetParentMsgID())
	assert.Equal(t, 0, msg.GetBucketSize())
	assert.False(t, msg.GetBatchMsgMode())
	assert.Equal(t, _defaultStateModelFactoryName, msg.GetStateModelFactoryName())
	record, err := NewRecordFromBytes([]byte(_testMsgJSONString))
	assert.NoError(t, err)
	msg = &Message{ZNRecord: *record}
	assert.Equal(t, "93406067297878252", msg.GetTargetSessionID())
	partition, err := msg.GetPartitionName()
	assert.Equal(t, "myDB_5", partition)
	assert.Equal(t, "OFFLINE", msg.GetFromState())
	assert.Equal(t, "SLAVE", msg.GetToState())
	record.SetSimpleField(FieldKeyPartitionName, "")

	msg = &Message{ZNRecord: *record}
	partition, err = msg.GetPartitionName()
	assert.Equal(t, "", partition)
	assert.Error(t, err)
}

func TestInstanceConfig(t *testing.T) {
	config := NewInstanceConfig("test_instance")
	assert.False(t, config.GetEnabled())
	config.SetEnabled(true)
	assert.True(t, config.GetEnabled())
}

func TestLiveInstanceConfig(t *testing.T) {
	instanceName := "test_instance"
	instance := NewLiveInstance(instanceName, "test_session")
	record := instance.ZNRecord
	version, ok := record.GetSimpleField(FieldKeyHelixVersion)
	assert.Equal(t, _helixVersion, version)
	assert.True(t, ok)
	session, ok := record.GetSimpleField(FieldKeySessionID)
	assert.Equal(t, "test_session", session)
	assert.True(t, ok)
}

func TestCurrentState(t *testing.T) {
	msg := NewMsg("msg_id")
	sessionID := "test_session"
	state := NewCurrentStateFromMsg(msg, "test_resource", sessionID)
	assert.Equal(t, sessionID, state.GetSessionID())

	assert.Equal(t, state.GetState("partition_1"), "")
	assert.Equal(t, state.GetState("partition_2"), "")
	state.SetState("partition_1", "state1")
	state.SetState("partition_2", "state2")
	assert.Equal(t, state.GetState("partition_1"), "state1")
	assert.Equal(t, state.GetState("partition_2"), "state2")
	assert.Len(t, state.GetPartitionStateMap(), 2)
}

func TestIdealState(t *testing.T) {
	numPartitions := 10
	record, err := NewRecordFromBytes([]byte("{}"))
	assert.NoError(t, err)
	record.SetIntField(FieldKeyNumPartitions, numPartitions)
	state := &IdealState{ZNRecord: *record}
	assert.Equal(t, numPartitions, state.GetNumPartitions())
}
