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

package helix

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/go-helix/model"
)

var (
	_accessorTestKeyBuilder = &KeyBuilder{TestClusterName}
)

type DataAccessorTestSuite struct {
	BaseHelixTestSuite
}

func TestDataAccessorTestSuite(t *testing.T) {
	suite.Run(t, &DataAccessorTestSuite{})
}

func (s *DataAccessorTestSuite) TestMsg() {
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, _accessorTestKeyBuilder)
	path := fmt.Sprintf("/test_path/msg/%s", CreateRandomString())
	err := accessor.createMsg(path, model.NewMsg("test_id"))
	s.NoError(err)
	msg, err := accessor.Msg(path)
	s.NoError(err)
	msg.SetMsgState(model.MessageStateRead)
	err = accessor.setMsg(path, msg)
	s.NoError(err)
	s.Equal(model.MessageStateRead, msg.GetMsgState())
}

func (s *DataAccessorTestSuite) TestCurrentState() {
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, _accessorTestKeyBuilder)
	session := CreateRandomString()
	resource := CreateRandomString()
	path := _accessorTestKeyBuilder.currentStateForResource(testInstanceName, session, resource)
	msg := model.NewMsg(CreateRandomString())
	msg.SetStateModelDef(StateModelNameOnlineOffline)
	state := model.NewCurrentStateFromMsg(msg, resource, session)
	state.SetMapField("partition_1", model.FieldKeyCurrentState, StateModelStateOnline)
	state.SetMapField("partition_2", model.FieldKeyCurrentState, StateModelStateOffline)
	err := accessor.createCurrentState(path, state)
	s.NoError(err)
	state, err = accessor.CurrentState(testInstanceName, session, resource)
	s.NoError(err)
	s.Equal(session, state.GetSessionID())
	partitionStateMap := state.GetPartitionStateMap()
	s.Len(partitionStateMap, 2)
	s.assertCurrentState(state, "partition_1", StateModelStateOnline)
	s.assertCurrentState(state, "partition_2", StateModelStateOffline)
	s.Equal(state.GetStateModelDef(), StateModelNameOnlineOffline)
}

func (s *DataAccessorTestSuite) TestUpdateExistingCurrentState() {
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, _accessorTestKeyBuilder)
	session := CreateRandomString()
	resource := CreateRandomString()
	path := _accessorTestKeyBuilder.currentStateForResource(testInstanceName, session, resource)
	msg := model.NewMsg(CreateRandomString())
	msg.SetStateModelDef(StateModelNameOnlineOffline)
	state := model.NewCurrentStateFromMsg(msg, resource, session)
	state.SetMapField("partition_1", model.FieldKeyCurrentState, StateModelStateOnline)
	state.SetMapField("partition_2", model.FieldKeyCurrentState, StateModelStateOffline)
	err := accessor.createCurrentState(path, state)
	s.NoError(err)

	err = accessor.updateData(path, func(data *model.ZNRecord) (*model.ZNRecord, error) {
		currentState := &model.CurrentState{ZNRecord: *data}
		currentState.SetState("partition_1", StateModelStateOffline)
		currentState.SetState("partition_2", StateModelStateOnline)
		return &currentState.ZNRecord, nil
	})
	s.NoError(err)

	state, err = accessor.CurrentState(testInstanceName, session, resource)
	s.NoError(err)
	s.assertCurrentState(state, "partition_1", StateModelStateOffline)
	s.assertCurrentState(state, "partition_2", StateModelStateOnline)
	s.Equal(state.GetStateModelDef(), StateModelNameOnlineOffline)
}

func (s *DataAccessorTestSuite) TestUpdateNonExistingCurrentState() {
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, _accessorTestKeyBuilder)
	session := CreateRandomString()
	resource := CreateRandomString()
	path := _accessorTestKeyBuilder.currentStateForResource(testInstanceName, session, resource)

	err := accessor.updateData(path, func(data *model.ZNRecord) (*model.ZNRecord, error) {
		if data != nil {
			s.Fail("data is expected to be nil")
		}
		currentState := &model.CurrentState{ZNRecord: *model.NewRecord(resource)}
		currentState.SetState("partition_1", StateModelStateOffline)
		currentState.SetState("partition_2", StateModelStateOnline)
		return &currentState.ZNRecord, nil
	})
	s.NoError(err)

	state, err := accessor.CurrentState(testInstanceName, session, resource)
	s.NoError(err)
	s.assertCurrentState(state, "partition_1", StateModelStateOffline)
	s.assertCurrentState(state, "partition_2", StateModelStateOnline)
}

func (s *DataAccessorTestSuite) TestInstanceConfig() {
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, _accessorTestKeyBuilder)
	path := fmt.Sprintf("/test_path/instance_config/%s", CreateRandomString())
	config := model.NewInstanceConfig(testInstanceName)
	err := accessor.createInstanceConfig(path, config)
	s.NoError(err)
	config, err = accessor.InstanceConfig(path)
	s.False(config.GetEnabled())
}

func (s *DataAccessorTestSuite) TestGetStateModelDef() {
	keyBuilder := &KeyBuilder{TestClusterName}
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, keyBuilder)

	onlineOfflineModel, err := accessor.StateModelDef(StateModelNameOnlineOffline)
	s.NoError(err)
	s.Equal(onlineOfflineModel.GetInitialState(), StateModelStateOffline)

	masterSlaveMode, err := accessor.StateModelDef("MasterSlave")
	s.NoError(err)
	s.Equal(masterSlaveMode.GetInitialState(), StateModelStateOffline)

	leaderStandbyModel, err := accessor.StateModelDef("LeaderStandby")
	s.NoError(err)
	s.Equal(leaderStandbyModel.GetInitialState(), StateModelStateOffline)
}

func (s *DataAccessorTestSuite) TestIdealState() {
	cluster, resource := CreateRandomString(), CreateRandomString()
	admin, err := NewAdmin(s.ZkConnectString)
	s.NoError(err)
	addedCluster := admin.AddCluster(cluster, false)
	s.True(addedCluster)
	numPartitions := rand.Int()
	err = admin.AddResource(cluster, resource, numPartitions, StateModelNameOnlineOffline)
	s.NoError(err)

	keyBuilder := &KeyBuilder{cluster}
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, keyBuilder)
	idealState, err := accessor.IdealState(resource)
	s.NoError(err)
	s.Equal(numPartitions, idealState.GetNumPartitions())
}

func (s *DataAccessorTestSuite) TestLiveInstance() {
	p, _ := s.createParticipantAndConnect()
	s.NotNil(p)

	keyBuilder := &KeyBuilder{TestClusterName}
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, keyBuilder)

	liveInstance, err := accessor.LiveInstance(p.InstanceName())
	s.NoError(err)
	s.Equal(liveInstance.GetSessionID(), p.zkClient.GetSessionID())
}

func (s *DataAccessorTestSuite) assertCurrentState(
	state *model.CurrentState,
	partition string,
	expectedState string,
) {
	s.Equal(state.GetPartitionStateMap()[partition], expectedState)
	s.Equal(state.GetState(partition), expectedState)
}
