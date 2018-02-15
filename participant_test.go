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
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/go-helix/model"
	"github.com/uber-go/go-helix/util"
	uzk "github.com/uber-go/go-helix/zk"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type ParticipantTestSuite struct {
	BaseHelixTestSuite
}

func TestParticipantTestSuite(t *testing.T) {
	s := &ParticipantTestSuite{}
	s.EmbeddedZkPath = "zk/embedded" // TODO (zhijin): remove when moving to github
	suite.Run(t, s)
}

func (s *ParticipantTestSuite) TestConnectAndDisconnect() {
	p1, _ := s.createParticipantAndConnect()
	err := p1.Connect()
	s.NoError(err)
	p1.Disconnect()
	s.Admin.DropInstance(TestClusterName, p1.instanceName)
	s.False(p1.IsConnected())
	// disconnect twice doesn't cause panics
	p1.Disconnect()

	p2, _ := s.createParticipantAndConnect()
	err = p2.createLiveInstance()
	s.Error(err, zk.ErrNodeExists)
	p2.Disconnect()
	s.Admin.DropInstance(TestClusterName, p2.instanceName)
}

func (s *ParticipantTestSuite) TestParticipantNameCollision() {
	port := GetRandomPort()
	p1, _ := NewParticipant(zap.NewNop(), tally.NoopScope,
		s.ZkConnectString, testApplication, TestClusterName, TestResource, testParticipantHost, port)
	s.NotNil(p1)
	p1.RegisterStateModel(StateModelNameOnlineOffline, createNoopStateModelProcessor())
	err := p1.Connect()
	s.NoError(err)
	p2, _ := NewParticipant(zap.NewNop(), tally.NoopScope,
		s.ZkConnectString, testApplication, TestClusterName, TestResource, testParticipantHost, port)
	s.NotNil(p2)
	p2.RegisterStateModel(StateModelNameOnlineOffline, createNoopStateModelProcessor())
	err = p2.Connect()
	s.Error(err)
}

func (s *ParticipantTestSuite) TestIsClusterSetup() {
	p, _ := s.createParticipantAndConnect()
	defer p.Disconnect()

	setup, err := p.isClusterSetup()
	s.NoError(err)
	s.True(setup)
}

func (s *ParticipantTestSuite) TestHandleInvalidMessages() {
	p, _ := s.createParticipantAndConnect()
	defer p.Disconnect()

	msgEmpty := &model.Message{}
	err := p.handleMsg(msgEmpty)
	s.Equal(errMsgMissingStateModelDef, err)

	msgWithoutStateModelDef := s.createMsg(p, removeMsgFieldsOp(model.FieldKeyStateModelDef))
	err = p.handleMsg(msgWithoutStateModelDef)
	s.Equal(errMsgMissingStateModelDef, err)

	msgWithoutFromState := s.createMsg(p, removeMsgFieldsOp(model.FieldKeyFromState))
	err = p.handleMsg(msgWithoutFromState)
	s.Equal(errMsgMissingFromOrToState, err)

	msgWithoutToState := s.createMsg(p, removeMsgFieldsOp(model.FieldKeyToState))
	err = p.handleMsg(msgWithoutToState)
	s.Equal(errMsgMissingFromOrToState, err)
}

func (s *ParticipantTestSuite) TestHandleValidMessages() {
	p, _ := s.createParticipantAndConnect()
	defer p.Disconnect()

	validMsgs := []*model.Message{
		s.createMsg(p),
		s.createMsg(p, removeMsgFieldsOp(model.FieldKeyParentMsgID)),
		s.createMsg(p, setMsgFieldsOp(model.FieldKeyTargetSessionID, "wrong id")),
		s.createMsg(p, setMsgFieldsOp(model.FieldKeyToState, StateModelStateDropped)),
	}

	for _, msg := range validMsgs {
		err := p.handleMsg(msg)
		s.NoError(err)
	}
}

func (s *ParticipantTestSuite) TestGetCurrentResourceNames() {
	p, _ := s.createParticipantAndConnect()
	defer p.Disconnect()

	resources := util.NewStringSet(p.getCurrentResourceNames()...)
	s.Equal(0, resources.Size())

	keyBuilder := &KeyBuilder{TestClusterName}
	accessor := p.DataAccessor()

	resource := CreateRandomString()
	msg := model.NewMsg("test_id")
	session := p.zkClient.GetSessionID()
	path := keyBuilder.currentStateForResource(p.instanceName, session, resource)
	state := model.NewCurrentStateFromMsg(msg, resource, session)
	err := accessor.createCurrentState(path, state)
	s.NoError(err)

	resources = util.NewStringSet(p.getCurrentResourceNames()...)
	s.Equal(1, resources.Size(), "participant should find one resource")
	s.True(resources.Contains(resource), "participant should find the added resource")
}

func (s *ParticipantTestSuite) TestCarryOverPreviousCurrentStateWhenCurrentStateNotExist() {
	p, _ := s.createParticipantAndConnect()
	defer p.Disconnect()

	keyBuilder := &KeyBuilder{TestClusterName}
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, keyBuilder)
	resource := CreateRandomString()
	preSession := CreateRandomString()
	currentSession := p.zkClient.GetSessionID()

	// Current state is not created
	_, err := accessor.CurrentState(p.instanceName, currentSession, resource)
	s.EqualError(errors.Cause(err), zk.ErrNoNode.Error())

	path := keyBuilder.currentStateForResource(p.instanceName, preSession, resource)
	preState := &model.CurrentState{ZNRecord: *model.NewRecord(resource)}
	preState.SetStateModelDef(StateModelNameOnlineOffline)
	preState.SetState("partition_1", StateModelStateOffline)
	preState.SetState("partition_2", StateModelStateOnline)
	preState.SetState("partition_3", StateModelStateOnline)
	err = accessor.createCurrentState(path, preState)
	s.NoError(err)

	p.carryOverPreviousCurrentState()
	currentState, err := accessor.CurrentState(p.instanceName, currentSession, resource)
	s.NoError(err)
	// carry over only when current state not exist
	// if not exist, set to initial state
	s.Equal(currentState.GetState("partition_1"), StateModelStateOffline)
	s.Equal(currentState.GetState("partition_2"), StateModelStateOffline)
	s.Equal(currentState.GetState("partition_3"), StateModelStateOffline)
	s.Len(currentState.GetPartitionStateMap(), 3)
}

func (s *ParticipantTestSuite) TestCarryOverPreviousCurrentStateWhenCurrentStateExists() {
	p, _ := s.createParticipantAndConnect()
	defer p.Disconnect()

	keyBuilder := &KeyBuilder{TestClusterName}
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, keyBuilder)
	resource := CreateRandomString()
	preSession := CreateRandomString()
	currentSession := p.zkClient.GetSessionID()

	// Current state is not created
	_, err := accessor.CurrentState(p.instanceName, currentSession, resource)
	s.EqualError(errors.Cause(err), zk.ErrNoNode.Error())

	path := keyBuilder.currentStateForResource(p.instanceName, preSession, resource)
	preState := &model.CurrentState{ZNRecord: *model.NewRecord(resource)}
	preState.SetStateModelDef(StateModelNameOnlineOffline)
	preState.SetState("partition_1", StateModelStateOffline)
	preState.SetState("partition_2", StateModelStateOnline)
	preState.SetState("partition_3", StateModelStateOnline)
	err = accessor.createCurrentState(path, preState)
	s.NoError(err)

	path = keyBuilder.currentStateForResource(p.instanceName, currentSession, resource)
	currentState := &model.CurrentState{ZNRecord: *model.NewRecord(resource)}
	currentState.SetStateModelDef(StateModelNameOnlineOffline)
	currentState.SetState("partition_2", StateModelStateOnline)
	err = accessor.createCurrentState(path, currentState)
	s.NoError(err)

	p.carryOverPreviousCurrentState()
	currentState, err = accessor.CurrentState(p.instanceName, currentSession, resource)
	s.NoError(err)
	// carry over only when current state not exist
	// if not exist, set to initial state
	s.Equal(currentState.GetState("partition_1"), StateModelStateOffline)
	s.Equal(currentState.GetState("partition_2"), StateModelStateOnline)
	s.Equal(currentState.GetState("partition_3"), StateModelStateOffline)
	s.Len(currentState.GetPartitionStateMap(), 3)
}

func (s *ParticipantTestSuite) TestCarryOverPreviousCurrentStateForMultipleResources() {
	p, _ := s.createParticipantAndConnect()
	defer p.Disconnect()

	numberOfResources := 3
	var resources []string
	keyBuilder := &KeyBuilder{TestClusterName}
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, keyBuilder)
	preSession := CreateRandomString()
	currentSession := p.zkClient.GetSessionID()

	for i := 0; i < numberOfResources; i++ {
		resource := CreateRandomString()
		resources = append(resources, resource)
		path := keyBuilder.currentStateForResource(p.instanceName, preSession, resource)
		preState := &model.CurrentState{ZNRecord: *model.NewRecord(resource)}
		preState.SetStateModelDef(StateModelNameOnlineOffline)
		preState.SetState("partition_1", StateModelStateOffline)
		preState.SetState("partition_2", StateModelStateOnline)
		preState.SetState("partition_3", StateModelStateOnline)
		err := accessor.createCurrentState(path, preState)
		s.NoError(err)
	}

	p.carryOverPreviousCurrentState()

	for _, resource := range resources {
		currentState, err := accessor.CurrentState(p.instanceName, currentSession, resource)
		s.NoError(err)
		// carry over only when current state not exist
		// if not exist, set to initial state
		s.Equal(currentState.GetState("partition_1"), StateModelStateOffline)
		s.Equal(currentState.GetState("partition_2"), StateModelStateOffline)
		s.Equal(currentState.GetState("partition_3"), StateModelStateOffline)
		s.Len(currentState.GetPartitionStateMap(), 3)
	}
}

func (s *ParticipantTestSuite) TestProcessMessages() {
	port := GetRandomPort()
	processor := NewStateModelProcessor()
	mu := &sync.Mutex{}
	counters := map[string]map[string]int{
		StateModelStateOffline: {
			StateModelStateOnline:  0,
			StateModelStateDropped: 0,
		},
		StateModelStateOnline: {
			StateModelStateOffline: 0,
		},
	}

	processor.AddTransition(
		StateModelStateOnline, StateModelStateOffline, func(m *model.Message) error {
			mu.Lock()
			defer mu.Unlock()
			counters[StateModelStateOnline][StateModelStateOffline]++
			log.Printf("partition ONLINE=>OFFLINE: %v", m)
			return nil
		})
	processor.AddTransition(
		StateModelStateOffline, StateModelStateOnline, func(m *model.Message) error {
			mu.Lock()
			defer mu.Unlock()
			counters[StateModelStateOffline][StateModelStateOnline]++
			log.Printf("partition OFFLINE=>ONLINE: %v", m)
			return nil
		})
	processor.AddTransition(
		StateModelStateOffline, StateModelStateDropped, func(m *model.Message) error {
			mu.Lock()
			defer mu.Unlock()
			counters[StateModelStateOffline][StateModelStateDropped]++
			return nil
		})
	p, _ := NewParticipant(zap.NewNop(), tally.NoopScope,
		s.ZkConnectString, testApplication, TestClusterName, TestResource, testParticipantHost, port)
	pImpl := p.(*participant)
	s.NotNil(pImpl)
	pImpl.RegisterStateModel(StateModelNameOnlineOffline, processor)
	err := pImpl.Connect()
	s.NoError(err)
	mu.Lock()
	s.Equal(0, counters[StateModelStateOnline][StateModelStateOffline])
	s.Equal(0, counters[StateModelStateOffline][StateModelStateOnline])
	s.Equal(0, counters[StateModelStateOffline][StateModelStateDropped])
	mu.Unlock()

	keyBuilder := &KeyBuilder{TestClusterName}
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, keyBuilder)

	msg := s.createMsg(pImpl,
		setMsgFieldsOp(model.FieldKeyFromState, StateModelStateOffline),
		setMsgFieldsOp(model.FieldKeyToState, StateModelStateOnline),
	)
	accessor.createMsg(keyBuilder.participantMsg(pImpl.InstanceName(), CreateRandomString()), msg)
	// wait for the participant to process messages
	time.Sleep(1 * time.Second)
	mu.Lock()
	s.Equal(1, counters[StateModelStateOffline][StateModelStateOnline])
	s.Equal(0, counters[StateModelStateOffline][StateModelStateDropped])
	s.Equal(0, counters[StateModelStateOnline][StateModelStateOffline])
	mu.Unlock()

	msgID := CreateRandomString()
	msg = s.createMsg(pImpl,
		setMsgFieldsOp(model.FieldKeyTargetSessionID, CreateRandomString()),
	)
	msg.ID = msgID
	accessor.createMsg(keyBuilder.participantMsg(pImpl.InstanceName(), msgID), msg)
	time.Sleep(2 * time.Second)
	path := keyBuilder.participantMsg(pImpl.InstanceName(), msgID)
	exists, _, err := client.Exists(path)
	s.NoError(err)
	s.False(exists)

	msgID = CreateRandomString()
	msg = s.createMsg(pImpl,
		setMsgFieldsOp(model.FieldKeyMsgType, MsgTypeNoop),
	)
	msg.ID = msgID
	accessor.createMsg(keyBuilder.participantMsg(pImpl.InstanceName(), msgID), msg)
	time.Sleep(2 * time.Second)
	path = keyBuilder.participantMsg(pImpl.InstanceName(), msgID)
	exists, _, err = client.Exists(path)
	s.NoError(err)
	s.False(exists)
}

func (s *ParticipantTestSuite) TestUpdateCurrentState() {
	p, _ := s.createParticipantAndConnect()
	defer p.Disconnect()

	keyBuilder := &KeyBuilder{TestClusterName}
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, keyBuilder)

	currentStatePath := keyBuilder.currentStatesForSession(p.instanceName, p.zkClient.GetSessionID())
	exists, _, err := client.Exists(currentStatePath)
	s.NoError(err)
	s.False(exists)

	resource := CreateRandomString()
	partition := strconv.Itoa(rand.Int())
	msg := s.createMsg(p,
		setMsgFieldsOp(model.FieldKeyFromState, StateModelStateOffline),
		setMsgFieldsOp(model.FieldKeyToState, StateModelStateOnline),
		setMsgFieldsOp(model.FieldKeyResourceName, resource),
		setMsgFieldsOp(model.FieldKeyMsgType, MsgTypeStateTransition),
		setMsgFieldsOp(model.FieldKeyPartitionName, partition),
	)
	accessor.CreateParticipantMsg(p.instanceName, msg)
	// wait for the participant to process messages
	time.Sleep(2 * time.Second)

	currentState, err := accessor.CurrentState(p.instanceName, p.zkClient.GetSessionID(), resource)
	s.NoError(err)
	s.Equal(int32(1), currentState.Version, "current state has been created and updated once")
	s.Equal(currentState.GetState(partition), StateModelStateOnline)
}

func (s *ParticipantTestSuite) TestMismatchStateIsRejected() {
	p, _ := s.createParticipantAndConnect()
	defer p.Disconnect()

	keyBuilder := &KeyBuilder{TestClusterName}
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	accessor := newDataAccessor(client, keyBuilder)

	currentStatePath := keyBuilder.currentStatesForSession(p.instanceName, p.zkClient.GetSessionID())
	exists, _, err := client.Exists(currentStatePath)
	s.NoError(err)
	s.False(exists)

	resource := CreateRandomString()
	partition := strconv.Itoa(rand.Int())
	msg := s.createMsg(p,
		setMsgFieldsOp(model.FieldKeyFromState, StateModelStateOffline),
		setMsgFieldsOp(model.FieldKeyToState, StateModelStateOnline),
		setMsgFieldsOp(model.FieldKeyResourceName, resource),
		setMsgFieldsOp(model.FieldKeyMsgType, MsgTypeStateTransition),
		setMsgFieldsOp(model.FieldKeyPartitionName, partition),
	)
	accessor.createMsg(keyBuilder.participantMsg(p.instanceName, CreateRandomString()), msg)
	// wait for the participant to process messages
	time.Sleep(2 * time.Second)
	currentState, err := accessor.CurrentState(p.instanceName, p.zkClient.GetSessionID(), resource)
	s.NoError(err)
	s.Equal(currentState.GetState(partition), StateModelStateOnline)

	// the expected fromState is ONLINE, so the transition should be rejected
	msg = s.createMsg(p,
		setMsgFieldsOp(model.FieldKeyFromState, StateModelStateOffline),
		setMsgFieldsOp(model.FieldKeyToState, StateModelStateDropped),
		setMsgFieldsOp(model.FieldKeyResourceName, resource),
		setMsgFieldsOp(model.FieldKeyMsgType, MsgTypeStateTransition),
		setMsgFieldsOp(model.FieldKeyPartitionName, partition),
	)
	accessor.createMsg(keyBuilder.participantMsg(p.instanceName, CreateRandomString()), msg)
	// wait for the participant to process messages
	time.Sleep(2 * time.Second)
	currentState, err = accessor.CurrentState(p.instanceName, p.zkClient.GetSessionID(), resource)
	s.NoError(err)
	// state should remain unchanged
	s.Equal(currentState.GetState(partition), StateModelStateOnline)
}

func (s *ParticipantTestSuite) TestHandleNewSessionCalledAfterZookeeperSessionExpired() {
	port := GetRandomPort()
	p, _ := NewParticipant(zap.NewNop(), tally.NoopScope,
		s.ZkConnectString, testApplication, TestClusterName, TestResource, testParticipantHost, port)
	pImpl := p.(*participant)
	defer pImpl.Disconnect()
	// set default state to StateHasSession so Participant would believe it is connected
	fakeZK := uzk.NewFakeZk(uzk.DefaultConnectionState(zk.StateHasSession))
	pImpl.zkClient = uzk.NewClient(zap.NewNop(), tally.NoopScope, uzk.WithConnFactory(fakeZK),
		uzk.WithRetryTimeout(time.Second))
	pImpl.Connect()
	s.Len(fakeZK.GetConnections(), 1)

	// Create(liveInstancePath) will be called if and only if a new session is created
	// check the times it is called would infer the number of times a new session is created
	fakeZKConnection := fakeZK.GetConnections()[0]
	liveInstancePath := pImpl.keyBuilder.liveInstance(pImpl.InstanceName())
	methodHistory := fakeZKConnection.GetHistory()
	historyForChildrenW := methodHistory.GetHistoryForMethod("Create")
	s.Len(historyForChildrenW, 1)
	s.Equal(historyForChildrenW[0].Params[0].(string), liveInstancePath)

	// simulate a session expiration and reconnection event
	fakeZK.SetState(fakeZKConnection, zk.StateExpired)
	fakeZK.SetState(fakeZKConnection, zk.StateHasSession)
	time.Sleep(1 * time.Second)
	historyForChildrenW = methodHistory.GetHistoryForMethod("Create")
	s.Len(historyForChildrenW, 2)
	s.Equal(historyForChildrenW[1].Params[0].(string), liveInstancePath)
}

func (s *ParticipantTestSuite) TestFatalErrorCh() {
	port := GetRandomPort()
	p, errCh := NewTestParticipant(zap.NewNop(), tally.NoopScope,
		s.ZkConnectString, testApplication, TestClusterName, TestResource, testParticipantHost, port)
	s.True(errCh == p.GetFatalErrorChan())
}

func (s *ParticipantTestSuite) createMsg(p *participant, ops ...msgOp) *model.Message {
	msg := s.createValidMsg(p)
	for _, op := range ops {
		op(msg)
	}
	// TODO: add msg to zk?
	return msg
}

func (s *ParticipantTestSuite) createValidMsg(p *participant) *model.Message {
	msg := model.NewMsg(CreateRandomString())
	msg.SetSimpleField(model.FieldKeyStateModelDef, StateModelNameOnlineOffline)
	msg.SetSimpleField(model.FieldKeyTargetSessionID, p.zkClient.GetSessionID())
	msg.SetSimpleField(model.FieldKeyFromState, StateModelStateOffline)
	msg.SetSimpleField(model.FieldKeyToState, StateModelStateOnline)
	msg.SetMsgState(model.MessageStateNew)
	return msg
}

type msgOp func(*model.Message)

func removeMsgFieldsOp(fields ...string) msgOp {
	return func(msg *model.Message) {
		for _, f := range fields {
			msg.RemoveSimpleField(f)
			msg.RemoveMapField(f)
		}
	}
}

func setMsgFieldsOp(key, val string) msgOp {
	return func(msg *model.Message) {
		msg.SetSimpleField(key, val)
	}
}
