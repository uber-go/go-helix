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
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/uber-go/go-helix/model"
	uzk "github.com/uber-go/go-helix/zk"
)

var (
	// ErrorNilUpdatedData returns when updateFn returns nil *model.ZNRecord without error
	ErrorNilUpdatedData = errors.New("data accessor: updated data is nil")
)

// updateFn gets the old data (nil if data does not exist) and return the updated data
// return data is expected to be non-nil
type updateFn func(data *model.ZNRecord) (*model.ZNRecord, error)

// DataAccessor helps to interact with Helix Data Types like IdealState, LiveInstance, Message
// it mirrors org.apache.helix.HelixDataAccessor
type DataAccessor struct {
	zkClient   *uzk.Client
	keyBuilder *KeyBuilder
}

// newDataAccessor creates new DataAccessor with Zookeeper client
func newDataAccessor(zkClient *uzk.Client, keyBuilder *KeyBuilder) *DataAccessor {
	return &DataAccessor{zkClient: zkClient, keyBuilder: keyBuilder}
}

// Msg helps get Helix property with type Message
func (a *DataAccessor) Msg(path string) (*model.Message, error) {
	record, err := a.zkClient.GetRecordFromPath(path)
	if err != nil {
		return nil, err
	}
	return &model.Message{ZNRecord: *record}, nil
}

// InstanceConfig helps get Helix property with type Message
func (a *DataAccessor) InstanceConfig(path string) (*model.InstanceConfig, error) {
	record, err := a.zkClient.GetRecordFromPath(path)
	if err != nil {
		return nil, err
	}
	return &model.InstanceConfig{ZNRecord: *record}, nil
}

// IdealState helps get Helix property with type IdealState
func (a *DataAccessor) IdealState(resourceName string) (*model.IdealState, error) {
	path := a.keyBuilder.idealStateForResource(resourceName)
	record, err := a.zkClient.GetRecordFromPath(path)
	if err != nil {
		return nil, err
	}
	return &model.IdealState{ZNRecord: *record}, nil
}

// CurrentState helps get Helix property with type CurrentState
func (a *DataAccessor) CurrentState(instanceName, session, resourceName string,
) (*model.CurrentState, error) {
	path := a.keyBuilder.currentStateForResource(instanceName, session, resourceName)
	record, err := a.zkClient.GetRecordFromPath(path)
	if err != nil {
		return nil, err
	}
	return &model.CurrentState{ZNRecord: *record}, nil
}

// LiveInstance returns Helix property with type LiveInstance
func (a *DataAccessor) LiveInstance(instanceName string) (*model.LiveInstance, error) {
	path := a.keyBuilder.liveInstance(instanceName)
	record, err := a.zkClient.GetRecordFromPath(path)
	if err != nil {
		return nil, err
	}
	return &model.LiveInstance{ZNRecord: *record}, nil
}

// StateModelDef helps get Helix property with type StateModelDef
func (a *DataAccessor) StateModelDef(stateModel string) (*model.StateModelDef, error) {
	path := a.keyBuilder.stateModelDef(stateModel)
	record, err := a.zkClient.GetRecordFromPath(path)
	if err != nil {
		return nil, err
	}
	return &model.StateModelDef{ZNRecord: *record}, nil
}

// updateData would update the data in path with updateFn
// if path does not exist, updateData would create it
// and updateFn would have a nil *model.ZNRecord as input
func (a *DataAccessor) updateData(path string, update updateFn) error {
	var err error
	var record *model.ZNRecord
	for {
		nodeExists := true
		record, err = a.zkClient.GetRecordFromPath(path)
		cause := errors.Cause(err)
		if cause == zk.ErrNoNode {
			nodeExists = false
		} else if cause != nil {
			return err
		}

		record, err = update(record)
		if err != nil {
			return err
		}
		if record == nil {
			return ErrorNilUpdatedData
		}
		if nodeExists {
			err = a.setData(path, *record, record.Version)
		} else {
			err = a.createData(path, *record)
		}

		if cause := errors.Cause(err); cause != nil &&
			(cause != zk.ErrBadVersion || cause != zk.ErrNoNode) {
			return err
		}

		// retry for ErrBadVersion or ErrNoNode
		if err == nil {
			return nil
		}
	}
}

func (a *DataAccessor) createData(path string, data model.ZNRecord) error {
	serialized, err := data.Marshal()
	if err != nil {
		return err
	}
	return a.zkClient.CreateDataWithPath(path, serialized)
}

func (a *DataAccessor) setData(path string, data model.ZNRecord, version int32) error {
	serialized, err := data.Marshal()
	if err != nil {
		return err
	}
	return a.zkClient.SetDataForPath(path, serialized, version)
}

// createMsg creates a new message
func (a *DataAccessor) createMsg(path string, msg *model.Message) error {
	return a.createData(path, msg.ZNRecord)
}

// CreateParticipantMsg creates a message for the participant and helps
// testing the participant
func (a *DataAccessor) CreateParticipantMsg(instanceName string, msg *model.Message) error {
	path := a.keyBuilder.participantMsg(instanceName, msg.ID)
	return a.createData(path, msg.ZNRecord)
}

func (a *DataAccessor) setMsg(path string, msg *model.Message) error {
	return a.setData(path, msg.ZNRecord, msg.Version)
}

func (a *DataAccessor) createCurrentState(path string, state *model.CurrentState) error {
	return a.createData(path, state.ZNRecord)
}

func (a *DataAccessor) createInstanceConfig(path string, config *model.InstanceConfig) error {
	return a.createData(path, config.ZNRecord)
}
