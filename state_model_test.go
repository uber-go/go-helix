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
	"testing"

	"github.com/stretchr/testify/suite"
)

const (
	_testResource     string = "testResource"
	_testPartition    string = "testPartition"
	_testPartitionTwo string = "testPartitionTwo"
)

type StateModelTestSuite struct {
	suite.Suite
}

func TestStateModelTestSuite(t *testing.T) {
	suite.Run(t, &StateModelTestSuite{})
}

func (s *StateModelTestSuite) TestUpdateAndGetState() {
	stateModel := NewStateModel()
	result, exist := stateModel.GetState(_testResource, _testPartition)
	s.False(exist)

	stateModel.UpdateState(_testResource, _testPartition, StateModelStateOffline)
	result, exist = stateModel.GetState(_testResource, _testPartition)
	s.True(exist)
	s.Equal(result, StateModelStateOffline)

	stateModel.UpdateState(_testResource, _testPartition, StateModelStateOnline)
	result, exist = stateModel.GetState(_testResource, _testPartition)
	s.True(exist)
	s.Equal(result, StateModelStateOnline)
}

func (s *StateModelTestSuite) TestRemoveState() {
	stateModel := NewStateModel()
	result, exist := stateModel.GetState(_testResource, _testPartition)
	s.False(exist)

	stateModel.UpdateState(_testResource, _testPartition, StateModelStateOffline)
	result, exist = stateModel.GetState(_testResource, _testPartition)
	s.True(exist)
	s.Equal(result, StateModelStateOffline)

	stateModel.RemoveState(_testResource, _testPartition)
	_, exist = stateModel.GetState(_testResource, _testPartition)
	s.False(exist)
}

func (s *StateModelTestSuite) TestRemoveStateWillCleanSubMap() {
	stateModel := NewStateModel().(*stateModel)
	stateModel.UpdateState(_testResource, _testPartition, StateModelStateOffline)
	stateModel.UpdateState(_testResource, _testPartitionTwo, StateModelStateOffline)
	s.Len(stateModel.stateModelMap, 1)

	stateModel.RemoveState(_testResource, _testPartition)
	stateModel.RemoveState(_testResource, _testPartitionTwo)
	s.Len(stateModel.stateModelMap, 0)
}

func (s *StateModelTestSuite) TestRemoveNonExistentState() {
	stateModel := NewStateModel()
	stateModel.RemoveState(_testResource, _testPartition)
	_, exist := stateModel.GetState(_testResource, _testPartition)
	s.False(exist)

	stateModel.RemoveState(_testResource, _testPartition)
	_, exist = stateModel.GetState(_testResource, _testPartition)
	s.False(exist)
}
