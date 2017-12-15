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

// CurrentState represents a Helix current state
type CurrentState struct {
	ZNRecord
}

// NewCurrentStateFromMsg creates current state from a message
func NewCurrentStateFromMsg(msg *Message, resourceName string, sessionID string) *CurrentState {
	currentState := &CurrentState{*NewRecord(resourceName)}
	currentState.SetBucketSize(msg.GetBucketSize())
	currentState.SetStateModelDef(msg.GetStateModelDef())
	currentState.SetSessionID(sessionID)
	currentState.SetBatchMsgMode(msg.GetBatchMsgMode())
	currentState.SetStateModelFactoryName(msg.GetStateModelFactoryName())
	return currentState
}

// GetPartitionStateMap returns a map of partition and state
func (s *CurrentState) GetPartitionStateMap() map[string]string {
	result := make(map[string]string, len(s.ZNRecord.MapFields))
	for partition, val := range s.ZNRecord.MapFields {
		result[partition] = val[FieldKeyCurrentState]
	}
	return result
}

// GetState returns state of a partition
func (s *CurrentState) GetState(partition string) string {
	return s.GetMapField(partition, FieldKeyCurrentState)
}

// GetSessionID returns the session ID field
func (s *CurrentState) GetSessionID() string {
	return s.GetStringField(FieldKeySessionID, "")
}

// GetStateModelDef return the state model def field
func (s *CurrentState) GetStateModelDef() string {
	return s.GetStringField(FieldKeyStateModelDef, "")
}

// SetBucketSize sets the bucket size field
func (s *CurrentState) SetBucketSize(size int) {
	s.SetIntField(FieldKeyBucketSize, size)
}

// SetSessionID sets the session ID field
func (s *CurrentState) SetSessionID(ID string) {
	s.SetSimpleField(FieldKeySessionID, ID)
}

// SetStateModelDef sets the state model definition field
func (s *CurrentState) SetStateModelDef(stateModelDef string) {
	s.SetSimpleField(FieldKeyStateModelDef, stateModelDef)
}

// SetBatchMsgMode sets the batch message mode field
func (s *CurrentState) SetBatchMsgMode(mode bool) {
	s.SetBooleanField(FieldKeyBatchMsgMode, mode)
}

// SetStateModelFactoryName sets state model factory name field
func (s *CurrentState) SetStateModelFactoryName(name string) {
	s.SetSimpleField(FieldKeyStateModelFactoryName, name)
}

// SetState sets state of a partition
func (s *CurrentState) SetState(partition string, state string) {
	s.SetMapField(partition, FieldKeyCurrentState, state)
}
