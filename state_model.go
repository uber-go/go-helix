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

import "sync"

// StateModel mirrors the behavior of org.apache.helix.participant.statemachine.StateModel and
// org.apache.helix.participant.statemachine.StateModelFactory
// it keeps the state of each resource/partition combination
type StateModel interface {
	// GetState returns the state of a resource/partition combination
	GetState(resourceName string, partitionKey string) (string, bool)
	// UpdateState updates the state of a resource/partition combination
	UpdateState(resourceName string, partitionKey string, state string)
	// RemoveState removes the state of a resource/partition combination
	RemoveState(resourceName string, partitionKey string)
}

type stateModel struct {
	sync.RWMutex
	stateModelMap map[string]map[string]string
}

// NewStateModel creates a StateModel
func NewStateModel() StateModel {
	return &stateModel{
		stateModelMap: make(map[string]map[string]string),
	}
}

func (s *stateModel) GetState(resourceName string, partitionKey string) (string, bool) {
	s.RLock()
	defer s.RUnlock()
	partitionStateMap, exists := s.stateModelMap[resourceName]
	if !exists {
		return "", false
	}
	state, exists := partitionStateMap[partitionKey]
	if !exists {
		return "", false
	}
	return state, true
}

func (s *stateModel) UpdateState(resourceName string, partitionKey string, state string) {
	s.Lock()
	defer s.Unlock()
	partitionStateMap, exists := s.stateModelMap[resourceName]
	if !exists {
		partitionStateMap = make(map[string]string)
		s.stateModelMap[resourceName] = partitionStateMap
	}
	partitionStateMap[partitionKey] = state
}

func (s *stateModel) RemoveState(resourceName string, partitionKey string) {
	s.Lock()
	defer s.Unlock()
	partitionStateMap, exists := s.stateModelMap[resourceName]
	if !exists {
		return
	}
	delete(partitionStateMap, partitionKey)
	if len(partitionStateMap) == 0 {
		delete(s.stateModelMap, resourceName)
	}
}
