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
	"github.com/uber-go/go-helix/model"
)

// StateTransitionHandler is type for handler method
type StateTransitionHandler func(msg *model.Message) error

// Transition associates a handler function with state transitions
type Transition struct {
	FromState string
	ToState   string
	Handler   StateTransitionHandler
}

// StateModelProcessor handles state transitions
// This mirrors org.apache.helix.participant.statemachine.StateModelFactory
type StateModelProcessor struct {
	// fromState->toState->StateTransitionHandler
	Transitions map[string]map[string]StateTransitionHandler
}

// NewStateModelProcessor functions similarly to StateMachineEngine
func NewStateModelProcessor() *StateModelProcessor {
	return &StateModelProcessor{
		Transitions: map[string]map[string]StateTransitionHandler{},
	}
}

// AddTransition adds a new transition handler
func (p *StateModelProcessor) AddTransition(fromState string, toState string, handler StateTransitionHandler) {
	if _, ok := p.Transitions[fromState]; !ok {
		p.Transitions[fromState] = make(map[string]StateTransitionHandler)
	}
	p.Transitions[fromState][toState] = handler
}
