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
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// TestParticipant is a participant used for test purpose
type TestParticipant struct {
	Participant
}

// NewTestParticipant returns a TestParticipant
func NewTestParticipant(
	logger *zap.Logger,
	scope tally.Scope,
	zkConnectString string,
	application string,
	clusterName string,
	resourceName string,
	host string,
	port int32,
) (*TestParticipant, <-chan error) {
	participant, fatalErrChan :=
		NewParticipant(logger, scope, zkConnectString, application, clusterName, resourceName, host, port)
	return &TestParticipant{participant}, fatalErrChan
}

// GetFatalErrorChan returns the fatal error chan, so user can send test error
func (p *TestParticipant) GetFatalErrorChan() chan error {
	return p.Participant.(*participant).fatalErrChan
}
