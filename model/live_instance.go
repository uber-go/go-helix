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
	"log"
	"os"
)

// LiveInstance represents a Helix participant that is online
type LiveInstance struct {
	ZNRecord
}

// NewLiveInstance creates a new instance of Message for representing a live instance
func NewLiveInstance(instanceName string, sessionID string) *LiveInstance {
	liveInstance := &LiveInstance{*NewRecord(instanceName)}
	liveInstance.SetSimpleField(FieldKeyHelixVersion, _helixVersion)
	liveInstance.SetSimpleField(FieldKeySessionID, sessionID)
	liveInstance.SetSimpleField(FieldKeyLiveInstance, getLiveInstanceName(instanceName))
	return liveInstance
}

// GetSessionID returns the session ID field
func (i *LiveInstance) GetSessionID() string {
	return i.GetStringField(FieldKeySessionID, "")
}

// Mirrors Java LiveInstanceName, ManagementFactory.getRuntimeMXBean().getName()
func getLiveInstanceName(instanceName string) string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("failed to get host name for live instance name: %v", err)
		hostname = instanceName
	}
	return fmt.Sprintf("%d@%s", os.Getpid(), hostname)
}
