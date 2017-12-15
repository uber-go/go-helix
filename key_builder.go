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
)

// KeyBuilder generates Zookeeper paths
// Mirrors org.apache.helix.PropertyKey#Builder
type KeyBuilder struct {
	clusterName string
}

func (b *KeyBuilder) cluster() string {
	return fmt.Sprintf("/%s", b.clusterName)
}

func (b *KeyBuilder) clusterConfig() string {
	return fmt.Sprintf("/%s/CONFIGS/CLUSTER/%s", b.clusterName, b.clusterName)
}

func (b *KeyBuilder) controller() string {
	return fmt.Sprintf("/%s/CONTROLLER", b.clusterName)
}

func (b *KeyBuilder) controllerMessages() string {
	return fmt.Sprintf("/%s/CONTROLLER/MESSAGES", b.clusterName)
}

func (b *KeyBuilder) controllerErrors() string {
	return fmt.Sprintf("/%s/CONTROLLER/ERRORS", b.clusterName)
}

func (b *KeyBuilder) controllerStatusUpdates() string {
	return fmt.Sprintf("/%s/CONTROLLER/STATUSUPDATES", b.clusterName)
}

func (b *KeyBuilder) controllerHistory() string {
	return fmt.Sprintf("/%s/CONTROLLER/HISTORY", b.clusterName)
}

func (b *KeyBuilder) externalView() string {
	return fmt.Sprintf("/%s/EXTERNALVIEW", b.clusterName)
}

func (b *KeyBuilder) externalViewForResource(resource string) string {
	return fmt.Sprintf("/%s/EXTERNALVIEW/%s", b.clusterName, resource)
}

func (b *KeyBuilder) propertyStore() string {
	return fmt.Sprintf("/%s/PROPERTYSTORE", b.clusterName)
}

func (b *KeyBuilder) idealStates() string {
	return fmt.Sprintf("/%s/IDEALSTATES", b.clusterName)
}

// IdealStateForResource returns path for ideal state of a resource
func (b *KeyBuilder) idealStateForResource(resource string) string {
	return fmt.Sprintf("/%s/IDEALSTATES/%s", b.clusterName, resource)
}

func (b *KeyBuilder) resourceConfigs() string {
	return fmt.Sprintf("/%s/CONFIGS/RESOURCE", b.clusterName)
}

func (b *KeyBuilder) resourceConfig(resource string) string {
	return fmt.Sprintf("/%s/CONFIGS/RESOURCE/%s", b.clusterName, resource)
}

func (b *KeyBuilder) participantConfigs() string {
	return fmt.Sprintf("/%s/CONFIGS/PARTICIPANT", b.clusterName)
}

func (b *KeyBuilder) participantConfig(participantID string) string {
	return fmt.Sprintf("/%s/CONFIGS/PARTICIPANT/%s", b.clusterName, participantID)
}

func (b *KeyBuilder) liveInstances() string {
	return fmt.Sprintf("/%s/LIVEINSTANCES", b.clusterName)
}

func (b *KeyBuilder) instances() string {
	return fmt.Sprintf("/%s/INSTANCES", b.clusterName)
}

func (b *KeyBuilder) instance(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s", b.clusterName, participantID)
}

func (b *KeyBuilder) liveInstance(partipantID string) string {
	return fmt.Sprintf("/%s/LIVEINSTANCES/%s", b.clusterName, partipantID)
}

func (b *KeyBuilder) currentStates(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/CURRENTSTATES", b.clusterName, participantID)
}

func (b *KeyBuilder) currentStatesForSession(participantID string, sessionID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/CURRENTSTATES/%s", b.clusterName, participantID, sessionID)
}

func (b *KeyBuilder) currentStateForResource(
	participantID string, sessionID string, resourceID string) string {
	return fmt.Sprintf(
		"/%s/INSTANCES/%s/CURRENTSTATES/%s/%s", b.clusterName, participantID, sessionID, resourceID)
}

func (b *KeyBuilder) errorsR(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/ERRORS", b.clusterName, participantID)
}

func (b *KeyBuilder) errors(participantID string, sessionID string, resourceID string) string {
	return fmt.Sprintf(
		"/%s/INSTANCES/%s/ERRORS/%s/%s", b.clusterName, participantID, sessionID, resourceID)
}

func (b *KeyBuilder) healthReport(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/HEALTHREPORT", b.clusterName, participantID)
}

func (b *KeyBuilder) statusUpdates(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/STATUSUPDATES", b.clusterName, participantID)
}

func (b *KeyBuilder) stateModelDefs() string {
	return fmt.Sprintf("/%s/STATEMODELDEFS", b.clusterName)
}

func (b *KeyBuilder) stateModelDef(stateModel string) string {
	return fmt.Sprintf("/%s/STATEMODELDEFS/%s", b.clusterName, stateModel)
}

func (b *KeyBuilder) participantMessages(participantID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/MESSAGES", b.clusterName, participantID)
}

func (b *KeyBuilder) participantMsg(participantID string, messageID string) string {
	return fmt.Sprintf("/%s/INSTANCES/%s/MESSAGES/%s", b.clusterName, participantID, messageID)
}

// IdealStateKey returns path for ideal state of a given cluster and resource
func IdealStateKey(clusterName string, resourceName string) string {
	return fmt.Sprintf("/%s/IDEALSTATES/%s", clusterName, resourceName)
}
