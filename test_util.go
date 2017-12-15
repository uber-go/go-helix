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
	"log"
	"math/rand"

	"github.com/uber-go/go-helix/model"
	"github.com/uber-go/go-helix/zk"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	// TestClusterName is the name of cluster used in test
	TestClusterName = "test_cluster"
	// TestResource is the name of resource used in test
	TestResource        = "test_resource"
	testApplication     = "test_app"
	testParticipantHost = "localhost"
	testInstanceName    = "test_instance"
	maxValidPort        = int32(2<<15) - 1
)

// BaseHelixTestSuite can be embedded in any test suites that need to
// interact with the test zk server or helix admin. It provides ZK
// connect string and Helix admin references for convenient uses in parent suites.
type BaseHelixTestSuite struct {
	zk.BaseZkTestSuite
	Admin *Admin
}

// SetupSuite ensures zk server is up
func (s *BaseHelixTestSuite) SetupSuite() {
	s.BaseZkTestSuite.SetupSuite()
	admin, err := NewAdmin(s.ZkConnectString)
	s.Admin = admin
	s.NoError(err)

	s.ensureHelixClusterUp()
}

// TearDownSuite disconnects zk if not done already
func (s *BaseHelixTestSuite) TearDownSuite() {
	if s.Admin.zkClient.IsConnected() {
		s.Admin.zkClient.Disconnect()
	}
}

// GetRandomPort returns random valid port number (1~65535)
func GetRandomPort() int32 {
	return rand.Int31n(maxValidPort) + 1
}

func (s *BaseHelixTestSuite) ensureHelixClusterUp() {
	setup, err := s.Admin.isClusterSetup(TestClusterName)
	s.NoError(err, "Error checking helix cluster setup")
	if !setup {
		log.Println("test helix cluster doesn't exist, try creating..")
		created := s.Admin.AddCluster(TestClusterName, true)
		if !created {
			s.Fail("Error creating helix cluster")
		}
	}
	err = s.Admin.SetConfig(TestClusterName, "CLUSTER", map[string]string{
		_allowParticipantAutoJoinKey: "true",
	})
	s.NoError(err, "error setting cluster config")
}

func (s *BaseHelixTestSuite) createParticipantAndConnect() *Participant {
	port := GetRandomPort()
	p := NewParticipant(zap.NewNop(), tally.NoopScope,
		s.ZkConnectString, testApplication, TestClusterName, TestResource, testParticipantHost, port)
	s.NotNil(p)
	p.RegisterStateModel(StateModelNameOnlineOffline, createNoopStateModelProcessor())
	log.Println("Created participant ", p.instanceName)
	err := p.Connect()
	s.NoError(err)
	return p
}

func createNoopStateModelProcessor() *StateModelProcessor {
	processor := NewStateModelProcessor()
	processor.AddTransition(
		StateModelStateOnline, StateModelStateOffline, func(m *model.Message) error {
			log.Printf("partition ONLINE=>OFFLINE: %v", m)
			return nil
		})
	processor.AddTransition(
		StateModelStateOffline, StateModelStateOnline, func(m *model.Message) error {
			log.Printf("partition OFFLINE=>ONLINE: %v", m)
			return nil
		})
	processor.AddTransition(
		StateModelStateOffline, StateModelStateDropped, func(m *model.Message) error {
			log.Printf("partition OFFLINE=>DROPPED: %v", m)
			return nil
		})
	return processor
}

// CreateRandomString creates a random with numeric characters
func CreateRandomString() string {
	return fmt.Sprintf("%d", rand.Int63())
}
