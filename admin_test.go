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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/go-helix/model"
)

type AdminTestSuite struct {
	BaseHelixTestSuite
}

func TestAdminTestSuite(t *testing.T) {
	suite.Run(t, &AdminTestSuite{})
}

func (s *AdminTestSuite) TestAddAndDropCluster() {
	t := s.T()

	// definitely a new cluster name by timestamp
	now := time.Now().Local()
	cluster := "AdminTest_TestAddAndDropCluster_" + now.Format("20060102150405")

	added := s.Admin.AddCluster(cluster, false)
	if !added {
		t.Error("Failed to add cluster: " + cluster)
	}

	// if cluster is already added, add it again and it should return false
	added = s.Admin.AddCluster(cluster, false)
	if added {
		t.Error("Should fail to add the same cluster")
	}

	//listClusters
	info, err := s.Admin.ListClusters()
	if err != nil || !strings.Contains(info, cluster) {
		t.Error("Expect OK")
	}

	s.Admin.DropCluster(cluster)
	info, err = s.Admin.ListClusters()
	if err != nil || strings.Contains(info, cluster) {
		t.Error("Expect dropped")
	}
}

func (s *AdminTestSuite) TestAddCluster() {
	now := time.Now().Local()
	cluster := "AdminTest_TestAddCluster" + now.Format("20060102150405")

	added := s.Admin.AddCluster(cluster, false)
	if added {
		defer s.Admin.DropCluster(cluster)
	}

	clusterInfo, err := s.Admin.ListClusterInfo(cluster)
	s.True(len(clusterInfo) > 0)
	s.NoError(err)

	// verify the data structure in zookeeper
	propertyStore := fmt.Sprintf("/%s/PROPERTYSTORE", cluster)
	s.verifyNodeExist(propertyStore)

	stateModelDefs := fmt.Sprintf("/%s/STATEMODELDEFS", cluster)
	s.verifyNodeExist(stateModelDefs)
	s.verifyChildrenCount(stateModelDefs, 6)

	instances := fmt.Sprintf("/%s/INSTANCES", cluster)
	s.verifyNodeExist(instances)

	instancesInfo, err := s.Admin.ListInstances(cluster)
	s.True(len(instancesInfo) > 0)
	s.NoError(err)
	err = s.Admin.GetInstances(cluster)
	s.NoError(err)

	configs := fmt.Sprintf("/%s/CONFIGS", cluster)
	s.verifyNodeExist(configs)
	s.verifyChildrenCount(configs, 3)

	idealStates := fmt.Sprintf("/%s/IDEALSTATES", cluster)
	s.verifyNodeExist(idealStates)

	externalView := fmt.Sprintf("/%s/EXTERNALVIEW", cluster)
	s.verifyNodeExist(externalView)

	liveInstances := fmt.Sprintf("/%s/LIVEINSTANCES", cluster)
	s.verifyNodeExist(liveInstances)

	controller := fmt.Sprintf("/%s/CONTROLLER", cluster)
	s.verifyNodeExist(controller)
	s.verifyChildrenCount(controller, 4)
}

func (s *AdminTestSuite) TestSetConfig() {
	t := s.T()

	now := time.Now().Local()
	cluster := "AdminTest_TestSetConfig_" + now.Format("20060102150405")

	added := s.Admin.AddCluster(cluster, false)
	if added {
		defer s.Admin.DropCluster(cluster)
	}

	property := map[string]string{
		_allowParticipantAutoJoinKey: "true",
	}

	err := s.Admin.SetConfig(cluster, "CLUSTER", property)
	s.NoError(err)

	prop, err := s.Admin.GetConfig(cluster, "CLUSTER", []string{_allowParticipantAutoJoinKey})
	s.NoError(err)

	if prop[_allowParticipantAutoJoinKey] != "true" {
		t.Error("allowParticipantAutoJoin config set/get failed")
	}
	err = s.Admin.SetConfig(cluster, "CONSTRAINT", property)
	s.NoError(err)
	prop, err = s.Admin.GetConfig(cluster, "CONSTRAINT", []string{})
	s.Equal(0, len(prop))
	s.NoError(err)
	err = s.Admin.SetConfig(cluster, "PARTICIPANT", property)
	s.NoError(err)
	prop, err = s.Admin.GetConfig(cluster, "PARTICIPANT", []string{})
	s.Equal(0, len(prop))
	s.NoError(err)
	err = s.Admin.SetConfig(cluster, "PARTITION", property)
	s.NoError(err)
	prop, err = s.Admin.GetConfig(cluster, "PARTITION", []string{})
	s.Equal(0, len(prop))
	s.NoError(err)
	err = s.Admin.SetConfig(cluster, "RESOURCE", property)
	s.NoError(err)
	prop, err = s.Admin.GetConfig(cluster, "RESOURCE", []string{})
	s.Equal(0, len(prop))
	s.NoError(err)
}

func (s *AdminTestSuite) TestAddDropNode() {
	t := s.T()

	// verify not able to add node before cluster is setup
	now := time.Now().Local()
	cluster := "AdminTest_TestAddDropNode_" + now.Format("20060102150405")

	node := "localhost_19932"

	// add node before adding cluster, expect fail
	if err := s.Admin.AddNode(cluster, node); err != ErrClusterNotSetup {
		t.Error("Must error out for AddNode if cluster not setup")
	}

	// now add the cluster and add the node again
	s.Admin.AddCluster(cluster, false)
	defer s.Admin.DropCluster(cluster)

	if err := s.Admin.AddNode(cluster, node); err != nil {
		t.Error("Should be able to add node")
	}

	// add the same node again, should expect error ErrNodeAlreadyExists
	if err := s.Admin.AddNode(cluster, node); err != ErrNodeAlreadyExists {
		t.Error("should not be able to add the same node")
	}

	// listInstanceInfo
	info, err := s.Admin.ListInstanceInfo(cluster, node)
	if err != nil || info == "" || !strings.Contains(info, node) {
		t.Error("expect OK")
	}

	// drop the node
	if err := s.Admin.DropNode(cluster, node); err != nil {
		t.Error("failed to drop cluster node")
	}
	// listInstanceInfo
	if _, err := s.Admin.ListInstanceInfo(cluster, node); err != ErrNodeNotExist {
		t.Error("expect OK")
	}

	// drop node again and we should see an error ErrNodeNotExist
	if err := s.Admin.DropNode(cluster, node); err != ErrNodeNotExist {
		t.Error("failed to see expected error ErrNodeNotExist")
	}

	// make sure the path does not exist in zookeeper
	s.verifyNodeNotExist(fmt.Sprintf("/%s/INSTANCES/%s", cluster, node))
	s.verifyNodeNotExist(fmt.Sprintf("/%s/CONFIGS/PARTICIPANT/%s", cluster, node))
}

func (s *AdminTestSuite) TestAddDropResource() {
	t := s.T()

	now := time.Now().Local()
	cluster := "AdminTest_TestAddResource_" + now.Format("20060102150405")
	resource := "resource"

	// expect error if cluster not setup
	err := s.Admin.AddResource(cluster, resource, 32, "MasterSlave")
	if err != ErrClusterNotSetup {
		t.Error("must setup cluster before addResource")
	}
	if err := s.Admin.DropResource(cluster, resource); err != ErrClusterNotSetup {
		t.Error("must setup cluster before addResource")
	}
	if _, err := s.Admin.ListResources(cluster); err != ErrClusterNotSetup {
		t.Error("must setup cluster")
	}

	s.Admin.AddCluster(cluster, false)
	defer s.Admin.DropCluster(cluster)

	// it is ok to dropResource before resource exists
	if err := s.Admin.DropResource(cluster, resource); err != nil {
		t.Error("expect OK")
	}

	// expect error if state model does not exist
	err = s.Admin.AddResource(cluster, resource, 32, "NotExistStateModel")
	if err != ErrStateModelDefNotExist {
		t.Error("must use valid state model")
	}

	// expect pass
	err = s.Admin.AddResource(cluster, resource, 32, "MasterSlave")
	if err != nil {
		t.Error("fail addResource")
	}
	// expect failure
	err = s.Admin.AddResource(cluster, resource, 32, "MasterSlave")
	s.Error(err)

	if info, err := s.Admin.ListResources(cluster); err != nil || info == "" {
		t.Error("expect OK")
	}

	kb := KeyBuilder{cluster}
	isPath := kb.idealStates() + "/resource"
	s.verifyNodeExist(isPath)

	if err := s.Admin.DropResource(cluster, resource); err != nil {
		t.Error("expect OK")
	}

	s.verifyNodeNotExist(isPath)
}

func (s *AdminTestSuite) TestEnableDisableResource() {
	t := s.T()

	now := time.Now().Local()
	cluster := "AdminTest_TestEnableDisableResource_" + now.Format("20060102150405")
	resource := "resource"

	// expect error if cluster not setup
	if err := s.Admin.EnableResource(cluster, resource); err != ErrClusterNotSetup {
		t.Error("must setup cluster before enableResource")
	}
	if err := s.Admin.DisableResource(cluster, resource); err != ErrClusterNotSetup {
		t.Error("must setup cluster before enableResource")
	}

	s.Admin.AddCluster(cluster, false)
	defer s.Admin.DropCluster(cluster)

	// expect error if resource not exist
	if err := s.Admin.EnableResource(cluster, resource); err != ErrResourceNotExists {
		t.Error("expect ErrResourceNotExists")
	}
	if err := s.Admin.DisableResource(cluster, resource); err != ErrResourceNotExists {
		t.Error("expect ErrResourceNotExists")
	}
	if err := s.Admin.AddResource(cluster, "resource", 32, "MasterSlave"); err != nil {
		t.Error("fail addResource")
	}
	if err := s.Admin.EnableResource(cluster, resource); err != nil {
		// expect error if resource not exist
		t.Error("expect OK")
	}
}

func (s *AdminTestSuite) TestListExternalView() {
	t := s.T()

	now := time.Now().Local()
	cluster := "AdminTest_TestListExternalView_" + now.Format("20060102150405")
	resource := "resource"

	// expect error if cluster not setup
	if _, err := s.Admin.ListExternalView(cluster, resource); err != ErrClusterNotSetup {
		t.Error("must setup cluster before ListExternalView")
	}

	s.Admin.AddCluster(cluster, false)
	defer s.Admin.DropCluster(cluster)

	// fail when resource doesn't exist
	if _, err := s.Admin.ListExternalView(cluster, resource); err != ErrNodeNotExist {
		t.Error("must setup resource before ListExternalView")
	}
	// expect pass
	if err := s.Admin.AddResource(cluster, "resource", 32, "MasterSlave"); err != nil {
		t.Error("fail addResource")
	}
	// should fail, doesn't create externalView by default
	if _, err := s.Admin.ListExternalView(cluster, resource); err != ErrNodeNotExist {
		t.Error("must setup resource externalView before ListExternalView")
	}

	// create externalview
	externalView := fmt.Sprintf("/%s/EXTERNALVIEW/%s", cluster, resource)
	m := model.NewRecord("resource")
	m.SetIntField("NUM_PARTITIONS", 32)
	data, err := json.Marshal(m)
	if err != nil {
		t.Error("expect OK")
	}
	s.Admin.zkClient.CreateDataWithPath(externalView, data)

	res, err := s.Admin.ListExternalView(cluster, resource)
	if err != nil {
		t.Error("expect OK")
	}
	if res.ZNRecord.ID != "resource" || res.GetNumPartitions() != 32 {
		t.Error("expect read model OK")
	}
}

func (s *AdminTestSuite) TestListIdealState() {
	t := s.T()

	now := time.Now().Local()
	cluster := "AdminTest_TestListIdealState_" + now.Format("20060102150405")
	resource := "resource"

	// expect error if cluster not setup
	if _, err := s.Admin.ListIdealState(cluster, resource); err != ErrClusterNotSetup {
		t.Error("must setup cluster before ListIdealState")
	}

	s.Admin.AddCluster(cluster, false)
	defer s.Admin.DropCluster(cluster)

	// fail when resource doesn't exist
	if _, err := s.Admin.ListIdealState(cluster, resource); err != ErrNodeNotExist {
		t.Error("must setup resource before ListIdealState")
	}
	// expect pass
	if err := s.Admin.AddResource(cluster, "resource", 32, "MasterSlave"); err != nil {
		t.Error("fail addResource")
	}

	res, err := s.Admin.ListIdealState(cluster, resource)
	if err != nil {
		t.Error("expect OK")
	}
	if res.ZNRecord.ID != "resource" || res.GetNumPartitions() != 32 {
		t.Error("expect read model OK")
	}
}

func (s *AdminTestSuite) verifyNodeExist(path string) {
	if exists, _, err := s.Admin.zkClient.Exists(path); err != nil || !exists {
		s.T().Error("failed verifyNodeExist")
	}
}

func (s *AdminTestSuite) verifyNodeNotExist(path string) {
	if exists, _, err := s.Admin.zkClient.Exists(path); err != nil || exists {
		s.T().Error("failed verifyNotNotExist")
		s.T().FailNow()
	}
}

func (s *AdminTestSuite) verifyChildrenCount(path string, count int) {
	children, err := s.Admin.zkClient.Children(path)
	if err != nil {
		s.T().FailNow()
	}

	if len(children) != count {
		s.T().Errorf("Node %s should have %d children, "+
			"but only have %d children", path, count, len(children))
	}
}
