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
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/uber-go/go-helix/model"
	"github.com/uber-go/go-helix/zk"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	// ErrClusterNotSetup means the helix data structure in zookeeper /{CLUSTER_NAME}
	// is not correct or does not exist
	ErrClusterNotSetup = errors.New("cluster not setup")

	// ErrNodeAlreadyExists the zookeeper node exists when it is not expected to
	ErrNodeAlreadyExists = errors.New("node already exists in cluster")

	// ErrNodeNotExist the zookeeper node does not exist when it is expected to
	ErrNodeNotExist = errors.New("node does not exist in config for cluster")

	// ErrInstanceNotExist the instance of a cluster does not exist when it is expected to
	ErrInstanceNotExist = errors.New("node does not exist in instances for cluster")

	// ErrStateModelDefNotExist the state model definition is expected to exist in zookeeper
	ErrStateModelDefNotExist = errors.New("state model not exist in cluster")

	// ErrResourceExists the resource already exists in cluster and cannot be added again
	ErrResourceExists = errors.New("resource already exists in cluster")

	// ErrResourceNotExists the resource does not exists and cannot be removed
	ErrResourceNotExists = errors.New("resource not exists in cluster")
)

var (
	_helixDefaultNodes = map[string]string{
		"LeaderStandby": `
	{
	  "id" : "LeaderStandby",
	  "mapFields" : {
		"DROPPED.meta" : {
		  "count" : "-1"
		},
		"LEADER.meta" : {
		  "count" : "1"
		},
		"LEADER.next" : {
		  "DROPPED" : "STANDBY",
		  "STANDBY" : "STANDBY",
		  "OFFLINE" : "STANDBY"
		},
		"OFFLINE.meta" : {
		  "count" : "-1"
		},
		"OFFLINE.next" : {
		  "DROPPED" : "DROPPED",
		  "STANDBY" : "STANDBY",
		  "LEADER" : "STANDBY"
		},
		"STANDBY.meta" : {
		  "count" : "R"
		},
		"STANDBY.next" : {
		  "DROPPED" : "OFFLINE",
		  "OFFLINE" : "OFFLINE",
		  "LEADER" : "LEADER"
		}
	  },
	  "listFields" : {
		"STATE_PRIORITY_LIST" : [ "LEADER", "STANDBY", "OFFLINE", "DROPPED" ],
		"STATE_TRANSITION_PRIORITYLIST" : [ "LEADER-STANDBY", "STANDBY-LEADER", "OFFLINE-STANDBY", "STANDBY-OFFLINE", "OFFLINE-DROPPED" ]
	  },
	  "simpleFields" : {
		"INITIAL_STATE" : "OFFLINE"
	  }
	}
	`,
		"MasterSlave": `
	{
	  "id" : "MasterSlave",
	  "mapFields" : {
		"DROPPED.meta" : {
		  "count" : "-1"
		},
		"ERROR.meta" : {
		  "count" : "-1"
		},
		"ERROR.next" : {
		  "DROPPED" : "DROPPED",
		  "OFFLINE" : "OFFLINE"
		},
		"MASTER.meta" : {
		  "count" : "1"
		},
		"MASTER.next" : {
		  "SLAVE" : "SLAVE",
		  "DROPPED" : "SLAVE",
		  "OFFLINE" : "SLAVE"
		},
		"OFFLINE.meta" : {
		  "count" : "-1"
		},
		"OFFLINE.next" : {
		  "SLAVE" : "SLAVE",
		  "DROPPED" : "DROPPED",
		  "MASTER" : "SLAVE"
		},
		"SLAVE.meta" : {
		  "count" : "R"
		},
		"SLAVE.next" : {
		  "DROPPED" : "OFFLINE",
		  "OFFLINE" : "OFFLINE",
		  "MASTER" : "MASTER"
		}
	  },
	  "listFields" : {
		"STATE_PRIORITY_LIST" : [ "MASTER", "SLAVE", "OFFLINE", "DROPPED", "ERROR" ],
		"STATE_TRANSITION_PRIORITYLIST" : [ "MASTER-SLAVE", "SLAVE-MASTER", "OFFLINE-SLAVE", "SLAVE-OFFLINE", "OFFLINE-DROPPED" ]
	  },
	  "simpleFields" : {
		"INITIAL_STATE" : "OFFLINE"
	  }
	}
	`,
		"OnlineOffline": `
	{
	  "id" : "OnlineOffline",
	  "mapFields" : {
		"DROPPED.meta" : {
		  "count" : "-1"
		},
		"OFFLINE.meta" : {
		  "count" : "-1"
		},
		"OFFLINE.next" : {
		  "DROPPED" : "DROPPED",
		  "ONLINE" : "ONLINE"
		},
		"ONLINE.meta" : {
		  "count" : "R"
		},
		"ONLINE.next" : {
		  "DROPPED" : "OFFLINE",
		  "OFFLINE" : "OFFLINE"
		}
	  },
	  "listFields" : {
		"STATE_PRIORITY_LIST" : [ "ONLINE", "OFFLINE", "DROPPED" ],
		"STATE_TRANSITION_PRIORITYLIST" : [ "OFFLINE-ONLINE", "ONLINE-OFFLINE", "OFFLINE-DROPPED" ]
	  },
	  "simpleFields" : {
		"INITIAL_STATE" : "OFFLINE"
	  }
	}
	`,
		"STORAGE_DEFAULT_SM_SCHEMATA": `
	{
	  "id" : "STORAGE_DEFAULT_SM_SCHEMATA",
	  "mapFields" : {
		"DROPPED.meta" : {
		  "count" : "-1"
		},
		"ERROR.meta" : {
		  "count" : "-1"
		},
		"ERROR.next" : {
		  "DROPPED" : "DROPPED",
		  "OFFLINE" : "OFFLINE"
		},
		"MASTER.meta" : {
		  "count" : "N"
		},
		"MASTER.next" : {
		  "DROPPED" : "OFFLINE",
		  "OFFLINE" : "OFFLINE"
		},
		"OFFLINE.meta" : {
		  "count" : "-1"
		},
		"OFFLINE.next" : {
		  "DROPPED" : "DROPPED",
		  "MASTER" : "MASTER"
		}
	  },
	  "listFields" : {
		"STATE_PRIORITY_LIST" : [ "MASTER", "OFFLINE", "DROPPED", "ERROR" ],
		"STATE_TRANSITION_PRIORITYLIST" : [ "MASTER-OFFLINE", "OFFLINE-MASTER" ]
	  },
	  "simpleFields" : {
		"INITIAL_STATE" : "OFFLINE"
	  }
	}
	`,
		"SchedulerTaskQueue": `
	{
	  "id" : "SchedulerTaskQueue",
	  "mapFields" : {
		"COMPLETED.meta" : {
		  "count" : "1"
		},
		"COMPLETED.next" : {
		  "DROPPED" : "DROPPED",
		  "COMPLETED" : "COMPLETED"
		},
		"DROPPED.meta" : {
		  "count" : "-1"
		},
		"DROPPED.next" : {
		  "DROPPED" : "DROPPED"
		},
		"OFFLINE.meta" : {
		  "count" : "-1"
		},
		"OFFLINE.next" : {
		  "DROPPED" : "DROPPED",
		  "OFFLINE" : "OFFLINE",
		  "COMPLETED" : "COMPLETED"
		}
	  },
	  "listFields" : {
		"STATE_PRIORITY_LIST" : [ "COMPLETED", "OFFLINE", "DROPPED" ],
		"STATE_TRANSITION_PRIORITYLIST" : [ "OFFLINE-COMPLETED", "OFFLINE-DROPPED", "COMPLETED-DROPPED" ]
	  },
	  "simpleFields" : {
		"INITIAL_STATE" : "OFFLINE"
	  }
	}
	`,
		"Task": `
	{
	  "id" : "Task",
	  "mapFields" : {
		"COMPLETED.meta" : {
		  "count" : "-1"
		},
		"COMPLETED.next" : {
		  "STOPPED" : "INIT",
		  "DROPPED" : "DROPPED",
		  "RUNNING" : "INIT",
		  "INIT" : "INIT",
		  "COMPLETED" : "COMPLETED",
		  "TASK_ERROR" : "INIT",
		  "TIMED_OUT" : "INIT"
		},
		"DROPPED.meta" : {
		  "count" : "-1"
		},
		"DROPPED.next" : {
		  "DROPPED" : "DROPPED"
		},
		"INIT.meta" : {
		  "count" : "-1"
		},
		"INIT.next" : {
		  "STOPPED" : "RUNNING",
		  "DROPPED" : "DROPPED",
		  "RUNNING" : "RUNNING",
		  "INIT" : "INIT",
		  "COMPLETED" : "RUNNING",
		  "TASK_ERROR" : "RUNNING",
		  "TIMED_OUT" : "RUNNING"
		},
		"RUNNING.meta" : {
		  "count" : "-1"
		},
		"RUNNING.next" : {
		  "STOPPED" : "STOPPED",
		  "DROPPED" : "DROPPED",
		  "RUNNING" : "RUNNING",
		  "INIT" : "INIT",
		  "COMPLETED" : "COMPLETED",
		  "TASK_ERROR" : "TASK_ERROR",
		  "TIMED_OUT" : "TIMED_OUT"
		},
		"STOPPED.meta" : {
		  "count" : "-1"
		},
		"STOPPED.next" : {
		  "STOPPED" : "STOPPED",
		  "DROPPED" : "DROPPED",
		  "RUNNING" : "RUNNING",
		  "INIT" : "INIT",
		  "COMPLETED" : "RUNNING",
		  "TASK_ERROR" : "RUNNING",
		  "TIMED_OUT" : "RUNNING"
		},
		"TASK_ERROR.meta" : {
		  "count" : "-1"
		},
		"TASK_ERROR.next" : {
		  "STOPPED" : "INIT",
		  "DROPPED" : "DROPPED",
		  "RUNNING" : "INIT",
		  "INIT" : "INIT",
		  "COMPLETED" : "INIT",
		  "TIMED_OUT" : "INIT",
		  "TASK_ERROR" : "TASK_ERROR"
		},
		"TIMED_OUT.meta" : {
		  "count" : "-1"
		},
		"TIMED_OUT.next" : {
		  "STOPPED" : "INIT",
		  "DROPPED" : "DROPPED",
		  "RUNNING" : "INIT",
		  "INIT" : "INIT",
		  "COMPLETED" : "INIT",
		  "TASK_ERROR" : "INIT",
		  "TIMED_OUT" : "TIMED_OUT"
		}
	  },
	  "listFields" : {
		"STATE_PRIORITY_LIST" : [ "INIT", "RUNNING", "STOPPED", "COMPLETED", "TIMED_OUT", "TASK_ERROR", "DROPPED" ],
		"STATE_TRANSITION_PRIORITYLIST" : [ "INIT-RUNNING", "RUNNING-STOPPED", "RUNNING-COMPLETED", "RUNNING-TIMED_OUT", "RUNNING-TASK_ERROR", "STOPPED-RUNNING", "INIT-DROPPED", "RUNNING-DROPPED", "COMPLETED-DROPPED", "STOPPED-DROPPED", "TIMED_OUT-DROPPED", "TASK_ERROR-DROPPED", "RUNNING-INIT", "COMPLETED-INIT", "STOPPED-INIT", "TIMED_OUT-INIT", "TASK_ERROR-INIT" ]
	  },
	  "simpleFields" : {
		"INITIAL_STATE" : "INIT"
	  }
	}`,
	}
)

// Admin handles the administration task for the Helix cluster. Many of the operations
// are mirroring the implementions documented at
// http://helix.apache.org/0.7.0-incubating-docs/Quickstart.html
type Admin struct {
	zkClient *zk.Client
}

// NewAdmin instantiates Admin
func NewAdmin(zkConnectString string) (*Admin, error) {
	zkClient := zk.NewClient(
		zap.NewNop(), tally.NoopScope, zk.WithZkSvr(zkConnectString), zk.WithSessionTimeout(zk.DefaultSessionTimeout))
	err := zkClient.Connect()
	if err != nil {
		return nil, err
	}

	return &Admin{zkClient: zkClient}, nil
}

// AddCluster add a cluster to Helix. As a result, a znode will be created in zookeeper
// root named after the cluster name, and corresponding data structures are populated
// under this znode.
// The cluster would be dropped and recreated if recreateIfExists is true
func (adm Admin) AddCluster(cluster string, recreateIfExists bool) bool {
	kb := &KeyBuilder{cluster}
	// c = "/<cluster>"
	c := kb.cluster()

	// check if cluster already exists
	exists, _, err := adm.zkClient.Exists(c)
	if err != nil || (exists && !recreateIfExists) {
		return false
	}

	if recreateIfExists {
		if err := adm.zkClient.DeleteTree(c); err != nil {
			return false
		}
	}

	adm.zkClient.CreateEmptyNode(c)

	// PROPERTYSTORE is an empty node
	propertyStore := fmt.Sprintf("/%s/PROPERTYSTORE", cluster)
	adm.zkClient.CreateEmptyNode(propertyStore)

	// STATEMODELDEFS has 6 children
	stateModelDefs := fmt.Sprintf("/%s/STATEMODELDEFS", cluster)
	adm.zkClient.CreateEmptyNode(stateModelDefs)
	adm.zkClient.CreateDataWithPath(
		stateModelDefs+"/LeaderStandby", []byte(_helixDefaultNodes["LeaderStandby"]))
	adm.zkClient.CreateDataWithPath(
		stateModelDefs+"/MasterSlave", []byte(_helixDefaultNodes["MasterSlave"]))
	adm.zkClient.CreateDataWithPath(
		stateModelDefs+"/OnlineOffline", []byte(_helixDefaultNodes[StateModelNameOnlineOffline]))
	adm.zkClient.CreateDataWithPath(
		stateModelDefs+"/STORAGE_DEFAULT_SM_SCHEMATA",
		[]byte(_helixDefaultNodes["STORAGE_DEFAULT_SM_SCHEMATA"]))
	adm.zkClient.CreateDataWithPath(
		stateModelDefs+"/SchedulerTaskQueue", []byte(_helixDefaultNodes["SchedulerTaskQueue"]))
	adm.zkClient.CreateDataWithPath(
		stateModelDefs+"/Task", []byte(_helixDefaultNodes["Task"]))

	// INSTANCES is initailly an empty node
	instances := fmt.Sprintf("/%s/INSTANCES", cluster)
	adm.zkClient.CreateEmptyNode(instances)

	// CONFIGS has 3 children: CLUSTER, RESOURCE, PARTICIPANT
	configs := fmt.Sprintf("/%s/CONFIGS", cluster)
	adm.zkClient.CreateEmptyNode(configs)
	adm.zkClient.CreateEmptyNode(configs + "/PARTICIPANT")
	adm.zkClient.CreateEmptyNode(configs + "/RESOURCE")
	adm.zkClient.CreateEmptyNode(configs + "/CLUSTER")

	clusterNode := model.NewMsg(cluster)
	accessor := newDataAccessor(adm.zkClient, kb)
	accessor.createMsg(configs+"/CLUSTER/"+cluster, clusterNode)

	// empty ideal states
	idealStates := fmt.Sprintf("/%s/IDEALSTATES", cluster)
	adm.zkClient.CreateEmptyNode(idealStates)

	// empty external view
	externalView := fmt.Sprintf("/%s/EXTERNALVIEW", cluster)
	adm.zkClient.CreateEmptyNode(externalView)

	// empty live instances
	liveInstances := fmt.Sprintf("/%s/LIVEINSTANCES", cluster)
	adm.zkClient.CreateEmptyNode(liveInstances)

	// CONTROLLER has four childrens: [ERRORS, HISTORY, MESSAGES, STATUSUPDATES]
	controller := fmt.Sprintf("/%s/CONTROLLER", cluster)
	adm.zkClient.CreateEmptyNode(controller)
	adm.zkClient.CreateEmptyNode(controller + "/ERRORS")
	adm.zkClient.CreateEmptyNode(controller + "/HISTORY")
	adm.zkClient.CreateEmptyNode(controller + "/MESSAGES")
	adm.zkClient.CreateEmptyNode(controller + "/STATUSUPDATES")

	return true
}

// SetConfig set the configuration values for the cluster, defined by the config scope
func (adm Admin) SetConfig(cluster string, scope string, properties map[string]string) error {
	switch strings.ToUpper(scope) {
	case "CLUSTER":
		if allow, ok := properties[_allowParticipantAutoJoinKey]; ok {
			builder := KeyBuilder{cluster}
			path := builder.clusterConfig()

			if strings.ToLower(allow) == "true" {
				adm.zkClient.UpdateSimpleField(path, _allowParticipantAutoJoinKey, "true")
			}
		}
	case "CONSTRAINT":
	case "PARTICIPANT":
	case "PARTITION":
	case "RESOURCE":
	}

	return nil
}

// GetConfig obtains the configuration value of a property, defined by a config scope
func (adm Admin) GetConfig(
	cluster string, scope string, builder []string) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	switch scope {
	case "CLUSTER":
		kb := KeyBuilder{cluster}
		path := kb.clusterConfig()

		for _, k := range builder {
			var err error
			val, err := adm.zkClient.GetSimpleFieldValueByKey(path, k)
			if err != nil {
				return nil, err
			}
			result[k] = val
		}
	case "CONSTRAINT":
	case "PARTICIPANT":
	case "PARTITION":
	case "RESOURCE":
	}

	return result, nil
}

// DropCluster removes a helix cluster from zookeeper. This will remove the
// znode named after the cluster name from the zookeeper root.
func (adm Admin) DropCluster(cluster string) error {
	kb := KeyBuilder{cluster}
	c := kb.cluster()

	return adm.zkClient.DeleteTree(c)
}

// AddNode is the internal implementation corresponding to command
// ./helix-admin.sh --zkSvr <ZookeeperServerAddress> --addNode <clusterName instanceId>
// node is in the form of host_port
func (adm Admin) AddNode(cluster string, node string) error {
	if ok, err := adm.isClusterSetup(cluster); ok == false || err != nil {
		return ErrClusterNotSetup
	}

	// check if node already exists under /<cluster>/CONFIGS/PARTICIPANT/<NODE>
	builder := &KeyBuilder{cluster}
	path := builder.participantConfig(node)
	exists, _, err := adm.zkClient.Exists(path)
	if err != nil {
		return err
	}
	if exists {
		return ErrNodeAlreadyExists
	}

	// create new node for the participant
	parts := strings.Split(node, "_")
	n := model.NewMsg(node)
	n.SetSimpleField("HELIX_HOST", parts[0])
	n.SetSimpleField("HELIX_PORT", parts[1])

	accessor := newDataAccessor(adm.zkClient, builder)
	accessor.createMsg(path, n)
	adm.zkClient.CreateEmptyNode(builder.instance(node))
	adm.zkClient.CreateEmptyNode(builder.participantMessages(node))
	adm.zkClient.CreateEmptyNode(builder.currentStates(node))
	adm.zkClient.CreateEmptyNode(builder.errorsR(node))
	adm.zkClient.CreateEmptyNode(builder.statusUpdates(node))

	return nil
}

// DropNode removes a node from a cluster. The corresponding znodes
// in zookeeper will be removed.
func (adm Admin) DropNode(cluster string, node string) error {
	// check if node already exists under /<cluster>/CONFIGS/PARTICIPANT/<node>
	builder := KeyBuilder{cluster}
	if exists, _, err := adm.zkClient.Exists(builder.participantConfig(node)); !exists || err != nil {
		return ErrNodeNotExist
	}

	// check if node exist under instance: /<cluster>/INSTANCES/<node>
	if exists, _, err := adm.zkClient.Exists(builder.instance(node)); !exists || err != nil {
		return ErrInstanceNotExist
	}

	// delete /<cluster>/CONFIGS/PARTICIPANT/<node>
	adm.zkClient.DeleteTree(builder.participantConfig(node))

	// delete /<cluster>/INSTANCES/<node>
	adm.zkClient.DeleteTree(builder.instance(node))

	return nil
}

// AddResource implements the helix-admin.sh --addResource
// ./helix-admin.sh --zkSvr localhost:2199 --addResource MYCLUSTER myDB 6 MasterSlave
func (adm Admin) AddResource(
	cluster string, resource string, partitions int, stateModel string) error {
	if ok, err := adm.isClusterSetup(cluster); !ok || err != nil {
		return ErrClusterNotSetup
	}

	builder := &KeyBuilder{cluster}

	// make sure the state model def exists
	exists, _, err := adm.zkClient.Exists(builder.stateModelDef(stateModel))
	if err != nil {
		return errors.Wrap(err, "state model doesnt't exist "+stateModel)
	}
	if !exists {
		return ErrStateModelDefNotExist
	}

	// make sure the path for the ideal state does not exit
	isPath := builder.idealStates() + "/" + resource
	if exists, _, err := adm.zkClient.Exists(isPath); exists || err != nil {
		if exists {
			return ErrResourceExists
		}
		return err
	}

	// create the idealstate for the resource
	// is := NewIdealState(resource)
	// is.SetNumPartitions(partitions)
	// is.SetReplicas(0)
	// is.SetRebalanceMode("SEMI_AUTO")
	// is.SetStateModelDefRef(stateModel)
	// // save the ideal state in zookeeper
	// is.Save(conn, cluster)

	is := model.NewMsg(resource)
	is.SetSimpleField("NUM_PARTITIONS", strconv.Itoa(partitions))
	is.SetSimpleField("REPLICAS", strconv.Itoa(0))
	is.SetSimpleField("REBALANCE_MODE", strings.ToUpper("SEMI_AUTO"))
	is.SetStateModelDef(stateModel)

	accessor := newDataAccessor(adm.zkClient, builder)
	accessor.createMsg(isPath, is)

	return nil
}

// DropResource removes the specified resource from the cluster.
func (adm Admin) DropResource(cluster string, resource string) error {
	// make sure the cluster is already setup
	if ok, err := adm.isClusterSetup(cluster); !ok || err != nil {
		return ErrClusterNotSetup
	}

	builder := KeyBuilder{cluster}

	// make sure the path for the ideal state does not exit
	adm.zkClient.DeleteTree(builder.idealStates() + "/" + resource)
	adm.zkClient.DeleteTree(builder.resourceConfig(resource))

	return nil
}

// EnableResource enables the specified resource in the cluster
func (adm Admin) EnableResource(cluster string, resource string) error {
	// make sure the cluster is already setup
	if ok, err := adm.isClusterSetup(cluster); !ok || err != nil {
		return ErrClusterNotSetup
	}

	builder := KeyBuilder{cluster}

	isPath := builder.idealStates() + "/" + resource

	if exists, _, err := adm.zkClient.Exists(isPath); !exists || err != nil {
		if !exists {
			return ErrResourceNotExists
		}
		return err
	}

	// TODO: set the value at leaf node instead of the record level
	adm.zkClient.UpdateSimpleField(isPath, "HELIX_ENABLED", "true")
	return nil
}

// DisableResource disables the specified resource in the cluster.
func (adm Admin) DisableResource(cluster string, resource string) error {
	// make sure the cluster is already setup
	if ok, err := adm.isClusterSetup(cluster); !ok || err != nil {
		return ErrClusterNotSetup
	}

	builder := KeyBuilder{cluster}

	isPath := builder.idealStates() + "/" + resource

	if exists, _, err := adm.zkClient.Exists(isPath); !exists || err != nil {
		if !exists {
			return ErrResourceNotExists
		}

		return err
	}

	// TODO: set the value at leaf node instead of the record level
	adm.zkClient.UpdateSimpleField(isPath, "HELIX_ENABLED", "false")

	return nil
}

// ListClusterInfo shows the existing resources and instances in the glaster
func (adm Admin) ListClusterInfo(cluster string) (string, error) {
	// make sure the cluster is already setup
	if ok, err := adm.isClusterSetup(cluster); !ok || err != nil {
		return "", ErrClusterNotSetup
	}

	builder := KeyBuilder{cluster}
	isPath := builder.idealStates()
	instancesPath := builder.instances()

	resources, err := adm.zkClient.Children(isPath)
	if err != nil {
		return "", err
	}

	instances, err := adm.zkClient.Children(instancesPath)
	if err != nil {
		return "", err
	}

	var buffer bytes.Buffer
	buffer.WriteString("Existing resources in cluster " + cluster + ":\n")

	for _, r := range resources {
		buffer.WriteString("  " + r + "\n")
	}

	buffer.WriteString("\nInstances in cluster " + cluster + ":\n")
	for _, i := range instances {
		buffer.WriteString("  " + i + "\n")
	}
	return buffer.String(), nil
}

// ListClusters shows all Helix managed clusters in the connected zookeeper cluster
func (adm Admin) ListClusters() (string, error) {
	var clusters []string

	children, err := adm.zkClient.Children("/")
	if err != nil {
		return "", err
	}

	for _, cluster := range children {
		if ok, err := adm.isClusterSetup(cluster); ok && err == nil {
			clusters = append(clusters, cluster)
		}
	}

	var buffer bytes.Buffer
	buffer.WriteString("Existing clusters: \n")

	for _, cluster := range clusters {
		buffer.WriteString("  " + cluster + "\n")
	}
	return buffer.String(), nil
}

// ListResources shows a list of resources managed by the helix cluster
func (adm Admin) ListResources(cluster string) (string, error) {
	// make sure the cluster is already setup
	if ok, err := adm.isClusterSetup(cluster); !ok || err != nil {
		return "", ErrClusterNotSetup
	}

	builder := KeyBuilder{cluster}
	isPath := builder.idealStates()
	resources, err := adm.zkClient.Children(isPath)
	if err != nil {
		return "", err
	}

	var buffer bytes.Buffer
	buffer.WriteString("Existing resources in cluster " + cluster + ":\n")

	for _, r := range resources {
		buffer.WriteString("  " + r + "\n")
	}

	return buffer.String(), nil
}

// ListInstances shows a list of instances participating the cluster.
func (adm Admin) ListInstances(cluster string) (string, error) {
	// make sure the cluster is already setup
	if ok, err := adm.isClusterSetup(cluster); !ok || err != nil {
		return "", ErrClusterNotSetup
	}

	builder := KeyBuilder{cluster}
	isPath := builder.instances()
	instances, err := adm.zkClient.Children(isPath)
	if err != nil {
		return "", err
	}

	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("Existing instances in cluster %s:\n", cluster))

	for _, r := range instances {
		buffer.WriteString("  " + r + "\n")
	}

	return buffer.String(), nil
}

// ListInstanceInfo shows detailed information of an inspace in the helix cluster
func (adm Admin) ListInstanceInfo(cluster string, instance string) (string, error) {
	builder := &KeyBuilder{cluster}
	instanceCfg := builder.participantConfig(instance)

	accessor, err := adm.createDataAccessorForPath(cluster, instanceCfg, builder)
	if err != nil {
		return "", err
	}

	r, err := accessor.Msg(instanceCfg)
	if err != nil {
		return "", err
	}
	return r.String(), nil
}

// ListIdealState shows a list of ideal states for the cluster resource
func (adm Admin) ListIdealState(cluster string, resource string) (*model.IdealState, error) {
	builder := &KeyBuilder{cluster}
	path := builder.idealStateForResource(resource)

	accessor, err := adm.createDataAccessorForPath(cluster, path, builder)
	if err != nil {
		return nil, err
	}

	return accessor.IdealState(resource)
}

// ListExternalView shows the externalviews for the cluster resource
func (adm Admin) ListExternalView(cluster string, resource string) (*model.ExternalView, error) {
	builder := &KeyBuilder{cluster}
	path := builder.externalViewForResource(resource)

	accessor, err := adm.createDataAccessorForPath(cluster, path, builder)
	if err != nil {
		return nil, err
	}

	return accessor.ExternalView(resource)
}

// GetInstances prints out lists of instances
func (adm Admin) GetInstances(cluster string) error {
	kb := KeyBuilder{cluster}
	instancesKey := kb.instances()

	data, _, err := adm.zkClient.Get(instancesKey)
	if err != nil {
		return err
	}

	for _, c := range data {
		fmt.Println(c)
	}

	return nil
}

// DropInstance removes a participating instance from the helix cluster
func (adm Admin) DropInstance(cluster string, instance string) error {
	kb := KeyBuilder{cluster}
	instanceKey := kb.instance(instance)
	err := adm.zkClient.DeleteTree(instanceKey)
	if err != nil {
		return err
	}

	fmt.Printf("/%s/%s deleted from zookeeper.\n", cluster, instance)

	return err
}

func (adm Admin) isClusterSetup(cluster string) (bool, error) {
	keyBuilder := KeyBuilder{cluster}

	return adm.zkClient.ExistsAll(
		keyBuilder.cluster(),
		keyBuilder.idealStates(),
		keyBuilder.participantConfigs(),
		keyBuilder.propertyStore(),
		keyBuilder.liveInstances(),
		keyBuilder.instances(),
		keyBuilder.externalView(),
		keyBuilder.stateModelDefs(),
	)
}

// createDataAccessorForPath checks that the cluster and path is valid to create a DataAcessor
func (adm Admin) createDataAccessorForPath(
	cluster string,
	path string,
	builder *KeyBuilder) (*DataAccessor, error) {
	// make sure the cluster is already setup
	if ok, err := adm.isClusterSetup(cluster); !ok || err != nil {
		return nil, ErrClusterNotSetup
	}

	if exists, _, err := adm.zkClient.Exists(path); !exists || err != nil {
		if !exists {
			return nil, ErrNodeNotExist
		}
		return nil, err
	}

	return newDataAccessor(adm.zkClient, builder), nil
}
