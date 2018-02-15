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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/uber-go/go-helix/model"
	"github.com/uber-go/go-helix/util"
	uzk "github.com/uber-go/go-helix/zk"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	_allowParticipantAutoJoinKey = "allowParticipantAutoJoin"

	_createLiveInstanceBackoff = 5 * time.Second
)

var (
	errMsgMissingStateModelDef = errors.New(
		"helix participant: messsage is missing state model processor definition")
	errMsgMissingFromOrToState = errors.New(
		"helix participant: missing participant state transition info")
	errMismatchState = errors.New(
		"helix participant: from state in transition message is unexpected")
)

// Participant is the Helix participant
type Participant interface {
	Connect() error
	Disconnect()
	IsConnected() bool
	RegisterStateModel(stateModelName string, processor *StateModelProcessor)
	DataAccessor() *DataAccessor
	InstanceName() string
	Process(e zk.Event)
}

type participant struct {
	logger zap.Logger
	scope  tally.Scope

	clusterName  string
	instanceName string
	host         string
	port         int32

	keyBuilder *KeyBuilder
	zkClient   *uzk.Client
	// Mirrors org.apache.helix.participant.HelixStateMachineEngine
	// stateModelName->stateModelProcessor
	stateModelProcessors     sync.Map
	stateModelProcessorLocks map[string]*sync.Mutex
	stateModel               StateModel
	sync.Mutex
	dataAccessor *DataAccessor

	// fatalErrChan would notify user when a fatal error occurs
	fatalErrChan chan error
}

// NewParticipant instantiates a Participant,
// when an error is sent from the error chan, it means participant sees nonrecoverable errors
// user is expected to clean up and restart the program
func NewParticipant(
	logger *zap.Logger,
	scope tally.Scope,
	zkConnectString string,
	application string,
	clusterName string,
	resourceName string,
	host string,
	port int32,
) (Participant, <-chan error) {
	zkClient := uzk.NewClient(logger, scope, uzk.WithZkSvr(zkConnectString),
		uzk.WithSessionTimeout(uzk.DefaultSessionTimeout))
	keyBuilder := &KeyBuilder{clusterName}
	instanceName := getInstanceName(host, port)
	fatalErrChan := make(chan error)
	return &participant{
		logger: *logger.With(
			zap.String("application", application),
			zap.String("cluster", clusterName),
			zap.String("resource", resourceName),
			zap.String("instance", instanceName),
		),
		scope: scope.SubScope("helix.participant").Tagged(map[string]string{
			"application": application,
			"cluster":     clusterName,
			"resource":    resourceName,
			"instance":    instanceName,
		}),
		clusterName:              clusterName,
		instanceName:             instanceName,
		host:                     host,
		port:                     port,
		keyBuilder:               keyBuilder,
		zkClient:                 zkClient,
		stateModelProcessorLocks: make(map[string]*sync.Mutex),
		dataAccessor:             newDataAccessor(zkClient, keyBuilder),
		stateModel:               NewStateModel(),
		fatalErrChan:             fatalErrChan,
	}, fatalErrChan
}

// Connect let the participant connect to Zookeeper
func (p *participant) Connect() error {
	if p.zkClient.IsConnected() {
		return nil
	}

	err := p.createClient()
	if err != nil {
		return errors.Wrap(err, "helix participant")
	}
	p.zkClient.AddWatcher(p)
	return nil
}

// Disconnect let the participant disconnect from Zookeeper
func (p *participant) Disconnect() {
	if !p.IsConnected() {
		p.logger.Warn("helix instance already isDisconnected")
		return
	}
	p.zkClient.Disconnect()
}

// IsConnected checks if the participant is connected to Zookeeper
func (p *participant) IsConnected() bool {
	return p.zkClient.IsConnected()
}

// RegisterStateModel associates state trasition functions with the participant
func (p *participant) RegisterStateModel(stateModelName string, processor *StateModelProcessor) {
	p.stateModelProcessors.Store(stateModelName, processor)
	p.stateModelProcessorLocks[stateModelName] = &sync.Mutex{}
}

// DataAccessor returns the underlying accessor to help change Zookeeper data
func (p *participant) DataAccessor() *DataAccessor {
	return p.dataAccessor
}

// InstanceName returns the instance name of the participant
func (p *participant) InstanceName() string {
	return p.instanceName
}

// isClusterSetup ensures the Helix cluster is set up
// mirrors org.apache.helix.manager.zk.ZKUtil#isClusterSetup
func (p *participant) isClusterSetup() (bool, error) {
	return p.zkClient.ExistsAll(
		p.keyBuilder.cluster(),
		p.keyBuilder.controller(),
		p.keyBuilder.controllerMessages(),
		p.keyBuilder.controllerErrors(),
		p.keyBuilder.controllerStatusUpdates(),
		p.keyBuilder.controllerHistory(),
		p.keyBuilder.idealStates(),
		p.keyBuilder.participantConfigs(),
		p.keyBuilder.propertyStore(),
		p.keyBuilder.resourceConfigs(),
		p.keyBuilder.liveInstances(),
		p.keyBuilder.instances(),
		p.keyBuilder.externalView(),
		p.keyBuilder.stateModelDefs(),
	)
}

func (p *participant) createLiveInstance() error {
	p.logger.Info("start to create live instance")
	path := p.keyBuilder.liveInstance(p.instanceName)
	node := model.NewLiveInstance(p.instanceName, p.zkClient.GetSessionID())
	data, err := node.Marshal()
	if err != nil {
		return err
	}

	err = p.zkClient.Create(path, data, uzk.FlagsEphemeral, uzk.ACLPermAll)
	if err == nil {
		return nil
	}
	// TODO(yulun): re-visit if the infinite loop if ErrNodeExists
	// in Java is necessary
	if err == zk.ErrNodeExists {
		// wait for previous session to time out
		time.Sleep(uzk.DefaultSessionTimeout + _createLiveInstanceBackoff)
		err = p.zkClient.Create(path, data, uzk.FlagsEphemeral, uzk.ACLPermAll)
	}
	return err
}

func (p *participant) createClient() error {
	// TODO: refactor out the retry count
	for retryCount := 0; retryCount < 3; {
		err := p.zkClient.Connect()
		if err == nil {
			err = p.handleNewSession()
			if err == nil {
				break
			}
		}
		retryCount++
		if retryCount == 3 {
			p.Disconnect()
			return err
		}
		p.logger.Warn("failed to connect to zookeeper, will retry", zap.Error(err))
	}
	return nil
}

// Process can be called from a separate goroutine
func (p *participant) Process(e zk.Event) {
	switch e.State {
	case zk.StateHasSession:
		p.logger.Info("zookeeper session created", zap.String("sessionID", p.zkClient.GetSessionID()))
		if err := p.handleNewSession(); err != nil {
			p.logger.Error("handle new session failed", zap.Error(err))
			// handleNewSession() error is fatal, inform user to clean up and restart
			p.sendFatalError(err)
		} else {
			p.logger.Info("handle new session succeed")
		}
	case zk.StateExpired:
		p.logger.Warn("zookeeper session expired", zap.String("sessionID", p.zkClient.GetSessionID()))
	}
}

func (p *participant) sendFatalError(err error) {
	p.scope.Counter("fatal-errors").Inc(1)
	p.fatalErrChan <- err
}

func (p *participant) handleNewSession() error {
	isSetup, err := p.isClusterSetup()
	if err != nil {
		return err
	} else if !isSetup {
		return errors.Errorf("helix cluster %v not set up", p.clusterName)
	}
	err = p.joinCluster()
	if err != nil {
		return err
	}
	err = p.createLiveInstance()
	if err != nil {
		return err
	}
	err = p.carryOverPreviousCurrentState()
	if err != nil {
		return err
	}
	p.setupMsgHandler()
	return nil
}

func (p *participant) joinCluster() error {
	// check if instance is set up
	participantConfigKey := p.keyBuilder.participantConfig(p.instanceName)
	isInstanceSetup, _, err := p.zkClient.Exists(participantConfigKey)
	if err != nil {
		return err
	} else if isInstanceSetup {
		return nil
	}

	// check if instance can auto join cluster
	clusterConfigKey := p.keyBuilder.clusterConfig()
	config, _, err := p.zkClient.Get(clusterConfigKey)
	if err != nil {
		return err
	}

	c, err := model.NewRecordFromBytes(config)
	if err != nil {
		return err
	}

	allowed := c.GetBooleanField(_allowParticipantAutoJoinKey, false)
	if !allowed {
		return errors.Errorf("cluster %v does not allow auto join", p.clusterName)
	}

	instanceConfig := model.NewInstanceConfig(p.instanceName)
	instanceConfig.SetHost(p.host)
	instanceConfig.SetPort(int(p.port))
	instanceConfig.SetEnabled(true)

	err = p.dataAccessor.createInstanceConfig(participantConfigKey, instanceConfig)
	if err != nil {
		return errors.Wrap(err, "failed to create participant config when joinCluster")
	}

	instance := p.keyBuilder.instance(p.instanceName)
	err = p.zkClient.CreateEmptyNode(instance)
	if err != nil {
		return errors.Wrap(err, "failed to create instance when joinCluster")
	}

	currentStates := p.keyBuilder.currentStates(p.instanceName)
	err = p.zkClient.CreateEmptyNode(currentStates)
	if err != nil {
		return errors.Wrap(err, "failed to create participant current states when joinCluster")
	}

	errs := p.keyBuilder.errorsR(p.instanceName)
	err = p.zkClient.CreateEmptyNode(errs)
	if err != nil {
		return errors.Wrap(err, "failed to create errors node when joinCluster")
	}

	health := p.keyBuilder.healthReport(p.instanceName)
	err = p.zkClient.CreateEmptyNode(health)
	if err != nil {
		return errors.Wrap(err, "failed to create participant health report when joinCluster")
	}

	messages := p.keyBuilder.participantMessages(p.instanceName)
	err = p.zkClient.CreateEmptyNode(messages)
	if err != nil {
		return errors.Wrap(err, "failed to create participant messages when joinCluster")
	}

	updates := p.keyBuilder.statusUpdates(p.instanceName)
	err = p.zkClient.CreateEmptyNode(updates)
	if err != nil {
		return errors.Wrap(err, "failed to create participant statusUpdate when joinCluster")
	}

	return nil
}

func (p *participant) setupMsgHandler() {
	msgCh, errCh, stopCh := p.watchMessages()
	go p.handleMessages(msgCh, errCh, stopCh)
}

func (p *participant) watchMessages() (chan []string, chan error, chan struct{}) {
	msgCh := make(chan []string)
	errCh := make(chan error)
	stopCh := make(chan struct{})
	path := p.keyBuilder.participantMessages(p.instanceName)

	go func() {
		for {
			msgIDs, eventCh, err := p.zkClient.ChildrenW(path)
			if err != nil {
				errCh <- err
				continue
			}
			msgCh <- msgIDs
			select {
			case ev, ok := <-eventCh:
				// eventCh closed after watcher is triggered, recreate the watcher
				if !ok {
					continue
				}
				if ev.Err != nil {
					// ev.Err is non-nil when session expires or zkClient is closed.
					// In either case goroutines spawned in setupMsgHandler should be stopped.
					// Otherwise the goroutines would leak, when a new session is created after expiration
					p.logger.Error("watchMessages has watcher error, stopping message watcher", zap.Error(ev.Err))
					close(stopCh)
					return
				}
				switch ev.Type {
				case zk.EventNodeChildrenChanged:
					p.logger.Info("changes in messages detected. rewatch", zap.Any("event", ev))
					continue
				}
				p.logger.Warn("unexpected messages watcher event", zap.Any("event", ev))
			}
		}
	}()
	return msgCh, errCh, stopCh
}

func (p *participant) handleMessages(
	msgCh <-chan []string, errCh <-chan error, stopCh <-chan struct{}) {
	for {
		select {
		case msgIDs := <-msgCh:
			p.logger.Info("messages watchers received notification", zap.Any("msgIDs", msgIDs))
			messages, err := p.getMessages(msgIDs)
			if err != nil {
				p.logger.Error("participant failed to fetch messages", zap.Error(err))
				break
			}
			p.processMessages(messages)
		case err, ok := <-errCh:
			if !ok {
				break
			}
			p.logger.Error("participant message processor has an error after retries", zap.Error(err))
			p.sendFatalError(err)
			// TODO(yulun): allow services to pass in callback func to handle conn failures
		case <-stopCh:
			p.logger.Info("stop handling messages")
			return
		}
	}
}

func (p *participant) handleMsg(msg *model.Message) error {
	// locking mirrors org.apache.helix.messaging.handling.HelixStateTransitionHandler#handleMessage
	// in Java synchronized on _stateModel
	mu, ok := p.stateModelProcessorLocks[msg.GetStateModelDef()]
	if !ok {
		p.logger.Error("failed to find state model in stateModelProcessorLocks",
			zap.String("StateModelDefinition", msg.GetStateModelDef()),
			zap.Any("stateModelProcessorLocks", p.stateModelProcessorLocks))
		return errMsgMissingStateModelDef
	}
	mu.Lock()
	defer mu.Unlock()

	handleMsgErr := p.preHandleMsg(msg)
	if handleMsgErr == nil {
		handleMsgErr = p.handleStateTransition(msg)
	}
	// TODO: should the message be deleted from ZK after successful processing?
	// https://github.com/yichen/gohelix/blob/master/participant.go#L364
	p.postHandleMsg(msg, handleMsgErr)

	// similar to HelixTask#call(), delete message even if handling was not successful
	if msg.GetParentMsgID() == "" {
		msgPath := p.keyBuilder.participantMsg(p.instanceName, msg.ID)
		p.logger.Info("deleting message at path", zap.String("msgPath", msgPath))
		err := p.zkClient.DeleteTree(msgPath)
		if err != nil {
			p.logger.Error("failed to delete msg after handling", zap.Error(err))
		}
		// TODO(yulun): send reply msg to controller
	}

	// return error although the caller might not respond, helpful at least for unit tests
	// TODO: what about the error from post handling?
	return handleMsgErr
}

func (p *participant) preHandleMsg(msg *model.Message) error {
	//TODO: verify msg is valid
	fromState := msg.GetFromState()
	partitionName, _ := msg.GetPartitionName()
	stateModelDef, err := p.dataAccessor.StateModelDef(msg.GetStateModelDef())
	if err != nil {
		return err
	}
	// if there is no local state for the resource/partition, set it to init state
	localState, exist := p.stateModel.GetState(msg.GetResourceName(), partitionName)
	if !exist {
		initialState := stateModelDef.GetInitialState()
		localState = initialState
		p.stateModel.UpdateState(msg.GetResourceName(), partitionName, initialState)
	}
	if fromState != "" && fromState != "*" &&
		strings.ToLower(fromState) != strings.ToLower(localState) {
		p.logger.Warn("get unexpected fromState when handling transition",
			zap.String("expected", localState),
			zap.String("actual", fromState),
			zap.String("partition", partitionName),
		)
		return errMismatchState
	}
	return nil
}

func (p *participant) postHandleMsg(msg *model.Message, handleMsgErr error) {
	// sessionID might change when we update the state model
	// skip if we are handling an expired session
	sessionID := p.zkClient.GetSessionID()

	if msg.GetTargetSessionID() != sessionID {
		p.logger.Info("session has changed, skip postHandleMsg",
			zap.String("targetSessionID", msg.GetTargetSessionID()),
			zap.String("sessionID", sessionID),
			zap.Any("helixMsg", msg))
		return
	}

	var targetState string
	partitionName, _ := msg.GetPartitionName()
	if handleMsgErr == nil {
		// if the target state is DROPPED, we need to remove the partition name
		// from the current state of the instance because the partition is dropped.
		// In the state model it will stay as OFFLINE, which is OK.
		if strings.ToUpper(msg.GetToState()) == StateModelStateDropped {
			currentStateForResourcePath := p.keyBuilder.currentStateForResource(
				p.instanceName, sessionID, msg.GetResourceName())
			err := p.zkClient.RemoveMapFieldKey(currentStateForResourcePath, partitionName)
			if err != nil {
				p.logger.Error("error removing dropped partition", zap.Error(err))
			} else {
				// update local state only after zk is successfully updated
				p.stateModel.RemoveState(msg.GetResourceName(), partitionName)
			}
			return
		}
		targetState = msg.GetToState()
	} else if handleMsgErr == errMismatchState {
		targetState, _ = p.stateModel.GetState(msg.GetResourceName(), partitionName)
	} else {
		targetState = "ERROR"
		p.logger.Error("error handling msg", zap.Error(handleMsgErr))
	}
	// actually set the current state
	currentStateForResourcePath := p.keyBuilder.currentStateForResource(p.instanceName,
		sessionID, msg.GetResourceName())

	err := p.zkClient.UpdateMapField(currentStateForResourcePath, partitionName,
		model.FieldKeyCurrentState, targetState)
	if err != nil {
		p.logger.Error("failed to update current state in postHandleMsg", zap.Error(err))
	} else {
		// update local state only after zk is successfully updated
		p.stateModel.UpdateState(msg.GetResourceName(), partitionName, targetState)
	}
}

func (p *participant) handleStateTransition(msg *model.Message) error {
	fromState := msg.GetFromState()
	toState := msg.GetToState()
	if fromState == "" || toState == "" {
		p.logger.Info("missing participant state transition info", zap.Any("helixMsg", msg))
		return errMsgMissingFromOrToState
	}

	// set the msg execution time
	msg.SetExecuteStartTime(time.Now())

	if val, ok := p.stateModelProcessors.Load(msg.GetStateModelDef()); ok {
		processor := val.(*StateModelProcessor)
		if toStateHandler, ok := processor.Transitions[fromState]; ok {
			if handler, ok := toStateHandler[toState]; ok {
				// TODO: deal with handler error
				handler(msg)
				return nil
			}
			return errors.Errorf("handler for to state %v not found", toState)
		}
		return errors.Errorf("handlers for from state %v not found", fromState)
	}
	return errors.Errorf("handler from state %v to state %v not found", fromState, toState)
}

func (p *participant) getCurrentResourceNames() []string {
	sessionID := p.zkClient.GetSessionID()
	return p.getCurrentResourceNamesForSession(sessionID)
}

func (p *participant) getCurrentResourceNamesForSession(sessionID string) []string {
	currentResourcesPath := p.keyBuilder.currentStatesForSession(p.instanceName, sessionID)
	resources, err := p.zkClient.Children(currentResourcesPath)
	if err != nil && errors.Cause(err) != zk.ErrNoNode {
		p.logger.Error("failed to get resources of CURRENT_STATES", zap.Error(err))
	}
	return resources
}

func (p *participant) processMessages(messages []*model.Message) {
	sessionID := p.zkClient.GetSessionID()
	var messagesToHandle []*model.Message
	var msgPathsToUpdate []string
	var messagesToUpdate []*model.Message
	pathToCurrentStateToUpdate := map[string]*model.CurrentState{}
	currentResourceNames := util.NewStringSet(p.getCurrentResourceNames()...)
	for _, msg := range messages {
		msgPath := p.keyBuilder.participantMsg(p.instanceName, msg.ID)
		if msg.GetMsgType() == MsgTypeNoop {
			p.logger.Info("dropping NO-OP message", zap.Any("helixMsg", msg))
			err := p.zkClient.DeleteTree(msgPath)
			if err != nil {
				p.logger.Error("failed to delete no-op msg",
					zap.Any("helixMsg", msg),
					zap.Error(err))
			}
			continue
		}

		// prepare for message state update
		targetSessionID := msg.GetTargetSessionID()
		// targetSessionID can be "*" in the following cases
		// 1. All reply messages have wildcard targetSessionID, as in HelixTask.java
		// 2. When the participant calls syncSessionToController in HelixTaskExecutor,
		//    targetSessionID is "*" if sessionID doesn't match targetSessionID
		//    although in this case, the target is the controller
		// 3. This is an option for custom controller to use if needed.
		if targetSessionID != sessionID && targetSessionID != "*" {
			p.logger.Warn("sessionID doesn't match targetSessionID",
				zap.String("sessionID", sessionID), zap.String("targetSessionID", targetSessionID))
			err := p.zkClient.DeleteTree(msgPath)
			if err != nil {
				p.logger.Error("failed to delete message with mismatching sessionID",
					zap.Any("helixMsg", msg), zap.Error(err))
			}
			continue
		}
		if msg.GetMsgState() != model.MessageStateNew {
			continue
		}
		// TODO(yulun): T1270781 will change messagesToHandle to store handler types
		messagesToHandle = append(messagesToHandle, msg)
		msg.SetMsgState(model.MessageStateRead)
		msgPathsToUpdate = append(msgPathsToUpdate, msgPath)
		messagesToUpdate = append(messagesToUpdate, msg)
		// if CurrentState for the resource has been created, avoid creating again
		resourceCreated := currentResourceNames.Contains(msg.GetResourceName())
		_, resourceToCreate := pathToCurrentStateToUpdate[msg.GetResourceName()]

		if !resourceCreated && !resourceToCreate &&
			// we would ideally check if the target is the instanceName of the participant,
			// but we decide to do a less granular check of the message target to resemble with
			// logic in Helix Java
			!strings.EqualFold(msg.GetTargetName(), TargetController) &&
			strings.EqualFold(msg.GetMsgType(), MsgTypeStateTransition) {
			currentState := model.NewCurrentStateFromMsg(msg, msg.GetResourceName(), sessionID)
			path := p.keyBuilder.currentStateForResource(p.instanceName, sessionID, msg.GetResourceName())
			pathToCurrentStateToUpdate[path] = currentState
		}
	}
	// only log errors to mirror Helix Java
	for path, currentStateRecord := range pathToCurrentStateToUpdate {
		err := p.dataAccessor.createCurrentState(path, currentStateRecord)
		if err != nil {
			p.logger.Error("failed to create current state for msg",
				zap.String("path", path), zap.Error(err))
			continue
		}
		p.logger.Info("created current state for msg", zap.String("path", path))
	}
	for i := 0; i < len(msgPathsToUpdate); i++ {
		path := msgPathsToUpdate[i]
		msg := messagesToUpdate[i]

		// update message state after current state is updated
		// mirrors Helix Java which doesn't explicitly handle failures and allows
		// messages to be processed more than once (which relies on the rebalancer to heal)
		err := p.dataAccessor.setMsg(path, msg)
		if err != nil {
			p.logger.Error("failed to update msg to read",
				zap.Any("helixMsg", msg),
				zap.Error(err))
		}
	}
	// only start processing when all messages are marked read
	// mirrors logic in org.apache.helix.manager.zk.CallbackHandler#CallbackInvoker
	for _, msg := range messagesToHandle {
		go p.handleMsg(msg)
	}
}

func (p *participant) carryOverPreviousCurrentState() error {
	currentStatesPath := p.keyBuilder.currentStates(p.instanceName)

	sessionIDs, err := p.zkClient.Children(currentStatesPath)
	if err != nil {
		p.logger.Error("failed to get previous currentStates", zap.Error(err))
		return err
	}

	for _, sessionID := range sessionIDs {
		if sessionID == p.zkClient.GetSessionID() {
			continue
		}
		if err := p.carryOverPreviousCurrentStateFromSession(sessionID); err != nil {
			p.logger.Error("failed to carry over previous state",
				zap.Error(err), zap.String("session", sessionID))
			return err
		}
	}

	// remove previous currentStates after all of them have been updated
	for _, sessionID := range sessionIDs {
		if sessionID == p.zkClient.GetSessionID() {
			continue
		}
		path := currentStatesPath + "/" + sessionID
		p.logger.Info("removing current states from previous sessionIDs.", zap.String("path", path))
		err = p.zkClient.DeleteTree(path)
		if err != nil && errors.Cause(err) != zk.ErrNoNode {
			p.logger.Error("failed to remove previous state", zap.Error(err))
			return err
		}
	}
	return nil
}

func (p *participant) carryOverPreviousCurrentStateFromSession(sessionID string) error {
	for _, resource := range p.getCurrentResourceNamesForSession(sessionID) {
		p.logger.Info("carry over from old session",
			zap.String("oldSession", sessionID),
			zap.String("currentSession", p.zkClient.GetSessionID()),
			zap.String("resource", resource))
		lastCurState, err := p.dataAccessor.CurrentState(p.instanceName, sessionID, resource)
		if err != nil {
			return err
		}
		stateModelDefString := lastCurState.GetStateModelDef()
		if stateModelDefString == "" {
			p.logger.Error("skip carry over as previous current state doesn't have state model definition",
				zap.String("oldSession", sessionID),
				zap.String("resource", resource))
			continue
		}
		stateModelDef, err := p.dataAccessor.StateModelDef(stateModelDefString)
		if err != nil {
			return err
		}

		initialState := stateModelDef.GetInitialState()
		currentStatePath := p.dataAccessor.keyBuilder.currentStateForResource(
			p.instanceName, p.zkClient.GetSessionID(), resource)

		err = p.dataAccessor.updateData(currentStatePath,
			func(currentStateData *model.ZNRecord) (*model.ZNRecord, error) {
				var currentState *model.CurrentState
				if currentStateData == nil {
					currentState = &model.CurrentState{ZNRecord: *model.NewRecord(resource)}
					currentState.ZNRecord.SimpleFields = lastCurState.SimpleFields
					currentState.SetSessionID(p.zkClient.GetSessionID())
				} else {
					currentState = &model.CurrentState{ZNRecord: *currentStateData}
				}

				for partition := range lastCurState.GetPartitionStateMap() {
					// carry over only when current state not exist
					if currentState.GetState(partition) == "" {
						currentState.SetState(partition, initialState)
					}
				}
				return &currentState.ZNRecord, nil
			})
		if err != nil {
			return err
		}
	}
	return nil
}

// getMessages returns all message for participant
func (p *participant) getMessages(msgIDs []string) ([]*model.Message, error) {
	res := make([]*model.Message, 0, len(msgIDs))
	for i := 0; i < len(msgIDs); i++ {
		path := p.keyBuilder.participantMsg(p.instanceName, msgIDs[i])
		msg, err := p.dataAccessor.Msg(path)
		switch errors.Cause(err) {
		case nil:
			res = append(res, msg)
		case zk.ErrNoNode:
			// The msg is deleted and can be ignored
			continue
		default:
			return nil, err
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].GetCreateTimestamp() < res[j].GetCreateTimestamp()
	})
	return res, nil
}

func getInstanceName(host string, port int32) string {
	return fmt.Sprintf("%s_%d", host, port)
}
