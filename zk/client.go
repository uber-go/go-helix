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

package zk

import (
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/uber-go/go-helix/model"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	// DefaultSessionTimeout is the time for ZK server to expire client session
	// ZK uses 20 times tickTime, which is set to 200 ms
	// in existing ZK cluster to ensure proper detection
	// of Helix node failures
	DefaultSessionTimeout = 4 * time.Second
	// FlagsZero is the default ZK data node flag
	FlagsZero = int32(0)
	// FlagsEphemeral is the ephemeral ZK data node flag
	FlagsEphemeral = int32(zk.FlagEphemeral)

	_defaultRetryTimeout = 1 * time.Minute
)

var (
	// ACLPermAll is the mode of ZK nodes where all users have permission to access
	ACLPermAll = zk.WorldACL(zk.PermAll)
)

var (
	errOpBeforeConnect = errors.New("zookeeper: called connect() before any ops")
)

// Connection is the thread safe interface for ZK connection
type Connection interface {
	AddAuth(scheme string, auth []byte) error
	Children(path string) ([]string, *zk.Stat, error)
	ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error)
	Get(path string) ([]byte, *zk.Stat, error)
	GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)
	Exists(path string) (bool, *zk.Stat, error)
	ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error)
	Set(path string, data []byte, version int32) (*zk.Stat, error)
	Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	Delete(path string, version int32) error
	Multi(ops ...interface{}) ([]zk.MultiResponse, error)
	SessionID() int64
	SetLogger(zk.Logger)
	State() zk.State
	Close()
}

// ConnFactory provides interface that creates ZK connections
type ConnFactory interface {
	NewConn() (Connection, <-chan zk.Event, error)
}

// connFactory creates connections to real/embedded ZK
type connFactory struct {
	zkServers      []string
	sessionTimeout time.Duration
}

// NewConnFactory creates new connFactory
func NewConnFactory(zkServers []string, sessionTimeout time.Duration) ConnFactory {
	return &connFactory{
		zkServers:      zkServers,
		sessionTimeout: sessionTimeout,
	}
}

// NewConn creates new ZK connection to real/embedded ZK
func (f *connFactory) NewConn() (Connection, <-chan zk.Event, error) {
	return zk.Connect(f.zkServers, f.sessionTimeout)
}

// Client wraps utils to communicate with ZK
type Client struct {
	logger *zap.Logger
	scope  tally.Scope

	zkSvr          string
	sessionTimeout time.Duration
	retryTimeout   time.Duration
	connFactory    ConnFactory
	zkConn         Connection
	zkConnMu       *sync.RWMutex
	// coordinates Go routines waiting on ZK connection events
	cond *sync.Cond

	zkEventWatchersMu *sync.RWMutex
	zkEventWatchers   []Watcher
}

// Watcher mirrors org.apache.zookeeper.Watcher
type Watcher interface {
	Process(e zk.Event)
}

// ClientOption provides options or ZK client
type ClientOption func(*Client)

// WithZkSvr configures ZK servers for the client
func WithZkSvr(zkSvr string) ClientOption {
	return func(c *Client) {
		c.zkSvr = zkSvr
	}
}

// WithSessionTimeout configures sessionTimeout
func WithSessionTimeout(t time.Duration) ClientOption {
	return func(c *Client) {
		c.sessionTimeout = t
	}
}

// WithRetryTimeout configures retryTimeout for ZK operations
func WithRetryTimeout(t time.Duration) ClientOption {
	return func(c *Client) {
		c.retryTimeout = t
	}
}

// WithConnFactory configures ConnFactory used to make ZK connections
func WithConnFactory(connFactory ConnFactory) ClientOption {
	return func(c *Client) {
		c.connFactory = connFactory
	}
}

// NewClient returns new ZK client
func NewClient(logger *zap.Logger, scope tally.Scope, options ...ClientOption) *Client {
	mu := &sync.Mutex{}
	c := &Client{
		cond:              sync.NewCond(mu),
		retryTimeout:      _defaultRetryTimeout,
		zkConnMu:          &sync.RWMutex{},
		zkEventWatchersMu: &sync.RWMutex{},
	}
	for _, option := range options {
		option(c)
	}
	c.logger = logger.With(zap.String("zkSvr", c.zkSvr))
	c.scope = scope.SubScope("helix.zk").Tagged(map[string]string{"zkSvr": c.zkSvr})
	if c.connFactory == nil {
		zkServers := strings.Split(strings.TrimSpace(c.zkSvr), ",")
		c.connFactory = NewConnFactory(zkServers, c.sessionTimeout)
	}
	return c
}

// AddWatcher adds a Watcher to zk session event
func (c *Client) AddWatcher(w Watcher) {
	c.zkEventWatchersMu.Lock()
	c.zkEventWatchers = append(c.zkEventWatchers, w)
	c.zkEventWatchersMu.Unlock()
}

// ClearWatchers removes all the watchers the client has
func (c *Client) ClearWatchers() {
	c.zkEventWatchersMu.Lock()
	c.zkEventWatchers = nil
	c.zkEventWatchersMu.Unlock()
}

// Connect sets up ZK connection
func (c *Client) Connect() error {
	zkConn, eventCh, err := c.connFactory.NewConn()
	if err != nil {
		return err
	}
	c.zkConnMu.Lock()
	if c.zkConn != nil {
		c.zkConn.Close()
	}
	c.zkConn = zkConn
	c.zkConnMu.Unlock()
	go c.processEvents(eventCh)
	connected := c.waitUntilConnected(c.sessionTimeout)
	if !connected {
		return errors.New("zookeeper: failed to connect")
	}
	return nil
}

func (c *Client) processEvents(eventCh <-chan zk.Event) {
	for {
		select {
		case ev, ok := <-eventCh:
			if !ok {
				c.logger.Warn("zookeeper has quit, stop processing events")
				return
			}
			switch ev.Type {
			case zk.EventSession:
				c.logger.Info("receive EventSession", zap.Any("state", ev.State))
				c.cond.Broadcast()
				c.processSessionEvents(ev)
			case zk.EventNotWatching:
				c.logger.Info("watchers have been invalidated. zk client should be trying to reconnect")
			default:
				// no-op since node specific events are handled by caller
			}
		}
	}
}

func (c *Client) processSessionEvents(ev zk.Event) {
	c.zkEventWatchersMu.RLock()
	watchers := c.zkEventWatchers
	c.zkEventWatchersMu.RUnlock()
	for _, watcher := range watchers {
		watcher.Process(ev)
	}
}

// IsConnected returns if client is connected to Zookeeper
func (c *Client) IsConnected() bool {
	conn := c.getConn()
	return conn != nil && conn.State() == zk.StateHasSession
}

// GetSessionID returns current ZK session ID
func (c *Client) GetSessionID() string {
	conn := c.getConn()
	if conn == nil {
		return ""
	}
	return strconv.FormatInt(conn.SessionID(), 10)
}

// Disconnect closes ZK connection
func (c *Client) Disconnect() {
	c.ClearWatchers()
	conn := c.getConn()
	if conn != nil {
		conn.Close()
	}
}

// CreateEmptyNode creates an empty node for future use
func (c *Client) CreateEmptyNode(path string) error {
	return c.Create(path, []byte(""), FlagsZero, ACLPermAll)
}

// CreateDataWithPath creates a path with a string
func (c *Client) CreateDataWithPath(p string, data []byte) error {
	parent := path.Dir(p)
	if err := c.ensurePath(parent); err != nil {
		return err
	}
	return c.Create(p, data, FlagsZero, ACLPermAll)
}

// Exists checks if a key exists in ZK
func (c *Client) Exists(path string) (bool, *zk.Stat, error) {
	var res bool
	var stat *zk.Stat
	err := c.retryUntilConnected(func() error {
		r, s, err := c.getConn().Exists(path)
		if err != nil {
			return err
		}
		res = r
		stat = s
		return nil
	})
	return res, stat, errors.Wrapf(err, "zk client failed to check existence of %s", path)
}

// ExistsAll returns if all paths exist
func (c *Client) ExistsAll(paths ...string) (bool, error) {
	for _, path := range paths {
		if exists, _, err := c.Exists(path); err != nil || !exists {
			return false, err
		}
	}
	return true, nil
}

// Get returns data in ZK path
func (c *Client) Get(path string) ([]byte, *zk.Stat, error) {
	var data []byte
	var stat *zk.Stat
	err := c.retryUntilConnected(func() error {
		d, s, err := c.getConn().Get(path)
		if err != nil {
			return err
		}
		data = d
		stat = s
		return nil
	})
	return data, stat, errors.Wrapf(err, "zk client failed to get data at %s", path)
}

// GetW returns data in ZK path and watches path
func (c *Client) GetW(path string) ([]byte, <-chan zk.Event, error) {
	var data []byte
	var events <-chan zk.Event
	err := c.retryUntilConnected(func() error {
		d, _, evts, err := c.getConn().GetW(path)
		if err != nil {
			return err
		}
		data = d
		events = evts
		return nil
	})
	return data, events, errors.Wrapf(err, "zk client failed to get and watch data at %s", path)
}

// Set sets data in ZK path
func (c *Client) Set(path string, data []byte, version int32) error {
	err := c.retryUntilConnected(func() error {
		_, err := c.getConn().Set(path, data, version)
		return err
	})
	return errors.Wrapf(err, "zk client failed to set data at %s", path)
}

// SetWithDefaultVersion sets data with default version, -1
func (c *Client) SetWithDefaultVersion(path string, data []byte) error {
	return c.Set(path, data, -1)
}

// Create creates ZK path with data
func (c *Client) Create(path string, data []byte, flags int32, acl []zk.ACL) error {
	err := c.retryUntilConnected(func() error {
		_, err := c.getConn().Create(path, data, flags, acl)
		return err
	})
	return errors.Wrapf(err, "zk client failed to create data at %s", path)
}

// Children returns children of ZK path
func (c *Client) Children(path string) ([]string, error) {
	var children []string
	err := c.retryUntilConnected(func() error {
		res, _, err := c.getConn().Children(path)
		if err != nil {
			return err
		}
		children = res
		return nil
	})
	return children, errors.Wrapf(err, "zk client failed to get children of %s", path)
}

// ChildrenW gets children and watches path
func (c *Client) ChildrenW(path string) ([]string, <-chan zk.Event, error) {
	children := []string{}
	eventCh := make(<-chan zk.Event)

	err := c.retryUntilConnected(func() error {
		res, _, evts, err := c.getConn().ChildrenW(path)
		if err != nil {
			return err
		}
		children = res
		eventCh = evts
		return nil
	})

	return children, eventCh,
		errors.Wrapf(err, "zk client failed to get and watch children of %s", path)
}

// Delete removes ZK path
func (c *Client) Delete(path string) error {
	err := c.retryUntilConnected(func() error {
		err := c.getConn().Delete(path, -1)
		return err
	})
	return errors.Wrapf(err, "zk client failed to delete node at %s", path)
}

// UpdateMapField updates a map field for path
// key is the top-level key in the MapFields
// mapProperty is the inner key
//
// Example:
//
// mapFields":{
// "partition_1":{
//   "CURRENT_STATE":"OFFLINE",
//   "INFO":""
// }
//
// To set the CURRENT_STATE to ONLINE, use
// UpdateMapField(
//     "/CLUSTER/INSTANCES/{instance}/CURRENT_STATE/{sessionID}/{db}",
//     "partition_1", "CURRENT_STATE", "ONLINE")
func (c *Client) UpdateMapField(path string, key string, property string, value string) error {
	data, stat, err := c.Get(path)
	if err != nil {
		return err
	}

	// convert the result into Message
	node, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	// update the value
	node.SetMapField(key, property, value)

	// marshall to bytes
	data, err = node.Marshal()
	if err != nil {
		return err
	}

	// copy back to zookeeper
	err = c.Set(path, data, stat.Version)
	return err
}

// UpdateSimpleField updates a simple field
func (c *Client) UpdateSimpleField(path string, key string, value string) error {
	data, stat, err := c.Get(path)
	if err != nil {
		return err
	}

	// convert the result into Message
	node, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	// update the value
	node.SetSimpleField(key, value)

	// marshall to bytes
	data, err = node.Marshal()
	if err != nil {
		return err
	}

	// copy back to zookeeper
	err = c.Set(path, data, stat.Version)
	return err
}

// GetSimpleFieldValueByKey returns value in simple field by key
func (c *Client) GetSimpleFieldValueByKey(path string, key string) (string, error) {
	data, stat, err := c.Get(path)
	if err != nil {
		return "", err
	}

	record, err := model.NewRecordFromBytes(data)
	if err != nil {
		return "", err
	}
	record.Version = stat.Version

	v, ok := record.GetSimpleField(key)
	if !ok {
		return "", nil
	}
	return v, nil
}

// DeleteTree removes ZK path and its children
func (c *Client) DeleteTree(path string) error {
	if exists, _, err := c.Exists(path); !exists || err != nil {
		return err
	}

	children, err := c.Children(path)
	if err != nil {
		return err
	}

	if len(children) == 0 {
		err := c.Delete(path)
		return err
	}

	for _, child := range children {
		p := path + "/" + child
		e := c.DeleteTree(p)
		if e != nil {
			return e
		}
	}

	return c.Delete(path)
}

// RemoveMapFieldKey removes a map field by key
func (c *Client) RemoveMapFieldKey(path string, key string) error {
	data, stat, err := c.Get(path)
	if err != nil {
		return err
	}

	node, err := model.NewRecordFromBytes(data)
	if err != nil {
		return err
	}

	node.RemoveMapField(key)

	data, err = node.Marshal()
	if err != nil {
		return err
	}

	// save the data back to zookeeper
	err = c.Set(path, data, stat.Version)
	return err
}

// GetRecordFromPath returns message by ZK path
func (c *Client) GetRecordFromPath(path string) (*model.ZNRecord, error) {
	data, stat, err := c.Get(path)
	if err != nil {
		return nil, err
	}
	record, err := model.NewRecordFromBytes(data)
	if err != nil {
		return nil, err
	}
	record.Version = stat.Version
	return record, nil
}

// SetDataForPath updates data at given ZK path
func (c *Client) SetDataForPath(path string, data []byte, version int32) error {
	return c.Set(path, data, version)
}

// SetRecordForPath sets a record in give ZK path
func (c *Client) SetRecordForPath(path string, r *model.ZNRecord) error {
	version, err := c.getVersionFromPath(path)
	if err != nil {
		return err
	}

	data, err := r.Marshal()
	if err != nil {
		return err
	}
	return c.SetDataForPath(path, data, version)
}

func (c *Client) getConn() Connection {
	c.zkConnMu.RLock()
	defer c.zkConnMu.RUnlock()
	return c.zkConn
}

func (c *Client) getVersionFromPath(p string) (int32, error) {
	exists, s, _ := c.Exists(p)
	if !exists {
		version := int32(0)
		err := c.ensurePath(p)
		if err != nil {
			return version, errors.Wrap(err, "failed to get version from path: %v")
		}
		return version, nil
	}
	return s.Version, nil
}

// EnsurePath makes sure the specified path exists.
// If not, create it
func (c *Client) ensurePath(p string) error {
	exists, _, err := c.Exists(p)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	err = c.ensurePath(path.Dir(p))
	if err != nil {
		return err
	}
	return c.CreateEmptyNode(p)
}

// Mirrors org.I0Itec.zkclient.Client#retryUntilConnected
func (c *Client) retryUntilConnected(fn func() error) error {
	conn := c.getConn()
	if conn == nil {
		return errOpBeforeConnect
	}
	startTime := time.Now()
	for {
		if conn.State() == zk.StateDisconnected {
			return errors.New("zookeeper: client disconnected")
		}
		if (time.Since(startTime)) > c.retryTimeout {
			return errors.New("zookeeper: retry has timed out")
		}
		err := fn()
		if err == zk.ErrConnectionClosed || err == zk.ErrSessionExpired {
			c.waitUntilConnected(c.retryTimeout)
			continue
		} else if err != nil {
			return errors.Wrap(err, "zookeeper error shouldn't be retried")
		}
		return nil
	}
}

// waitUntilConnected returns if the client is connected in one of two scenarios
// 1) client is connected within duration t
// 2) client is not connected and duration t is exhausted
// ZooKeeper state change is broadcast by the cond variable of the client,
// note c.cond.L is not locked when Wait first resumes
func (c *Client) waitUntilConnected(t time.Duration) bool {
	startTime := time.Now()
	for !c.IsConnected() {
		if time.Since(startTime) > t {
			return false
		}
		doneCh := make(chan struct{})
		c.cond.L.Lock()
		go func() {
			c.cond.Wait()
			c.cond.L.Unlock()
			close(doneCh)
		}()
		select {
		case <-time.After(t):
		case <-doneCh:
		}
	}
	return true
}
