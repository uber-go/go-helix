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
	"sync"

	"github.com/samuel/go-zookeeper/zk"
)

// FakeZkConn is a fake ZK connection for testing
type FakeZkConn struct {
	pathWatchers
	history *MethodCallHistory
	zk      *FakeZk
}

// NewFakeZkConn creates a FakeZkConn
func NewFakeZkConn(zk *FakeZk) *FakeZkConn {
	return &FakeZkConn{
		zk: zk,
		history: &MethodCallHistory{
			dict: make(map[string][]*MethodCall),
		},
	}
}

// AddAuth addds auth info
func (c *FakeZkConn) AddAuth(scheme string, auth []byte) error {
	c.history.addToHistory("AddAuth", scheme, auth)
	return nil
}

// Children returns children of a path
func (c *FakeZkConn) Children(path string) ([]string, *zk.Stat, error) {
	c.history.addToHistory("Children", path)
	return nil, nil, nil
}

// ChildrenW returns children and watcher channel of a path
func (c *FakeZkConn) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	eventCh := c.addWatcher()
	c.history.addToHistory("ChildrenW", path)
	return nil, nil, eventCh, nil
}

// Get returns node by path
func (c *FakeZkConn) Get(path string) ([]byte, *zk.Stat, error) {
	c.history.addToHistory("Get", path)
	return nil, nil, nil
}

// GetW returns node and watcher channel of path
func (c *FakeZkConn) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	eventCh := c.addWatcher()
	c.history.addToHistory("GetW", path)
	return nil, nil, eventCh, nil
}

// Exists returns if the path exists
func (c *FakeZkConn) Exists(path string) (bool, *zk.Stat, error) {
	c.history.addToHistory("Exists", path)
	return true, nil, nil
}

// ExistsW returns if path exists and watcher chan of path
func (c *FakeZkConn) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	eventCh := c.addWatcher()
	c.history.addToHistory("ExistsW", path)
	return false, nil, eventCh, nil
}

// Set sets data for path
func (c *FakeZkConn) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	c.history.addToHistory("Set", path, data, version)
	return nil, nil
}

// Create creates new ZK node
func (c *FakeZkConn) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	c.history.addToHistory("Create", path, data, flags, acl)
	return "", nil
}

// Delete deletes ZK node
func (c *FakeZkConn) Delete(path string, version int32) error {
	c.history.addToHistory("Delete", path, version)
	return nil
}

// Multi executes multiple ZK operations
func (c *FakeZkConn) Multi(ops ...interface{}) ([]zk.MultiResponse, error) {
	c.history.addToHistory("Multi", ops)
	return nil, nil
}

// SessionID returns session ID
func (c *FakeZkConn) SessionID() int64 {
	c.history.addToHistory("SessionID")
	return int64(0)
}

// SetLogger sets loggeer for the client
func (c *FakeZkConn) SetLogger(l zk.Logger) {
	c.history.addToHistory("SetLogger", l)
}

// State returns state of the client
func (c *FakeZkConn) State() zk.State {
	c.history.addToHistory("State")
	return c.zk.GetState(c)
}

// Close closes the connection to ZK
func (c *FakeZkConn) Close() {
	c.invalidateWatchers(zk.ErrClosing)
	c.history.addToHistory("Close")
}

// GetHistory returns history
func (c *FakeZkConn) GetHistory() *MethodCallHistory {
	return c.history
}

type pathWatchers struct {
	sync.RWMutex
	watchers []chan zk.Event
}

func (p *pathWatchers) addWatcher() chan zk.Event {
	eventCh := make(chan zk.Event)
	p.Lock()
	p.watchers = append(p.watchers, eventCh)
	p.Unlock()
	return eventCh
}

func (p *pathWatchers) invalidateWatchers(err error) {
	ev := zk.Event{Type: zk.EventNotWatching, State: zk.StateDisconnected, Err: err}
	p.Lock()
	for _, watcher := range p.watchers {
		watcher <- ev
		close(watcher)
	}
	p.watchers = nil
	p.Unlock()
}

// MethodCall represents a call record
type MethodCall struct {
	MethodName string
	Params     []interface{}
}

// MethodCallHistory represents the history of the method called on the connection
type MethodCallHistory struct {
	sync.RWMutex
	history []*MethodCall
	dict    map[string][]*MethodCall
}

func (h *MethodCallHistory) addToHistory(method string, params ...interface{}) {
	methodCall := &MethodCall{method, params}
	h.Lock()
	h.history = append(h.history, methodCall)
	h.dict[method] = append(h.dict[method], methodCall)
	h.Unlock()
}

// GetHistory returns all of the histories
func (h *MethodCallHistory) GetHistory() []*MethodCall {
	defer h.RUnlock()
	h.RLock()
	return h.history
}

// GetHistoryForMethod returns all of the histories of a method
func (h *MethodCallHistory) GetHistoryForMethod(method string) []*MethodCall {
	defer h.RUnlock()
	h.RLock()
	return h.dict[method]
}
