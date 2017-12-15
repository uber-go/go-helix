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
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
)

type connInfo struct {
	state   zk.State
	eventCh chan zk.Event
}

// FakeZk provides utility to make fake connections and manipulate connection states
type FakeZk struct {
	connToConnInfo map[Connection]*connInfo
	opChan         chan interface{}

	defaultConnectionState zk.State
}

// FakeZkOption is the optional arg to create a FakeZk
type FakeZkOption func(*FakeZk)

// DefaultConnectionState sets the default state when a fake connection is made
func DefaultConnectionState(state zk.State) FakeZkOption {
	return func(zk *FakeZk) {
		zk.defaultConnectionState = state
	}
}

// NewFakeZk creates new FakeZk test utility
func NewFakeZk(opts ...FakeZkOption) *FakeZk {
	z := &FakeZk{
		connToConnInfo:         map[Connection]*connInfo{},
		opChan:                 make(chan interface{}, 1),
		defaultConnectionState: zk.StateDisconnected,
	}
	for _, opt := range opts {
		opt(z)
	}
	go z.run()
	return z
}

// NewConn makes new fake ZK connection
func (z *FakeZk) NewConn() (Connection, <-chan zk.Event, error) {
	respCh := make(chan makeConnResp, 1)
	z.opChan <- makeConnReq{
		c: respCh,
	}
	resp := <-respCh
	return resp.conn, resp.eventCh, resp.err
}

// GetState returns state by ZK connection
func (z *FakeZk) GetState(conn Connection) zk.State {
	respCh := make(chan zk.State, 1)
	z.opChan <- getConnStateReq{
		c:    respCh,
		conn: conn,
	}
	return <-respCh
}

// SetState sets state of ZK connection
func (z *FakeZk) SetState(conn Connection, state zk.State) {
	respCh := make(chan struct{}, 1)
	z.opChan <- setConnStateReq{
		eType: zk.EventSession,
		state: state,
		c:     respCh,
		conn:  conn,
	}
	<-respCh
}

// GetConnections returns all of the connections FakeZk has made
func (z *FakeZk) GetConnections() []*FakeZkConn {
	var result []*FakeZkConn
	for connection := range z.connToConnInfo {
		result = append(result, connection.(*FakeZkConn))
	}
	return result
}

func (z *FakeZk) run() {
	for op := range z.opChan {
		z.performOp(op)
	}
}

func (z *FakeZk) stop() {
	close(z.opChan)
}

func (z *FakeZk) performOp(op interface{}) {
	switch op := op.(type) {
	case makeConnReq:
		z.makeConn(op)
	case setConnStateReq:
		z.setConnState(op)
	case getConnStateReq:
		z.getConnState(op)
	default:
		panic(fmt.Sprintf("fake zk received unknown op %v", op))
	}
}

func (z *FakeZk) makeConn(op makeConnReq) {
	eventCh := make(chan zk.Event)
	conn := NewFakeZkConn(z)
	z.connToConnInfo[conn] = &connInfo{
		state:   z.defaultConnectionState,
		eventCh: eventCh,
	}
	resp := makeConnResp{
		conn:    conn,
		eventCh: eventCh,
		err:     nil,
	}
	op.c <- resp
}

func (z *FakeZk) getConnState(op getConnStateReq) {
	connInfo, ok := z.connToConnInfo[op.conn]
	if !ok {
		panic(fmt.Sprintf("fake zk has no connection for op %+v", op))
	}
	op.c <- connInfo.state
}

func (z *FakeZk) setConnState(op setConnStateReq) {
	connInfo, ok := z.connToConnInfo[op.conn]
	if !ok {
		panic(fmt.Sprintf("fake zk has no connection for op %+v", op))
	}
	connInfo.state = op.state
	connInfo.eventCh <- zk.Event{State: op.state, Type: op.eType}
	if op.state == zk.StateExpired {
		op.conn.(*FakeZkConn).invalidateWatchers(zk.ErrSessionExpired)
	}
	op.c <- struct{}{}
}

type makeConnReq struct {
	c chan makeConnResp
}

type makeConnResp struct {
	conn    Connection
	eventCh <-chan zk.Event
	err     error
}

type getConnStateReq struct {
	conn Connection
	c    chan zk.State
}

type setConnStateReq struct {
	eType zk.EventType
	state zk.State
	conn  Connection
	c     chan struct{}
}
