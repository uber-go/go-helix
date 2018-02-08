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
	"math/rand"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/go-helix/model"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type ZKClientTestSuite struct {
	BaseZkTestSuite

	zkClient *Client
}

type CountEventWatcher struct {
	count int
	sync.RWMutex
}

func (w *CountEventWatcher) Process(e zk.Event) {
	w.Lock()
	defer w.Unlock()
	w.count++
}

func (w *CountEventWatcher) GetCount() int {
	w.RLock()
	defer w.RUnlock()
	return w.count
}

func TestZKClientTestSuite(t *testing.T) {
	s := &ZKClientTestSuite{}
	suite.Run(t, s)
}

func (s *ZKClientTestSuite) SetupTest() {
	s.zkClient = NewClient(zap.NewNop(), tally.NoopScope, WithZkSvr(s.ZkConnectString),
		WithSessionTimeout(DefaultSessionTimeout))
}

func (s *ZKClientTestSuite) TearDownTest() {
	if s.zkClient.IsConnected() {
		s.zkClient.Disconnect()
	}
}

func (s *ZKClientTestSuite) TestEmbeddedZk() {
	err := EnsureZookeeperUp(s.EmbeddedZkPath)
	s.NoError(err)
	err = StopZookeeper(s.EmbeddedZkPath)
	s.NoError(err)
	err = EnsureZookeeperUp(s.EmbeddedZkPath)
	s.NoError(err)
}

func (s *ZKClientTestSuite) TestZKConnectAndDisconnect() {
	s.False(s.zkClient.IsConnected())

	err := s.zkClient.Connect()
	s.NoError(err)
	s.True(s.zkClient.IsConnected())
	s.True(len(s.zkClient.GetSessionID()) > 0)

	s.zkClient.Disconnect()
	// time.Sleep is needed because ZK client disconnects asynchronously,
	// which is in parity with the Java client
	time.Sleep(time.Millisecond)
	s.False(s.zkClient.IsConnected())
}

func (s *ZKClientTestSuite) TestBasicZkOps() {
	testData := fmt.Sprintf("%d", rand.Int())
	testData1 := fmt.Sprintf("%d", rand.Int())
	testPath := fmt.Sprintf("/%d/%d", rand.Int63(), rand.Int63())
	testPath1 := fmt.Sprintf("/%d/%d", rand.Int63(), rand.Int63())

	err := s.zkClient.CreateDataWithPath(testPath, []byte(testData))
	s.Equal(errOpBeforeConnect, errors.Cause(err))
	err = s.zkClient.Connect()
	s.NoError(err)
	s.True(s.zkClient.IsConnected())
	err = s.zkClient.CreateDataWithPath(testPath, []byte(testData))
	s.NoError(err)
	bytes, stat, err := s.zkClient.Get(testPath)
	s.NoError(err)
	s.Equal(testData, string(bytes))
	s.Equal(int32(0), stat.Version)
	err = s.zkClient.SetWithDefaultVersion(testPath, []byte(testData1))
	s.NoError(err)
	bytes, eventCh, err := s.zkClient.GetW(testPath)
	s.NoError(err)
	s.Equal(testData1, string(bytes))
	err = s.zkClient.Set(testPath, []byte(testData), 1)
	ev := <-eventCh
	s.Equal(zk.EventNodeDataChanged, ev.Type)

	parent := path.Dir(testPath)
	paths, eventCh, err := s.zkClient.ChildrenW(parent)
	s.NoError(err)
	s.Equal(1, len(paths))
	s.zkClient.Delete(testPath)
	ev = <-eventCh
	s.Equal(zk.EventNodeChildrenChanged, ev.Type)
	exists, _, err := s.zkClient.Exists(parent)
	s.True(exists)
	err = s.zkClient.CreateDataWithPath(testPath, []byte(testData))
	s.NoError(err)
	err = s.zkClient.DeleteTree(parent)
	s.NoError(err)

	err = s.zkClient.CreateDataWithPath(testPath, []byte(testData))
	s.NoError(err)
	s.False(s.zkClient.ExistsAll(testPath, testPath1))
	err = s.zkClient.CreateDataWithPath(testPath1, []byte(testData))
	s.NoError(err)
	s.True(s.zkClient.ExistsAll(testPath, testPath1))
}

func (s *ZKClientTestSuite) TestHelixRecordOps() {
	path := fmt.Sprintf("/%d", rand.Int63())
	partition := fmt.Sprintf("partition_%d", rand.Int())
	state := "ONLINE"
	key := fmt.Sprintf("%d", rand.Int())
	value := fmt.Sprintf("%d", rand.Int())
	c := NewClient(zap.NewNop(), tally.NoopScope, WithZkSvr(s.ZkConnectString),
		WithSessionTimeout(DefaultSessionTimeout))
	err := c.Connect()
	s.NoError(err)

	record := &model.ZNRecord{}
	err = c.SetRecordForPath(path, record)
	s.NoError(err)
	err = c.UpdateMapField(path, partition, model.FieldKeyCurrentState, state)
	s.NoError(err)
	err = c.UpdateSimpleField(path, key, value)
	record, err = c.GetRecordFromPath(path)
	s.NoError(err)
	s.Equal(state, record.GetMapField(partition, model.FieldKeyCurrentState))
	val, err := c.GetSimpleFieldValueByKey(path, key)
	s.NoError(err)
	s.Equal(value, val)
	err = c.RemoveMapFieldKey(path, partition)
	s.NoError(err)
	record, err = c.GetRecordFromPath(path)
	s.NoError(err)
	s.Equal("", record.GetMapField(partition, model.FieldKeyCurrentState))
	c.Disconnect()
}

func (s *ZKClientTestSuite) TestWatcher() {
	c := NewClient(zap.NewNop(), tally.NoopScope, WithZkSvr(s.ZkConnectString),
		WithSessionTimeout(DefaultSessionTimeout))
	watcher := &CountEventWatcher{}
	c.AddWatcher(watcher)
	err := c.Connect()
	s.NoError(err)
	s.Equal(3, watcher.GetCount())
	c.Disconnect()
	s.Equal(3, watcher.GetCount())
}

// TestRetryUntilConnected func failed once then succeeds on retry if the client is connected
func (s *ZKClientTestSuite) TestRetryUntilConnected() {
	z := NewFakeZk()
	client := s.createClientWithFakeConn(z)
	invokeCounter := 0
	client.Connect()
	s.Equal(1, len(z.GetConnections()))
	z.SetState(client.zkConn, zk.StateHasSession)
	s.NoError(client.retryUntilConnected(getFailOnceFunc(&invokeCounter)))
	s.Equal(2, invokeCounter)
	z.stop()
}

// TestRetryUntilConnectedWithoutSignal func fails and times out if client is not connected and
// condition receives no signals
func (s *ZKClientTestSuite) TestRetryUntilConnectedWithoutSignal() {
	z := NewFakeZk()
	client := s.createClientWithFakeConn(z)
	invokeCounter := 0
	expiringFn := func() error {
		invokeCounter++
		return zk.ErrSessionExpired
	}
	client.Connect()
	z.SetState(client.zkConn, zk.StateConnecting)
	s.Error(client.retryUntilConnected(expiringFn))
	s.Equal(1, invokeCounter)

}

// TestRetryUntilConnectedWithSignal func fails once, then succeeds on retry if the condition
// receives a signal
func (s *ZKClientTestSuite) TestRetryUntilConnectedWithSignal() {
	z := NewFakeZk()
	client := s.createClientWithFakeConn(z)
	client.Connect()
	z.SetState(client.zkConn, zk.StateConnecting)
	go func() {
		z.SetState(client.zkConn, zk.StateHasSession)
		// wait to allow event loop process op
		time.Sleep(10 * time.Millisecond)
		client.cond.Broadcast()
	}()
	invokeCounter := 0
	s.NoError(client.retryUntilConnected(getFailOnceFunc(&invokeCounter)))
	s.Equal(2, invokeCounter)
	z.stop()
}

// TestZkSizeLimit ensures zk write/update requests fail if size exceeds 1MB
func (s *ZKClientTestSuite) TestZkSizeLimit() {
	client := s.CreateAndConnectClient()
	defer client.Disconnect()
	numMb := 1024 * 1024
	legalData := make([]byte, numMb-128)
	legalData2 := make([]byte, numMb-128)
	legalData2[0] = 1
	oversizeData := make([]byte, numMb)
	path := s.createRandomPath()

	// create fails if data exceeds 1MB
	err := client.Create(path, oversizeData, FlagsZero, ACLPermAll)
	s.Error(err)

	// create succeeds if data size is legal
	err = client.Create(path, legalData, FlagsZero, ACLPermAll)
	s.Nil(err)
	res, _, err := client.Get(path)
	s.Equal(res, legalData)

	// set fails if data exceeds 1MB
	err = client.Set(path, oversizeData, -1)
	s.Error(err)

	// set succeeds if data size is legal
	err = client.Set(path, legalData2, -1)
	s.Nil(err)
	res, _, err = client.Get(path)
	s.Equal(res, legalData2)
}

func (s *ZKClientTestSuite) createClientWithFakeConn(z *FakeZk) *Client {
	return NewClient(zap.NewNop(), tally.NoopScope, WithConnFactory(z), WithRetryTimeout(time.Second))
}

func (s ZKClientTestSuite) createRandomPath() string {
	return fmt.Sprintf("/%d", rand.Int63())
}

// getFailOnceFunc returns a function that succeeds on second invocation
func getFailOnceFunc(invokeCounter *int) func() error {
	shouldPass := false
	return func() error {
		*invokeCounter++
		if shouldPass {
			return nil
		}
		shouldPass = true
		return zk.ErrSessionExpired
	}
}
