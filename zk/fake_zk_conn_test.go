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
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	_defaultPath    = "/"
	_defaultVersion = int32(0)
)

func TestFakeZkConn(t *testing.T) {
	conn := NewFakeZkConn(NewFakeZk())
	err := conn.AddAuth(_defaultPath, nil)
	assert.NoError(t, err)
	_, _, err = conn.Children(_defaultPath)
	assert.NoError(t, err)
	_, _, childrenCh, err := conn.ChildrenW(_defaultPath)
	assert.NoError(t, err)
	_, _, err = conn.Get(_defaultPath)
	assert.NoError(t, err)
	_, _, getCh, err := conn.GetW(_defaultPath)
	assert.NoError(t, err)
	_, _, err = conn.Exists(_defaultPath)
	assert.NoError(t, err)
	_, _, existsCh, err := conn.ExistsW(_defaultPath)
	assert.NoError(t, err)
	_, err = conn.Set(_defaultPath, nil, _defaultVersion)
	assert.NoError(t, err)
	conn.Create(_defaultPath, nil, _defaultVersion, nil)
	assert.NoError(t, err)
	err = conn.Delete(_defaultPath, _defaultVersion)
	assert.NoError(t, err)
	_, err = conn.Multi()
	assert.NoError(t, err)
	conn.SessionID()
	conn.SetLogger(nil)

	// verify history
	calls := conn.GetHistory().GetHistoryForMethod("AddAuth")
	assert.Equal(t, 1, len(calls))
	allHistory := conn.GetHistory().GetHistory()
	assert.Equal(t, 13, len(allHistory))

	// close and cleanup
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-childrenCh:
			case <-getCh:
			case <-existsCh:
			case <-doneCh:
				break
			}
		}
	}()
	conn.Close()
	doneCh <- struct{}{}
}
