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

package model

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecordEncoding(t *testing.T) {
	r := &ZNRecord{}
	r.Version = _defaultRecordVersion
	bytes, err := r.Marshal()
	assert.Nil(t, err)
	deserialized, err := NewRecordFromBytes(bytes)
	assert.True(t, reflect.DeepEqual(r, deserialized))
}

func TestGetDefaultFields(t *testing.T) {
	r := &ZNRecord{}
	_, ok := r.GetSimpleField("")
	assert.False(t, ok)
	defaultInt := 5
	i := r.GetIntField("", defaultInt)
	assert.Equal(t, i, defaultInt)
	r.SetIntField("", defaultInt)
	i = r.GetIntField("", defaultInt+1)
	assert.Equal(t, i, defaultInt)
	boolFieldKey := "bool"
	r.SetBooleanField(boolFieldKey, true)
	assert.True(t, r.GetBooleanField(boolFieldKey, false))
	mapFieldKey, mapFieldProp, mapFieldVal := "k", "p", "val"
	r.SetMapField(mapFieldKey, mapFieldProp, mapFieldVal)
	assert.Equal(t, mapFieldVal, r.GetMapField(mapFieldKey, mapFieldProp))
}
