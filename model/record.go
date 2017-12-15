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
	"encoding/json"
	"strconv"
	"strings"
)

const (
	_defaultRecordVersion = int32(-1)
)

// ZNRecord helps communication between nodes
// Mirrors org.apache.helix.ZNRecord
// Sample record:
// {
// 	"id" : "MyResource",
// 	"simpleFields" : {
// 		"REBALANCE_MODE" : "SEMI_AUTO",
// 		"NUM_PARTITIONS" : "3",
// 		"REPLICAS" : "2",
// 		"STATE_MODEL_DEF_REF" : "MasterSlave",
// 	}
// 	"listFields" : {
// 		"MyResource_0" : [node1, node2],
// 		"MyResource_1" : [node2, node3],
// 		"MyResource_2" : [node3, node1]
// 	},
// 	"mapFields" : {
// 	}
// }
type ZNRecord struct {
	ID           string                       `json:"id"`
	SimpleFields map[string]string            `json:"simpleFields"`
	ListFields   map[string][]string          `json:"listFields"`
	MapFields    map[string]map[string]string `json:"mapFields"`
	Version      int32                        `json:"-"`
}

// NewRecord returns new ZNRecord with id
func NewRecord(id string) *ZNRecord {
	return &ZNRecord{
		ID:           id,
		SimpleFields: map[string]string{},
		ListFields:   map[string][]string{},
		MapFields:    map[string]map[string]string{},
	}
}

// String returns the beautified JSON string for the ZNRecord
func (r ZNRecord) String() string {
	s, _ := r.Marshal()
	return string(s)
}

// Marshal generates the beautified json in byte array format
func (r ZNRecord) Marshal() ([]byte, error) {
	return json.MarshalIndent(r, "", "    ")
}

// NewRecordFromBytes creates a new znode instance from a byte array
func NewRecordFromBytes(data []byte) (*ZNRecord, error) {
	record := ZNRecord{Version: _defaultRecordVersion}
	err := json.Unmarshal(data, &record)
	return &record, err
}

// GetSimpleField returns a value of a key in SimpleField structure
func (r ZNRecord) GetSimpleField(key string) (val string, ok bool) {
	if r.SimpleFields == nil {
		return "", false
	}
	val, ok = r.SimpleFields[key]
	return val, ok
}

// GetIntField returns the integer value of a field in the SimpleField
func (r ZNRecord) GetIntField(key string, defaultValue int) int {
	value, ok := r.GetSimpleField(key)
	if !ok {
		return defaultValue
	}

	intVal, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return intVal
}

// GetInt64Field returns the integer value of a field in the SimpleField
func (r ZNRecord) GetInt64Field(key string, defaultValue int64) int64 {
	value, ok := r.GetSimpleField(key)
	if !ok {
		return defaultValue
	}

	i64, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return defaultValue
	}
	return i64
}

// SetIntField sets the integer value of a key under SimpleField.
// the value is stored as in string form
func (r *ZNRecord) SetIntField(key string, value int) {
	r.SetSimpleField(key, strconv.Itoa(value))
}

// GetStringField returns string from simple fields
func (r ZNRecord) GetStringField(key, defaultValue string) string {
	res, ok := r.GetSimpleField(key)
	if !ok {
		return defaultValue
	}
	return res
}

// GetBooleanField gets the value of a key under SimpleField and
// convert the result to bool type. That is, if the value is "true",
// the result is true.
func (r ZNRecord) GetBooleanField(key string, defaultValue bool) bool {
	result, ok := r.GetSimpleField(key)
	if !ok {
		return defaultValue
	}
	return strings.ToLower(result) == "true"
}

// SetBooleanField sets a key under SimpleField with a specified bool
// value, serialized to string. For example, true will be stored as
// "TRUE"
func (r *ZNRecord) SetBooleanField(key string, value bool) {
	r.SetSimpleField(key, strconv.FormatBool(value))
}

// SetSimpleField sets the value of a key under SimpleField
func (r *ZNRecord) SetSimpleField(key string, value string) {
	if r.SimpleFields == nil {
		r.SimpleFields = make(map[string]string)
	}
	r.SimpleFields[key] = value
}

// RemoveSimpleField removes a key under SimpleField, currently for unit tests only
func (r *ZNRecord) RemoveSimpleField(key string) {
	delete(r.SimpleFields, key)
}

// GetMapField returns the string value of the property of a key
// under MapField.
func (r ZNRecord) GetMapField(key string, property string) string {
	if r.MapFields == nil || r.MapFields[key] == nil || r.MapFields[key][property] == "" {
		return ""
	}
	return r.MapFields[key][property]
}

// SetMapField sets the value of a key under MapField. Both key and
// value are string format.
func (r *ZNRecord) SetMapField(key string, property string, value string) {
	if r.MapFields == nil {
		r.MapFields = make(map[string]map[string]string)
	}

	if r.MapFields[key] == nil {
		r.MapFields[key] = make(map[string]string)
	}

	r.MapFields[key][property] = value
}

// RemoveMapField deletes a key from MapField
func (r *ZNRecord) RemoveMapField(key string) {
	delete(r.MapFields, key)
}
