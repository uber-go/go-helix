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

const (
	_helixVersion                 = "helix-0.6.8"
	_defaultStateModelFactoryName = "DEFAULT"
)

// Field keys commonly shared among Helix models
const (
	FieldKeyBucketSize            = "BUCKET_SIZE"
	FieldKeySessionID             = "SESSION_ID"
	FieldKeyBatchMsgMode          = "BATCH_MESSAGE_MODE"
	FieldKeyStateModelFactoryName = "STATE_MODEL_FACTORY_NAME"
)

// Field keys used by Helix message
const (
	FieldKeyStateModelDef         = "STATE_MODEL_DEF"
	FieldKeyTargetSessionID       = "TGT_SESSION_ID"
	FieldKeyTargetName            = "TGT_NAME"
	FieldKeyPartitionName         = "PARTITION_NAME"
	FieldKeyFromState             = "FROM_STATE"
	FieldKeyToState               = "TO_STATE"
	FieldKeyCurrentState          = "CURRENT_STATE"
	FieldKeyParentMsgID           = "PARENT_MSG_ID"
	FieldKeyMsgState              = "MSG_STATE"
	FieldKeyMsgType               = "MSG_TYPE"
	FieldKeyResourceName          = "RESOURCE_NAME"
	FieldKeyCreateTimestamp       = "CREATE_TIMESTAMP"
	FieldKeyExecuteStartTimestamp = "EXECUTE_START_TIMESTAMP"
)

// Field keys used by the ideal state
const (
	FieldKeyNumPartitions = "NUM_PARTITIONS"
)

// Field keys used by instance config
const (
	FieldKeyHelixHost    = "HELIX_HOST"
	FieldKeyHelixPort    = "HELIX_PORT"
	FieldKeyHelixEnabled = "HELIX_ENABLED"
)

// Field keys used by live instance
const (
	FieldKeyHelixVersion = "HELIX_VERSION"
	FieldKeyLiveInstance = "LIVE_INSTANCE"
)

// Field keys used by state model def
const (
	FieldKeyInitialState = "INITIAL_STATE"
)
