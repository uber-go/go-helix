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

// InstanceConfig represents configs for a Helix instance
type InstanceConfig struct {
	ZNRecord
}

// NewInstanceConfig creates a new instance config property
func NewInstanceConfig(instanceName string) *InstanceConfig {
	return &InstanceConfig{*NewRecord(instanceName)}
}

// SetHost sets host of the instance
func (c *InstanceConfig) SetHost(host string) {
	c.SetSimpleField(FieldKeyHelixHost, host)
}

// SetPort sets port of the instance
func (c *InstanceConfig) SetPort(port int) {
	c.SetIntField(FieldKeyHelixPort, port)
}

// GetEnabled sets if the instance is enabled
func (c *InstanceConfig) GetEnabled() bool {
	return c.GetBooleanField(FieldKeyHelixEnabled, false)
}

// SetEnabled sets if the instance is enabled
func (c *InstanceConfig) SetEnabled(enabled bool) {
	c.SetBooleanField(FieldKeyHelixEnabled, enabled)
}
