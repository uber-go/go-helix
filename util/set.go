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

package util

// StringSet is a wrapper for a hash set that stores string keys
type StringSet map[string]struct{}

// NewStringSet create a new set
func NewStringSet(initElements ...string) StringSet {
	set := make(map[string]struct{}, len(initElements))
	for _, key := range initElements {
		set[key] = struct{}{}
	}
	return set
}

// Add adds a new key to set
func (s StringSet) Add(key string) {
	s[key] = struct{}{}
}

// AddAll adds a new key to set
func (s StringSet) AddAll(keys ...string) {
	for _, key := range keys {
		s[key] = struct{}{}
	}
}

// Remove deletes a key from set
func (s StringSet) Remove(key string) {
	delete(s, key)
}

// Contains checks if the set contains the key
func (s StringSet) Contains(key string) bool {
	_, ok := s[key]
	return ok
}

// Size returns the size of the set
func (s StringSet) Size() int {
	return len(s)
}

// IsEmpty returns if the size is 0
func (s StringSet) IsEmpty() bool {
	return len(s) == 0
}
