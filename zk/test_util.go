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
	"bufio"
	"errors"
	"log"
	"os"
	"os/exec"
	"path"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// EmbeddedZkServer is the connect string for embedded ZK server
const EmbeddedZkServer = "localhost:2181"

var mu = sync.Mutex{}

// BaseZkTestSuite provides utility to test Zookeeper functions without Helix admin
type BaseZkTestSuite struct {
	suite.Suite

	EmbeddedZkPath  string
	ZkConnectString string
}

// SetupSuite ensures ZK server is up
func (s *BaseZkTestSuite) SetupSuite() {
	s.ZkConnectString = EmbeddedZkServer

	if s.EmbeddedZkPath == "" {
		s.EmbeddedZkPath = path.Join(os.Getenv("APP_ROOT"), "zk/embedded")
	}
	err := EnsureZookeeperUp(s.EmbeddedZkPath)
	s.NoError(err)
}

// CreateAndConnectClient creates ZK client and connects to ZK server
func (s *BaseZkTestSuite) CreateAndConnectClient() *Client {
	zkClient := NewClient(
		zap.NewNop(), tally.NoopScope, WithZkSvr(s.ZkConnectString), WithSessionTimeout(time.Second))
	err := zkClient.Connect()
	s.NoError(err)
	return zkClient
}

// EnsureZookeeperUp starts the embedded (test) Zookeeper if not running.
func EnsureZookeeperUp(scriptRelativeDirPath string) error {
	mu.Lock()
	defer mu.Unlock()

	if isEmbeddedZookeeperStarted(3 * time.Second) {
		return nil
	}

	err := startEmbeddedZookeeper(scriptRelativeDirPath)
	if err != nil {
		log.Println("Unable to start Zookeeper server: ", err)
		return err
	}

	log.Println("Zookeeper server is up.")

	return nil
}

// StopZookeeper stops the embedded (test) Zookeeper if running.
func StopZookeeper(scriptRelativeDirPath string) error {
	mu.Lock()
	defer mu.Unlock()

	_, _, err := zk.Connect([]string{EmbeddedZkServer}, time.Second)
	if err != nil {
		return nil
	}

	err = stopEmbeddedZookeeper(scriptRelativeDirPath)
	if err != nil {
		log.Println("Unable to stop Zookeeper server: ", err)
		return err
	}
	log.Println("Zookeeper server is stopped.")
	return nil
}

func isEmbeddedZookeeperStarted(timeout time.Duration) bool {
	zkConn, _, err := zk.Connect([]string{EmbeddedZkServer}, time.Second)
	if err == nil && zkConn != nil {
		defer zkConn.Close()
		done := time.After(timeout)
	loop:
		for { // zk.Connect is async
			if zkConn.State() == zk.StateHasSession {
				return true
			}
			select {
			case <-done:
				break loop
			default:
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	log.Printf("Unable to connect Zookeeper %s: %v\n", EmbeddedZkServer, err)
	return false
}

func startEmbeddedZookeeper(scriptRelativeDirPath string) error {
	err := runCmd(scriptRelativeDirPath, "start.sh")
	if err != nil {
		return err
	} else if !isEmbeddedZookeeperStarted(5 * time.Second) {
		return errors.New("embedded zk is not started")
	}
	return nil
}

func stopEmbeddedZookeeper(scriptRelativeDirPath string) error {
	return runCmd(scriptRelativeDirPath, "stop.sh")
}

func runCmd(scriptRelativeDirPath, scriptFileName string) error {
	cmd := exec.Cmd{
		Path: "/bin/bash",
		Args: []string{"/bin/bash", scriptFileName},
		Dir:  scriptRelativeDirPath,
	}

	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		log.Println("Error creating StdoutPipe: ", err)
		return err
	}

	err = cmd.Start()
	if err != nil {
		return err
	}

	in := bufio.NewScanner(cmdReader)
	go func() {
		for in.Scan() {
			log.Println(in.Text())
		}
	}()

	time.Sleep(5 * time.Second) // wait some time as cmd.Start doesn't wait
	return nil
}
