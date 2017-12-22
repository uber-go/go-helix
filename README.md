# go-helix [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov]

The Go implementation of [Apache Helix](https://helix.apache.org) (currently the participant part only).

## Installation

`go get -u github.com/uber-go/go-helix`

## Quick Start

### Init participant

```go
participant, fatalErrChan := NewParticipant(
	zap.NewNop(),
	tally.NoopScope,
	"localhost:2181", // Zookeeper connect string
	"test_app", // application identifier
	"test_cluster", // helix cluster name
	"test_resource", // helix resource name
	"localhost", // participant host name
	123, // participant port
)

processor := NewStateModelProcessor()
processor.AddTransition("OFFLINE", "ONLINE", func(m *model.Message) (err error) {
    partitionNum, err := p.getPartitionNum(m)
    if err != nil {
        return err
    }
    // add logic here to save the owned partition and/or perform any actions for going online
})
processor.AddTransition("ONLINE", "OFFLINE", func(m *model.Message) (err error) {
   partitionNum, err := p.getPartitionNum(m)
   if err != nil {
       return err
   }
   // add logic here to remove the owned partition and/or perform any actions for going offline
})

participant.RegisterStateModel("OnlineOffline", processor)

err := participant.Connect() // initialization is complete if err is nil

// listen to the fatalError and handle the error by
// 1. recreate participant from scratch and connect, or
// 2. quit the program and restart
faltalErr := <- faltalErrChan 
```

### Use participant

Use the saved partitions to see if the partition should be handled by the participant.

### Disconnect participant

```go
participant.Disconnect()
```

## Development Status: Beta

The APIs are functional. We do not expect, but there's no guarantee that no breaking changes will be made.

## Contributing

We encourage and support an active, healthy community of contributors &mdash;
including you! Details are in the [contribution guide](CONTRIBUTING.md) and
the [code of conduct](CODE_OF_CONDUCT.md). The go-helix maintainers keep an eye on
issues and pull requests, but you can also report any negative conduct to
oss-conduct@uber.com. That email list is a private, safe space; even the go-helix
maintainers don't have access, so don't hesitate to hold us to a high standard.

<hr>

Released under the [MIT License](LICENSE).

[doc-img]: https://godoc.org/github.com/uber-go/go-helix?status.svg
[doc]: https://godoc.org/github.com/uber-go/go-helix
[//]: # (TODO: update to https://travis-ci.org/uber-go/go-helix after making the repo public)
[ci-img]: https://travis-ci.com/uber-go/go-helix.svg?token=iecXysxCKpLxFnkjyQYH&branch=master
[ci]: https://travis-ci.com/uber-go/go-helix
[cov-img]: https://codecov.io/gh/uber-go/go-helix/branch/master/graph/badge.svg
[cov]: https://codecov.io/gh/uber-go/go-helix
