# Honey Badger

[![Go Report Card](https://goreportcard.com/badge/github.com/honey-badger-io/honey-badger)](https://goreportcard.com/report/github.com/honey-badger-io/honey-badger)
[![Build honey-badger](https://github.com/honey-badger-io/honey-badger/actions/workflows/ci.yml/badge.svg)](https://github.com/honey-badger-io/honey-badger/actions/workflows/ci.yml)

Honey Badger is simple and fast key/value db server build on top of [BadgerDB](https://github.com/dgraph-io/badger). It uses [gRPC](https://grpc.io/) as transport protocol.

## Getting Started
### Build server
To build Honey Badger server you need [Go 1.21](https://go.dev/dl/) or above.

Windows users may need to install GNU Make. The best way is to use some package manager like [scoop](https://scoop.sh/#/apps?q=make)

To start, clone the repo

```sh
git clone git@github.com:honey-badger-io/honey-badger.git
```

Go to `honey-badger` directory and build server binaries
```sh
$ make build
```

This will produce server binary. Run it with default configuration
```sh
$ ./bin/hb
```

or on Windows
```
bin\hb.exe
```

### Docker

Run Docker image using
```sh
docker run --name honey-badger -p 18950:18950 -d meeron/honey-badger:latest
```

### Client
Current official clients:
* [.NET](https://www.nuget.org/packages/HoneyBadger.Client)
* [Go](https://pkg.go.dev/github.com/honey-badger-io/go-client)

Feel free to post na issue if you miss client for your favorite language.

### Benchmark (in memory)
To test server performance on your system run
```sh
$ ./bin/hb -bench localhost:18950
os: darwin/arm64
cpus: 8

payload size: 256 bytes
num goroutines: 20
Set_30000: 293.691166ms
Set_50000: 509.71825ms
Set_100000: 1.026570458s

payload size: 256 bytes
num goroutines: 20
Get_30000: 317.108166ms
Get_50000: 489.316917ms
Get_100000: 982.176458ms

payload size: 256 bytes
num goroutines: 1
SendWithStream_100000: 212.652541ms
SendWithStream_300000: 482.201958ms
SendWithStream_500000: 805.415042ms

payload size: 256 bytes
num goroutines: 1
ReadWithStream_100000: 100.984375ms
ReadWithStream_300000: 316.02125ms
ReadWithStream_500000: 532.293375ms
```

The result `Set_30000: 293.691166ms` says that in 293ms 30k items has been sent to server
using 20 concurrent tasks.

## Hardware requirements
Honey Badger server should run on anything. CPU and RAM depends on your needs, but absolute minium is SSD disk (if persistance storage will be in use). Use benchmark command to check how Honey Badger is working on your instance.

## System requirements
### Linux and Mac
Honey Badger should build and run on any Linux distro. [BadgerDB recommends](https://dgraph.io/docs/badger/faq/#are-there-any-linux-specific-settings-that-i-should-use) `max file descriptors` set to a high number depending upon the expected size of your data.

### Windows
It should build and run just fine.
